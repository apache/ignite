/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.validation;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UNDEFINED;

/**
 * Represents {@link PluggableSegmentationResolver} implementation that detects cluster nodes segmentation and
 * changes state of segmented part of the cluster to read-only mode.
 */
public class ClusterStateChangeSegmentationResolver implements PluggableSegmentationResolver {
    /** Ignite kernel context. */
    private final GridKernalContext ctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** The executor that asynchronously performs cluster state change procedure. */
    private final IgniteThreadPoolExecutor stateChangeExecutor;

    /** State of the current segment.*/
    private boolean isValid;

    /** Last checked exchange future. */
    private GridDhtPartitionsExchangeFuture lastCheckedExchangeFut;

    /** Baseline nodes count that the cluster had on previous PME. */
    private int prevBaselineNodesCnt;

    /** @param ctx Ignite kernel context. */
    public ClusterStateChangeSegmentationResolver(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        isValid = true;

        stateChangeExecutor = new IgniteThreadPoolExecutor(
            "segmentation-resolver-state-change-executor",
            ctx.igniteInstanceName(),
            1,
            1,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            UNDEFINED,
            new OomExceptionHandler(ctx));

        stateChangeExecutor.allowCoreThreadTimeOut(true);
    }

    /** {@inheritDoc} */
    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        if (lastCheckedExchangeFut == fut)
            return;

        int baselineNodesCnt = baselineNodesCount(fut);

        if (isValid) {
            if (hasServerFailed(fut) && baselineNodesCnt <= prevBaselineNodesCnt / 2) {
                isValid = false;

                U.warn(log, "Cluster segmentation was detected [segmentedNodes=" + formatTopologyNodes(fut) + ']');

                restrictSegmentedCluster();
            }
        }
        else
            checkSegmentationResolved(fut);

        prevBaselineNodesCnt = baselineNodesCnt;

        lastCheckedExchangeFut = fut;
    }

    /** Restricts segmented cluster by changing state to READ-ONLY mode. */
    private void restrictSegmentedCluster() {
        U.warn(log, "Switching cluster state to READ-ONLY mode due to segmentation.");

        if (U.isLocalNodeCoordinator(ctx.discovery())) {
            stateChangeExecutor.submit(() -> {
                try {
                    ctx.state().changeGlobalState(
                        ACTIVE_READ_ONLY,
                        false,
                        null,
                        false
                    ).get();
                }
                catch (Throwable e) {
                    U.error(log, "Failed to switch state of the segmented cluster nodes to the read-only mode.", e);
                }
            });
        }
    }

    /** Checks that segmentation was resolved manually and, if so, resets segmentation flag for the cluster nodes. */
    private void checkSegmentationResolved(GridDhtPartitionsExchangeFuture exchFut) {
        ExchangeActions exchActions = exchFut.exchangeActions();

        StateChangeRequest stateChangeReq = exchActions == null ? null : exchActions.stateChangeRequest();

        if (stateChangeReq != null && stateChangeReq.state() == ACTIVE) {
            isValid = true;

            if (log.isInfoEnabled())
                log.info("Segmentation was resolved manually for nodes [nodes=" + formatTopologyNodes(exchFut) + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isValidSegment() {
        return isValid;
    }

    /** @return Count of baseline nodes that are part of the cluster at the end of specified PME future. */
    private int baselineNodesCount(GridDhtPartitionsExchangeFuture fut) {
        int res = 0;

        DiscoCache discoCache = fut.events().discoveryCache();

        for (ClusterNode node : fut.events().lastEvent().topologyNodes()) {
            if (!node.isClient() && discoCache.baselineNode(node))
                ++res;
        }

        return res;
    }

    /** @return Whether any nodes failed since previous PME. */
    private boolean hasServerFailed(GridDhtPartitionsExchangeFuture fut) {
        boolean res = false;

        for (DiscoveryEvent event : fut.events().events()) {
            if (event.type() == EVT_NODE_FAILED && !event.eventNode().isClient())
                res = true;
        }

        return res;
    }

    /**
     * @return String representation of the topology nodes that are part of the cluster at the end of the specified PME
     * future.
     */
    private String formatTopologyNodes(GridDhtPartitionsExchangeFuture exchFut) {
        return exchFut.events().lastEvent().topologyNodes().stream()
            .map(n -> n.id().toString())
            .collect(Collectors.joining(", "));
    }
}
