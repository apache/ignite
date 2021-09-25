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

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;

/**
 * Represents {@link PluggableSegmentationResolver} implementation that detects cluster nodes segmentation and
 * makes an attempt to changes state of segmented part of the cluster to read-only. Current implementation assumes that
 * {@link #isValidSegment()} will be called at the end of each PME future execution.
 */
public class ClusterStateChangeSegmentationResolver implements PluggableSegmentationResolver, PartitionsExchangeAware {
    /** Ignite kernal context. */
    private final GridKernalContext ctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** The executor that asynchronously performs cluster state change procedure. */
    private final IgniteThreadPoolExecutor stateChangeExecutor;

    /** State of the current segment.*/
    private boolean isValid;

    /** Last checked exchange future. */
    private GridDhtPartitionsExchangeFuture lastCheckedExchangeFut;

    /** Baseline nodes that the cluster had on previous PME. */
    private int prevBaselineNodesCnt;

    /** @param ctx Ignite kernal context. */
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
            GridIoPolicy.UNDEFINED,
            new OomExceptionHandler(ctx));

        stateChangeExecutor.allowCoreThreadTimeOut(true);
    }

    /** {@inheritDoc} */
    @Override public boolean validateSegment() {
        GridDhtPartitionsExchangeFuture exchFut = ctx.cache().context().exchange().lastTopologyFuture();

        if (lastCheckedExchangeFut == exchFut)
            return isValid;

        lastCheckedExchangeFut = exchFut;

        Collection<ClusterNode> topNodes = exchFut.events().lastEvent().topologyNodes();

        if (isValid) {
            if (hasServerFailed(exchFut) && baselineNodesCount(exchFut) <= prevBaselineNodesCnt / 2) {
                isValid = false;

                U.warn(log, "Cluster segmentation was detected. An attempt will be made to put the state of segmented" +
                    " cluster nodes in read-only mode. Segmentation flag can be cleared manually by changing cluster" +
                    " state to the ACTIVE mode [segmentedNodeIds=" + toString(topNodes) + "].");

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
                            U.error(log, "Failed to switch state of the segmented cluster nodes to the read-only mode." +
                                " If the current state has not been previously changed to read-only mode, please retry" +
                                " this operation manually.", e);
                        }
                    });
                }
            }
        }
        else {
            ExchangeActions exchActions = exchFut.exchangeActions();

            StateChangeRequest stateChangeReq = exchActions == null ? null : exchActions.stateChangeRequest();

            if (stateChangeReq != null && stateChangeReq.state() == ACTIVE) {
                isValid = true;

                if (log.isInfoEnabled()) {
                    log.info("State of the previously segmented cluster nodes was manually changed to ACTIVE mode." +
                        " Segmentation flag cleared [topNodeIds=" + toString(topNodes) + ']');
                }
            }
        }

        return isValid;
    }

    /** {@inheritDoc} */
    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        prevBaselineNodesCnt = baselineNodesCount(fut);
    }

    /** @return Whether current segment is valid. */
    public boolean isValidSegment() {
        return isValid;
    }

    /**
     * @param fut PME future.
     * @return Count of baseline nodes in current topology.
     */
    private int baselineNodesCount(GridDhtPartitionsExchangeFuture fut) {
        int res = 0;

        DiscoCache discoCache = fut.events().discoveryCache();

        for (ClusterNode node : fut.events().lastEvent().topologyNodes()) {
            if (!node.isClient() && discoCache.baselineNode(node))
                ++res;
        }

        return res;
    }

    /**
     * @param fut PME future.
     * @return Whether any nodes failed since previous PME.
     */
    private boolean hasServerFailed(GridDhtPartitionsExchangeFuture fut) {
        boolean res = false;

        for (DiscoveryEvent event : fut.events().events()) {
            if (event.type() == EVT_NODE_FAILED && !event.eventNode().isClient())
                res = true;
        }

        return res;
    }

    /**
     * @param nodes Collection of the cluster nodes.
     * @return String representation of specified node IDs.
     */
    private String toString(Collection<ClusterNode> nodes) {
        return nodes.stream().map(n -> n.id().toString()).collect(Collectors.joining(", ", "[", "]"));
    }
}
