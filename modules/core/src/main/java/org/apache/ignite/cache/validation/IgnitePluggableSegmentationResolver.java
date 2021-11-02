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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;

import static java.lang.Boolean.TRUE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UNDEFINED;

/** */
public class IgnitePluggableSegmentationResolver implements PluggableSegmentationResolver {
    /** */
    public static final String SEG_RESOLVER_ENABLED_PROP_NAME = "org.apache.ignite.segmentation.resolver.enabled";

    /** */
    private static final String SEG_RESOLVER_THREAD_PREFIX = "segmentation-resolver";

    /** */
    private static final int[] TOP_CHANGED_EVTS = new int[] {
        EVT_NODE_LEFT,
        EVT_NODE_JOINED,
        EVT_NODE_FAILED
    };

    /** */
    private final SimpleDistributedProperty<Boolean> segResolverEnabledProp = new SimpleDistributedProperty<>(
        SEG_RESOLVER_ENABLED_PROP_NAME,
        Boolean::parseBoolean
    );

    /** Ignite kernel context. */
    private final GridKernalContext ctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** */
    private final IgniteThreadPoolExecutor stateChangeExec;

    /** */
    private long lastCheckedTopVer;

    /** */
    private volatile State state;

    /** @param ctx Ignite kernel context. */
    public IgnitePluggableSegmentationResolver(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        stateChangeExec = new IgniteThreadPoolExecutor(
            SEG_RESOLVER_THREAD_PREFIX,
            ctx.igniteInstanceName(),
            1,
            1,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            UNDEFINED,
            new OomExceptionHandler(ctx));

        stateChangeExec.allowCoreThreadTimeOut(true);
    }

    /** {@inheritDoc} */
    @Override public boolean isValidSegment() {
        return isDisabled() || state != State.INVALID;
    }

    /** */
    public void start() {
        if (ctx.clientNode())
            return;

        state = ctx.state().clusterState().state() == ACTIVE ? State.VALID : State.CLUSTER_WRITE_BLOCKED;

        ctx.event().addDiscoveryEventListener(new TopologyChangedEventListener(), TOP_CHANGED_EVTS);

        ctx.discovery().setCustomEventListener(
            ChangeGlobalStateFinishMessage.class,
            (topVer, snd, msg) -> state = msg.state() == ACTIVE ? State.VALID : State.CLUSTER_WRITE_BLOCKED
        );

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                /** {@inheritDoc} */
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    dispatcher.registerProperty(segResolverEnabledProp);
                }

                /** {@inheritDoc} */
                @Override public void onReadyToWrite() {
                    setDefaultValue(segResolverEnabledProp, U.isLocalNodeCoordinator(ctx.discovery()), log);
                }
            });
    }

    /** @return Discovery data. */
    public Serializable provideDiscoveryData() {
        return state;
    }

    /**
     * @param data Discovery data.
     * @param joiningNodeId ID of the joining node.
     */
    public void onDiscoveryDataReceived(UUID joiningNodeId, Serializable data) {
        if (ctx.localNodeId().equals(joiningNodeId))
            state = (State)data;
    }

    /**
     * @param node Node.
     * @param data Joining node discovery data.
     */
    public void validateNewNode(ClusterNode node, Serializable data) {
        if (node.isClient())
            return;

        if (data == null) {
            throw new IgniteException( "The Segmentation Resolver plugin is not configured for the server node that is" +
                " trying to join the cluster. Since the Segmentation Resolver is only applicable if all server nodes" +
                " in the cluster have one, node join request will be rejected [rejectedNodeId=" + node.id() + ']');
        }

        if (state == State.VALID) {
            DiscoCache discoCache = ctx.discovery().discoCache(new AffinityTopologyVersion(lastCheckedTopVer, 0));

            if (discoCache != null) {
                for (ClusterNode srv : discoCache.serverNodes()) {
                    if (!ctx.discovery().alive(srv))
                        throw new IgniteException("Node join request will be rejected due to concurrent node left" +
                            " process handling [rejectedNodeId=" + node.id() + ']');
                }
            }
        }
    }

    /** */
    private boolean isDisabled() {
        return !TRUE.equals(segResolverEnabledProp.get());
    }

    /** */
    private class TopologyChangedEventListener implements DiscoveryEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            lastCheckedTopVer = evt.topologyVersion();

            if (isDisabled() || state != State.VALID)
                return;

            if (evt.type() == EVT_NODE_FAILED) {
                List<? extends BaselineNode> baselineNodes = discoCache.baselineNodes();

                if (baselineNodes != null && aliveBaselineNodes(baselineNodes) < baselineNodes.size() / 2 + 1) {
                    state = State.INVALID;

                    stateChangeExec.execute(() -> {
                        try {
                            ctx.cluster().get().state(ACTIVE_READ_ONLY);
                        }
                        catch (Throwable e) {
                            U.error(
                                log,
                                "Failed to automatically switch state of the segmented cluster to the READ-ONLY mode" +
                                    " [segmentedNodes=" + formatTopologyNodes(discoCache.allNodes()) + "]. Cache writes" +
                                    " are already restricted for all configured caches, but this step is still required" +
                                    " in order to be able to unlock cache writes in the future. Retry this operation" +
                                    " manually, if possible.",
                                e
                            );
                        }
                    });

                    U.warn(log, "Cluster segmentation was detected [segmentedNodes=" +
                        formatTopologyNodes(discoCache.allNodes()) + ']');
                }
            }

            if (ctx.state().isBaselineAutoAdjustEnabled() && ctx.state().baselineAutoAdjustTimeout() == 0L)
                U.warn(log, "Segmentation Resolver considers any topology change to be valid with the current" +
                    " Baseline topology configuration. Configure Baseline nodes explicitly or set Baseline Auto" +
                    " Adjustment Timeout to greater than zero.");
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
        }

        /** */
        private int aliveBaselineNodes(Collection<? extends BaselineNode> baselineNodes) {
            int res = 0;

            for (BaselineNode node : baselineNodes) {
                if (!(node instanceof DetachedClusterNode))
                    ++res;
            }

            return res;
        }

        /** @return String representation of the specified node collection. */
        private String formatTopologyNodes(Collection<ClusterNode> nodes) {
            return nodes.stream().map(n -> n.id().toString()).collect(Collectors.joining(", "));
        }
    }

    /** */
    private enum State {
        /** */
        VALID,

        /** */
        INVALID,

        /** */
        CLUSTER_WRITE_BLOCKED
    }
}
