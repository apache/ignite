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

package org.apache.ignite.plugin.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.CacheTopologyValidatorProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/** */
public class CacheTopologyValidatorPluginProvider implements PluginProvider<PluginConfiguration>, TopologyValidator {
    /**
     * Decimal fraction of nodes that determines how many nodes must remain in the baseline topology in order to this
     * segment was considered valid and continued to accept write requests after segmentation. This value must be in
     * range from 0.5 (inclusively) to 1 or 0 if validation should be disabled.
     *
     * @see #DFLT_DEACTIVATION_THRESHOLD that is used as a default value.
     */
    public static final String TOP_VALIDATOR_DEACTIVATION_THRESHOLD_PROP_NAME =
        "org.apache.ignite.topology.validator.deactivation.threshold";

    /** */
    public static final float DFLT_DEACTIVATION_THRESHOLD = 0.5F;

    /** */
    private static final int[] TOP_CHANGED_EVTS = new int[] {
        EVT_NODE_LEFT,
        EVT_NODE_JOINED,
        EVT_NODE_FAILED
    };

    /** */
    private final SimpleDistributedProperty<Float> deactivateThresholdProp = new SimpleDistributedProperty<>(
        TOP_VALIDATOR_DEACTIVATION_THRESHOLD_PROP_NAME,
        str -> {
            float res = Float.parseFloat(str);

            if ((res < 0.5F || res >= 1F) && res != 0F) {
                throw new IgniteException("Topology validator cluster deactivation threshold must be a decimal" +
                    " fraction in the range from 0.5 (inclusively) to 1 or 0 if validation should be disabled.");
            }

            return res;
        }
    );

    /** Ignite kernel context. */
    private GridKernalContext ctx;

    /** Ignite logger. */
    private IgniteLogger log;

    /** */
    private long lastCheckedTopVer;

    /**
     * {@code null} value means that segmentation happened, cache writes were blocked and cluster is in process of
     * switching its state to READ-ONLY mode.
     */
    private volatile ClusterState state;

    /** {@inheritDoc} */
    @Override public String name() {
        return "Topology Validator";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0.0";
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)new IgnitePlugin() {
            // No-op.
        };
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext pluginCtx, ExtensionRegistry registry) {
        ctx = ((IgniteEx)pluginCtx.grid()).context();

        if (!ctx.clientNode()) {
            registry.registerExtension(CacheTopologyValidatorProvider.class, new CacheTopologyValidatorProvider() {
                /** {@inheritDoc} */
                @Override public TopologyValidator topologyValidator(String cacheName) {
                    return CacheTopologyValidatorPluginProvider.this;
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext pluginCtx) {
        if (ctx.clientNode())
            return;

        log = ctx.log(getClass());

        state = ctx.state().clusterState().state();

        ctx.event().addDiscoveryEventListener(new TopologyChangedEventListener(), TOP_CHANGED_EVTS);

        ctx.discovery().setCustomEventListener(
            ChangeGlobalStateFinishMessage.class,
            (topVer, snd, msg) -> state = msg.state()
        );

        ctx.state().baselineConfiguration().listenAutoAdjustTimeout((name, oldVal, newVal) ->
            validateBaselineConfiguration(ctx.state().isBaselineAutoAdjustEnabled(), newVal));

        ctx.state().baselineConfiguration().listenAutoAdjustEnabled((name, oldVal, newVal) ->
            validateBaselineConfiguration(newVal, ctx.state().baselineAutoAdjustTimeout())
        );

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                /** {@inheritDoc} */
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    dispatcher.registerProperty(deactivateThresholdProp);
                }

                /** {@inheritDoc} */
                @Override public void onReadyToWrite() {
                    float deactivationThreshold = U.isLocalNodeCoordinator(ctx.discovery())
                        ? DFLT_DEACTIVATION_THRESHOLD
                        : deactivateThresholdProp.getOrDefault(0F);

                    if (deactivationThreshold == 0F) {
                        U.warn(log, "Topology Validator will be disabled because it is not configured for the" +
                            " cluster the current node joined. Make sure the Topology Validator plugin is" +
                            " configured on all cluster nodes.");
                    }

                    setDefaultValue(deactivateThresholdProp, deactivationThreshold, log);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return state;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        if (ctx.localNodeId().equals(nodeId))
            state = (ClusterState)data;
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node, Serializable data) throws PluginValidationException {
        if (node.isClient())
            return;

        if (data == null) {
            String msg = "The Topology Validator plugin is not configured for the server node that is" +
                " trying to join the cluster. Since the Topology Validator is only applicable if all server nodes" +
                " in the cluster have one, node join request will be rejected [rejectedNodeId=" + node.id() + ']';

            throw new PluginValidationException(msg, msg, node.id());
        }

        // If the new node is joining but some node failed/left events has not been handled by
        // {@link TopologyChangedEventListener} yet, we cannot guarantee that the {@code state} on the joining node will
        // be consistent with one that on the cluster nodes.
        if (state == ACTIVE) {
            DiscoCache discoCache = ctx.discovery().discoCache(new AffinityTopologyVersion(lastCheckedTopVer, 0));

            if (discoCache != null) {
                for (ClusterNode srv : discoCache.serverNodes()) {
                    if (!ctx.discovery().alive(srv)) {
                        String msg = "Node join request was rejected due to concurrent node left" +
                            " process handling [rejectedNodeId=" + node.id() + ']';

                        throw new PluginValidationException(msg, msg, node.id());
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        return isDisabled() || state != null;
    }

    /** */
    private boolean isDisabled() {
        return deactivateThresholdProp.getOrDefault(0F) == 0F;
    }

    /** */
    private boolean isValidTopology(Collection<? extends BaselineNode> baselineNodes) {
        int aliveBaselineNodes = F.size(baselineNodes, n -> !(n instanceof DetachedClusterNode));

        if (aliveBaselineNodes == 0)
            return true;

        float threshold = deactivateThresholdProp.getOrDefault(DFLT_DEACTIVATION_THRESHOLD);

        assert threshold >= 0.5F && threshold < 1;

        // Actually Ignite considers segmentation as the sequential node failures. So we detect segmentation
        // even if the single node fails and less than half of baseline nodes are alive.
        return aliveBaselineNodes >= ((int)(baselineNodes.size() * threshold)) + 1;
    }

    /** */
    private void validateBaselineConfiguration(Boolean enabled, Long autoAdjustmentTimeout) {
        if (isDisabled() || enabled == null || autoAdjustmentTimeout == null)
            return;

        if (!isBaselineConfigurationCompatible(enabled, autoAdjustmentTimeout)) {
            LT.warn(log, "Topology Validator is currently skipping validation of topology changes because" +
                " Baseline Auto Adjustment with zero timeout is configured for the cluster. Configure Baseline Nodes" +
                " explicitly or set Baseline Auto Adjustment Timeout to greater than zero.");
        }
    }

    /**
     * Current implementation of segmentation detection compares node of each topology with configured baseline nodes.
     * If baseline auto adjustment is configured with zero timeout - baseline is updated on each topology change
     * and the comparison described above makes no sense.
     */
    private boolean isBaselineConfigurationCompatible(boolean enabled, long autoAdjustmentTimeout) {
        return !(enabled && autoAdjustmentTimeout == 0L);
    }

    /** */
    private class TopologyChangedEventListener implements DiscoveryEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            ClusterState locStateCopy = state;

            lastCheckedTopVer = evt.topologyVersion();

            boolean isTopValidationApplicable =
                !isDisabled() &&
                    state == ACTIVE &&
                    evt.type() == EVT_NODE_FAILED &&
                    isBaselineConfigurationCompatible(
                        ctx.state().isBaselineAutoAdjustEnabled(),
                        ctx.state().baselineAutoAdjustTimeout()
                    );

            if (isTopValidationApplicable && !isValidTopology(discoCache.baselineNodes())) {
                locStateCopy = null;

                try {
                    ctx.closure().runLocal(new GridPlainRunnable() {
                        @Override public void run() {
                            try {
                                ctx.cluster().get().state(ACTIVE_READ_ONLY);
                            }
                            catch (Throwable e) {
                                U.error(log,
                                    "Failed to automatically switch state of the segmented cluster to the READ-ONLY" +
                                        " mode. Cache writes were already restricted for all configured caches, but this" +
                                        " step is still required in order to be able to unlock cache writes in the future." +
                                        " Retry this operation manually, if possible [segmentedNodes=" +
                                        F.viewReadOnly(discoCache.allNodes(), F.node2id()) + "]", e);
                            }
                        }
                    }, PUBLIC_POOL);
                }
                catch (Throwable e) {
                    U.error(log, "Failed to schedule cluster state change to the READ-ONLY mode.", e);
                }

                U.warn(log, "Cluster segmentation was detected. Write to all user caches were blocked" +
                    " [segmentedNodes=" + F.viewReadOnly(discoCache.allNodes(), F.node2id()) + ']');
            }

            state = locStateCopy;
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
        }
    }
}
