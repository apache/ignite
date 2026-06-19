/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rollingupgrade;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureManager;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteProductFeatures;
import org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeaturesRegistry;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_VALIDATION_FAILED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.ROLLING_UPGRADE_PROC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RU_COMPLETE_VERSION_FINALIZATION;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RU_ENABLE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RU_PREPARE_VERSION_FINALIZATION;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_ROLLING_UPGRADE;

/** */
public class RollingUpgradeProcessor extends GridProcessorAdapter implements DiscoveryNodeValidationProcessor {
    /** */
    private final IgniteFeatureManager featureMgr;

    /** */
    private final ClusterVersionUpgradeEnableProcess enableProc;

    /** */
    private final ClusterVersionFinalizationProcess finalizeProc;

    /** */
    private final Object topGuard = new Object();

    /** */
    private final Set<ClusterNode> joiningNodes = new HashSet<>();

    /** */
    private volatile boolean isNodeFenceActive;

    /** */
    private volatile boolean isVerUpgradeEnabled;

    /** */
    public RollingUpgradeProcessor(GridKernalContext ctx) {
        this(ctx, () -> new IgniteProductFeatures(
            IgniteVersionUtils.VER,
            IgniteFeatureSet.buildFrom(SupportedFeaturesRegistry.class))
        );
    }

    /** */
    protected RollingUpgradeProcessor(GridKernalContext ctx, Supplier<IgniteProductFeatures> locVerFeaturesProv) {
        super(ctx);

        enableProc = new ClusterVersionUpgradeEnableProcess();
        finalizeProc = new ClusterVersionFinalizationProcess();
        featureMgr = new IgniteFeatureManager(ctx, locVerFeaturesProv);
    }

    /** @return Whether nodes running a higher Ignite version are allowed to join the cluster. */
    public boolean isVersionUpgradeEnabled() {
        return isVerUpgradeEnabled;
    }

    /**
     * Allows nodes running a higher Ignite version to join the cluster.
     * Until the cluster version is finalized, nodes running a higher version operate in
     * a compatibility mode that emulates the behavior of the current cluster version.
     */
    public void enableVersionUpgrade() throws IgniteCheckedException {
        ctx.security().authorize(ADMIN_ROLLING_UPGRADE);

        if (isVerUpgradeEnabled)
            return;

        enableProc.start().get();

        if (log.isInfoEnabled())
            log.info("Cluster version Rolling Upgrade was successfully enabled");
    }

    /**
     * Tries to finalize the cluster version.
     *
     * <p>If all cluster nodes are running the same Ignite version, this method:</p>
     * <ol>
     *     <li>Prevents nodes running a higher Ignite version from joining the cluster.</li>
     *     <li>Activates all {@link IgniteFeature}s supported by the finalized cluster version.</li>
     * </ol>
     *
     * <p>If the cluster contains nodes running different Ignite versions, the operation fails.</p>
     */
    public void finalizeClusterVersion() throws IgniteCheckedException {
        ctx.security().authorize(ADMIN_ROLLING_UPGRADE);

        if (!isVerUpgradeEnabled)
            return;

        finalizeProc.start().get();

        if (log.isInfoEnabled())
            log.info("Cluster version was successfully finalized [activeLogicalVer=" + clusterLogicalVersion() + ']');
    }

    /** */
    public IgniteFeatureManager features() {
        return featureMgr;
    }

    /** */
    RollingUpgradeState state() {
        synchronized (topGuard) {
            return detectRollingUpgradeState();
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(
            ATTR_IGNITE_FEATURES,
            U.marshal(ctx.marshallerContext().jdkMarshaller(), featureMgr.localVersionFeatures().features()));

        ctx.event().addLocalEventListener(
            evt -> {
                synchronized (topGuard) {
                    joiningNodes.remove(((DiscoveryEvent)evt).eventNode());
                }
            },
            EVT_NODE_JOINED,
            EVT_NODE_FAILED,
            EVT_NODE_LEFT,
            EVT_NODE_VALIDATION_FAILED
        );
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataExchangeType discoveryDataType() {
        return ROLLING_UPGRADE_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (ctx.clientNode())
            return;

        int cmpId = discoveryDataType().ordinal();

        if (!dataBag.commonDataCollectedFor(cmpId))
            dataBag.addGridCommonData(cmpId, collectRollingUpgradeNodeData());
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        RollingUpgradeNodeData gridData = (RollingUpgradeNodeData)data.commonData();

        isVerUpgradeEnabled = gridData.isVersionUpgradeEnabled();
        isNodeFenceActive = gridData.isNodeFenceActive();

        featureMgr.onGridDataReceived(gridData.activeFeatures());
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode joiningNode) {
        synchronized (topGuard) {
            if (isNodeFenceActive) {
                return new IgniteNodeValidationResult(
                    joiningNode.id(),
                    "Node joins are not allowed during cluster version finalization. Retry the node join procedure after" +
                        " cluster version finalization process is complete [joiningNode=" + joiningNode + ']');
            }

            if (isVerUpgradeEnabled) {
                RollingUpgradeState state = detectRollingUpgradeState();

                if (!state.isCompatible(joiningNode)) {
                    return new IgniteNodeValidationResult(
                        joiningNode.id(),
                        "The joining node version is incompatible with the current state of the cluster version Rolling" +
                            " Upgrade being in progress. Upgrade the joining node version and retry the node join procedure" +
                            " [rollingUpgradeState=" + state +
                            ", joiningNodeVer=" + joiningNode.version() +
                            ", joiningNode=" + joiningNode + ']');
                }

                IgniteProductFeatures locActiveFeatures = featureMgr.activeFeatures();

                IgniteProductFeatures joiningNodeProductFeatures;

                try {
                    joiningNodeProductFeatures = extractProductFeatures(joiningNode);
                }
                catch (IgniteCheckedException e) {
                    return new IgniteNodeValidationResult(
                        joiningNode.id(),
                        "Failed to resolve joining node product features" +
                            " [joiningNode=" + joiningNode + ", errMsg=" + e.getMessage() + ']');
                }

                if (!locActiveFeatures.isUpgradableTo(joiningNodeProductFeatures)) {
                    return new IgniteNodeValidationResult(
                        joiningNode.id(),
                        "Rolling Upgrade is not available between the current cluster logical version and the joining node" +
                            " product version. Refer to the documentation to determine between which versions of Ignite" +
                            " a Rolling Upgrade is possible and retry the node join procedure" +
                            " [clusterLogicalVer=" + locActiveFeatures.version() +
                            ", joiningNodeVer=" + joiningNode.version() +
                            ", joiningNode=" + joiningNode + ']');
                }
            }
            else if (!joiningNode.version().equals(localProductVersion())) {
                return new IgniteNodeValidationResult(
                    joiningNode.id(),
                    "The joining node version differs from the version of the cluster" +
                        " [clusterVer=" + localProductVersion() +
                        ", joiningNodeVer=" + joiningNode.version() +
                        ", joiningNode=" + joiningNode + ']');
            }

            joiningNodes.add(joiningNode);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        enableProc.onDisconnected();
        finalizeProc.onDisconnected();
    }

    /** */
    private IgniteProductVersion localProductVersion() {
        return featureMgr.localVersionFeatures().version();
    }

    /** */
    private IgniteProductVersion clusterLogicalVersion() {
        return featureMgr.activeFeatures().version();
    }

    /** */
    private RollingUpgradeState detectRollingUpgradeState() {
        SortedSet<IgniteProductVersion> clusterVers = distinctClusterProductVersions();

        assert !clusterVers.isEmpty() && clusterVers.size() <= 2;

        IgniteProductVersion minClusterVer = clusterVers.first();
        IgniteProductVersion maxClusterVer = clusterVers.last();

        if (!minClusterVer.equals(maxClusterVer))
            return new RollingUpgradeState(minClusterVer, maxClusterVer, false);

        IgniteProductVersion logicalVer = clusterLogicalVersion();

        return new RollingUpgradeState(
            logicalVer,
            logicalVer.equals(maxClusterVer) ? null : maxClusterVer,
            true);
    }

    /** */
    private SortedSet<IgniteProductVersion> distinctClusterProductVersions() {
        assert Thread.holdsLock(topGuard);

        TreeSet<IgniteProductVersion> res = new TreeSet<>();

        for (ClusterNode node : ctx.discovery().discoverySpiRemoteNodes())
            res.add(node.version());

        res.add(ctx.discovery().localNode().version());

        for (ClusterNode node : joiningNodes)
            res.add(node.version());

        return res;
    }

    /** */
    private RollingUpgradeNodeData collectRollingUpgradeNodeData() {
        return new RollingUpgradeNodeData(isVerUpgradeEnabled, isNodeFenceActive, featureMgr.activeFeatures());
    }

    /** */
    private IgniteProductFeatures extractProductFeatures(ClusterNode node) throws IgniteCheckedException {
        byte[] attrVal = node.attribute(ATTR_IGNITE_FEATURES);

        IgniteFeatureSet features = U.unmarshal(ctx.marshallerContext().jdkMarshaller(), attrVal, U.resolveClassLoader(ctx.config()));

        return new IgniteProductFeatures(node.version(), features);
    }

    /** */
    private static Throwable firstError(Map<UUID, Throwable> errors) {
        return F.isEmpty(errors) ? null : F.firstValue(errors);
    }

    /** */
    private class ClusterVersionUpgradeEnableProcess extends AbstractProcess {
        /** */
        private final DistributedProcess<Message, Message> distributedProc;

        /** */
        public ClusterVersionUpgradeEnableProcess() {
            distributedProc = new DistributedProcess<>(
                ctx,
                RU_ENABLE,
                this::execute,
                this::finish,
                (reqId, req) -> new InitMessage<>(reqId, RU_ENABLE, req, true));
        }

        /** {@inheritDoc} */
        @Override protected UUID startInternal() {
            UUID reqId = UUID.randomUUID();

            distributedProc.start(reqId, null);

            if (log.isInfoEnabled())
                log.info("Cluster version upgrade enable process has been started [procId=" + reqId + "]");

            return reqId;
        }

        /** */
        private IgniteInternalFuture<Message> execute(UUID ignored, Message req) {
            isVerUpgradeEnabled = true;

            return new GridFinishedFuture<>();
        }

        /** */
        private void finish(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            finishProcess(reqId, firstError(errors));
        }
    }

    /** */
    private class ClusterVersionFinalizationProcess extends AbstractProcess {
        /** */
        private final DistributedProcess<Message, Message> preparePhase;

        /** */
        private final DistributedProcess<Message, Message> completePhase;

        /** */
        @Nullable private volatile UUID activeProcId;

        /** */
        public ClusterVersionFinalizationProcess() {
            preparePhase = new DistributedProcess<>(
                ctx,
                RU_PREPARE_VERSION_FINALIZATION,
                this::executePreparePhase,
                this::finishPreparePhase,
                (reqId, req) -> new InitMessage<>(reqId, RU_PREPARE_VERSION_FINALIZATION, req, true));

            completePhase = new DistributedProcess<>(
                ctx,
                RU_COMPLETE_VERSION_FINALIZATION,
                this::executeCompletePhase,
                this::finishCompletePhase,
                (reqId, req) -> new InitMessage<>(reqId, RU_COMPLETE_VERSION_FINALIZATION, req, true));
        }

        /** {@inheritDoc} */
        @Override protected UUID startInternal() {
            UUID reqId = UUID.randomUUID();

            preparePhase.start(reqId, null);

            if (log.isInfoEnabled())
                log.info("Cluster version finalization process has been started [procId=" + reqId + ']');

            return reqId;
        }

        /** {@inheritDoc} */
        @Override protected void finishProcess(UUID reqId, @Nullable Throwable err) {
            if (err != null)
                U.error(log, "Cluster version finalization process failed [procId=" + reqId + ']', err);

            // We rely on the guarantee that {@code activeProcId} is only accessed from the Discovery thread, just like
            // the current method. Therefore, synchronization is not required.
            if (reqId.equals(activeProcId))
                activeProcId = null;

            super.finishProcess(reqId, err);
        }

        /** {@inheritDoc} */
        @Override protected void onDisconnected() {
            activeProcId = null;

            super.onDisconnected();
        }

        /** */
        private IgniteInternalFuture<Message> executePreparePhase(UUID reqId, Message req) {
            if (activeProcId != null) {
                U.error(log, "Failed to handle cluster version finalization request. Another process is " +
                    "already in progress [curProcId=" + reqId + ", activeProcId=" + activeProcId + ']');

                return new GridFinishedFuture<>(new IgniteCheckedException(
                    "Cluster version finalization process is already in progress"));
            }

            activeProcId = reqId;

            synchronized (topGuard) {
                Set<IgniteProductVersion> distinctNodeVersions = distinctClusterProductVersions();

                if (distinctNodeVersions.size() > 1) {
                    return new GridFinishedFuture<>(new IgniteCheckedException(
                        "Cluster version finalization failed. The topology contains nodes running multiple different" +
                            " versions. Retry the operation after all cluster nodes are upgraded to the same version " +
                            "[distinctNodeVersions=" + distinctNodeVersions + "]"
                    ));
                }

                isNodeFenceActive = true;

                return new GridFinishedFuture<>();
            }
        }

        /** */
        private void finishPreparePhase(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            if (!F.isEmpty(errors)) {
                if (reqId.equals(activeProcId))
                    isNodeFenceActive = false;

                finishProcess(reqId, firstError(errors));
            }
            else if (U.isLocalNodeCoordinator(ctx.discovery()))
                completePhase.start(reqId, null);
        }

        /** */
        private IgniteInternalFuture<Message> executeCompletePhase(UUID ignored, Message req) {
            featureMgr.activateLocalVersionFeatures();

            isVerUpgradeEnabled = false;

            isNodeFenceActive = false;

            return new GridFinishedFuture<>();
        }

        /** */
        private void finishCompletePhase(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            finishProcess(reqId, firstError(errors));
        }
    }

    /** */
    private static class RollingUpgradeNodeData implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final boolean isVersionUpgradeEnabled;

        /** */
        private final IgniteProductFeatures activeFeatures;

        /** */
        private final boolean isNodeFenceActive;

        /** */
        private RollingUpgradeNodeData(boolean isVersionUpgradeEnabled, boolean isNodeFenceActive, IgniteProductFeatures activeFeatures) {
            this.isVersionUpgradeEnabled = isVersionUpgradeEnabled;
            this.isNodeFenceActive = isNodeFenceActive;
            this.activeFeatures = activeFeatures;
        }

        /** */
        private boolean isVersionUpgradeEnabled() {
            return isVersionUpgradeEnabled;
        }

        /** */
        private boolean isNodeFenceActive() {
            return isNodeFenceActive;
        }

        /** */
        private IgniteProductFeatures activeFeatures() {
            return activeFeatures;
        }
    }

    /** */
    static class RollingUpgradeState {
        /** */
        final IgniteProductVersion srcVer;

        /** */
        @Nullable final IgniteProductVersion targetVer;

        /** Whether all cluster nodes running the same Ignite version. */
        final boolean isClusterVerHomogenous;

        /** */
        private RollingUpgradeState(
            IgniteProductVersion srcVer,
            @Nullable IgniteProductVersion targetVer,
            boolean isClusterVerHomogenous
        ) {
            this.srcVer = srcVer;
            this.targetVer = targetVer;
            this.isClusterVerHomogenous = isClusterVerHomogenous;
        }

        /** */
        boolean isCompatible(ClusterNode joiningNode) {
            IgniteProductVersion joiningNodeVer = joiningNode.version();

            if (isClusterVerHomogenous)
                return srcVer.compareTo(joiningNodeVer) <= 0;

            return joiningNodeVer.equals(srcVer) || joiningNodeVer.equals(targetVer);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            StringBuilder sb = new StringBuilder("[compatibleProductVersions=");

            if (isClusterVerHomogenous)
                sb.append("[").append(srcVer.semanticName()).append(" or greater]");
            else {
                String targetVerName = targetVer == null ? null : targetVer.semanticName();

                sb.append("[").append(srcVer.semanticName()).append(", ").append(targetVerName).append("]");
            }

            sb.append("]");

            return sb.toString();
        }
    }
}
