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
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureManager;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteProductFeatures;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteReleaseFeatures;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
    private volatile boolean isVersionUpgradeEnabled;

    /** */
    public RollingUpgradeProcessor(GridKernalContext ctx) {
        this(ctx, () -> new IgniteProductFeatures(
            IgniteVersionUtils.VER,
            IgniteFeatureSet.buildFrom(IgniteReleaseFeatures.class))
        );
    }

    /** */
    protected RollingUpgradeProcessor(GridKernalContext ctx, Supplier<IgniteProductFeatures> locVerFeaturesProv) {
        super(ctx);

        enableProc = new ClusterVersionUpgradeEnableProcess();
        finalizeProc = new ClusterVersionFinalizationProcess();
        featureMgr = new IgniteFeatureManager(locVerFeaturesProv);
    }

    /** */
    public boolean isVersionUpgradeEnabled() {
        return isVersionUpgradeEnabled;
    }

    /** */
    public void enableVersionUpgrade() throws IgniteCheckedException {
        ctx.security().authorize(ADMIN_ROLLING_UPGRADE);

        if (isVersionUpgradeEnabled)
            return;

        enableProc.start().get();

        if (log.isInfoEnabled())
            log.info("Cluster version Rolling Upgrade was enabled");
    }

    /** */
    public void finalizeClusterVersion() throws IgniteCheckedException {
        ctx.security().authorize(ADMIN_ROLLING_UPGRADE);

        if (!isVersionUpgradeEnabled)
            return;

        finalizeProc.start().get();

        if (log.isInfoEnabled())
            log.info("Cluster version was successfully finalized [activeLogicalVer=" + clusterLogicalVersion() + ']');
    }

    /** */
    public IgniteFeatureManager features() {
        return featureMgr;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_IGNITE_FEATURES, featureMgr.localVersionFeatures().features());

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

        isVersionUpgradeEnabled = gridData.isVersionUpgradeEnabled();
        isNodeFenceActive = gridData.isNodeFenceActive();

        featureMgr.onGridDataReceived(gridData.activeFeatures());
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode joiningNode) {
        synchronized (topGuard) {
            if (isNodeFenceActive) {
                return new IgniteNodeValidationResult(
                    joiningNode.id(),
                    "Node joins are not allowed during cluster version finalization [joiningNode=" + joiningNode + ']');
            }

            if (isVersionUpgradeEnabled) {
                RollingUpgradeState state = detectRollingUpgradeState();

                if (!state.isCompatible(joiningNode)) {
                    return new IgniteNodeValidationResult(
                        joiningNode.id(),
                        "The joining node is incompatible with the current state of the cluster version rolling upgrade being in progress" +
                            " [rollingUpgradeState=" + state +
                            ", joiningNodeVer=" + joiningNode.version() +
                            ", joiningNode=" + joiningNode + ']');
                }

                IgniteProductFeatures locActiveFeatures = featureMgr.activeFeatures();

                if (!locActiveFeatures.isUpgradableTo(extractProductFeatures(joiningNode))) {
                    return new IgniteNodeValidationResult(
                        joiningNode.id(),
                        "Rolling Upgrade is not available between the current cluster logical version and the joining node" +
                            " product version [clusterLogicalVer=" + locActiveFeatures.version() +
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

        boolean isClusterVerHeterogeneous = !maxClusterVer.equals(minClusterVer);

        if (isClusterVerHeterogeneous)
            return new RollingUpgradeState(minClusterVer, maxClusterVer, true);

        IgniteProductVersion logicalVer = clusterLogicalVersion();

        return logicalVer.equals(maxClusterVer)
            ? new RollingUpgradeState(logicalVer, null, false)
            : new RollingUpgradeState(logicalVer, maxClusterVer, false);
    }

    /** */
    private SortedSet<IgniteProductVersion> distinctClusterProductVersions() {
        assert Thread.holdsLock(topGuard);

        TreeSet<IgniteProductVersion> res = new TreeSet<>();

        for (ClusterNode node : ctx.discovery().allNodes())
            res.add(node.version());

        for (ClusterNode node : joiningNodes)
            res.add(node.version());

        return res;
    }

    /** */
    private RollingUpgradeNodeData collectRollingUpgradeNodeData() {
        return new RollingUpgradeNodeData(isVersionUpgradeEnabled, isNodeFenceActive, featureMgr.activeFeatures());
    }

    /** */
    private static IgniteProductFeatures extractProductFeatures(ClusterNode node) {
        return new IgniteProductFeatures(node.version(), node.attribute(ATTR_IGNITE_FEATURES));
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

            return reqId;
        }

        /** */
        private IgniteInternalFuture<Message> execute(Message req) {
            isVersionUpgradeEnabled = true;

            return new GridFinishedFuture<>();
        }

        /** */
        private void finish(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            finishProcess(reqId, null);
        }
    }

    /** */
    private class ClusterVersionFinalizationProcess extends AbstractProcess {
        /** */
        private final DistributedProcess<Message, Message> preparePhase;

        /** */
        private final DistributedProcess<Message, Message> completionPhase;

        /** */
        public ClusterVersionFinalizationProcess() {
            preparePhase = new DistributedProcess<>(
                ctx,
                RU_PREPARE_VERSION_FINALIZATION,
                this::executePreparePhase,
                this::finishPreparePhase,
                (reqId, req) -> new InitMessage<>(reqId, RU_PREPARE_VERSION_FINALIZATION, req, true));

            completionPhase = new DistributedProcess<>(
                ctx,
                RU_COMPLETE_VERSION_FINALIZATION,
                this::executeCompletionPhase,
                this::finishCompletionPhase,
                (reqId, req) -> new InitMessage<>(reqId, RU_COMPLETE_VERSION_FINALIZATION, req, true));
        }

        /** {@inheritDoc} */
        @Override protected UUID startInternal() {
            UUID reqId = UUID.randomUUID();

            preparePhase.start(reqId, null);

            return reqId;
        }

        /** */
        private IgniteInternalFuture<Message> executePreparePhase(Message req) {
            synchronized (topGuard) {
                if (isNodeFenceActive) {
                    return new GridFinishedFuture<>(new IgniteCheckedException(
                        "Cluster version finalization procedure is already in progress"));
                }

                Set<IgniteProductVersion> distinctNodeVersions = distinctClusterProductVersions();

                if (distinctNodeVersions.size() > 1) {
                    return new GridFinishedFuture<>(new IgniteCheckedException(
                        "Cluster version finalization failed. The topology contains nodes running multiple different" +
                            " versions [distinctNodeVersions=" + distinctNodeVersions + "]"
                    ));
                }

                isNodeFenceActive = true;

                return new GridFinishedFuture<>();
            }
        }

        /** */
        private void finishPreparePhase(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            if (!F.isEmpty(errors)) {
                finishProcess(reqId, F.firstValue(errors));
            }
            else if (U.isLocalNodeCoordinator(ctx.discovery()))
                completionPhase.start(reqId, null);
        }

        /** */
        private IgniteInternalFuture<Message> executeCompletionPhase(Message req) {
            featureMgr.activateLocalVersionFeatures();

            isVersionUpgradeEnabled = false;

            isNodeFenceActive = false;

            return new GridFinishedFuture<>();
        }

        /** */
        private void finishCompletionPhase(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            finishProcess(reqId, null);
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
    private static class RollingUpgradeState {
        /** */
        public final IgniteProductVersion sourceVer;

        /** */
        public final @Nullable IgniteProductVersion targetVer;

        /** */
        public final boolean isClusterHeterogeneous;

        /** */
        private RollingUpgradeState(
            IgniteProductVersion sourceVer,
            @Nullable IgniteProductVersion targetVer,
            boolean isClusterVerHeterogeneous
        ) {
            this.sourceVer = sourceVer;
            this.targetVer = targetVer;
            this.isClusterHeterogeneous = isClusterVerHeterogeneous;
        }

        /** */
        boolean isCompatible(ClusterNode joiningNode) {
            IgniteProductVersion joiningNodeVer = joiningNode.version();

            if (targetVer == null)
                return sourceVer.compareTo(joiningNodeVer) <= 0;

            if (joiningNodeVer.equals(sourceVer) || joiningNodeVer.equals(targetVer))
                return true;

            return !isClusterHeterogeneous && targetVer.compareTo(joiningNodeVer) < 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            StringBuilder sb = new StringBuilder("[compatibleProductVersions=");

            if (targetVer == null)
                sb.append("[").append(sourceVer.shortName()).append(" or greater]");
            else {
                sb.append("[").append(sourceVer.shortName()).append(", ").append(targetVer.shortName());

                if (!isClusterHeterogeneous)
                    sb.append(" or greater");

                sb.append("]");
            }

            sb.append("]");

            return sb.toString();
        }
    }
}
