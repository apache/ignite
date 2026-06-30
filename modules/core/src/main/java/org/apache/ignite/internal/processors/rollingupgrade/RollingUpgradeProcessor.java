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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteComponentFeatures;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureManager;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatures;
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
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RU_ABORT_VERSION_FINALIZATION;
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
    private final ClusterVersionFinalizationAbortProcess finalizeAbortProc;

    /** */
    private final Object topGuard = new Object();

    /** */
    private final Set<ClusterNode> joiningNodes = new HashSet<>();

    /** */
    private volatile boolean isNodeFenceActive;

    /** */
    private volatile boolean isVerUpgradeEnabled;

    /**
     * We rely on the guarantee that this varable is only accessed from the Discovery thread.
     * Therefore, synchronization is not required.
     */
    @Nullable private volatile UUID curFinalizeProcId;

    /** */
    public RollingUpgradeProcessor(GridKernalContext ctx) {
        this(ctx, new IgniteComponentFeatures(
            IgniteCoreFeature.COMPONENT_NAME,
            IgniteVersionUtils.VER,
            IgniteFeatureSet.buildFrom(SupportedFeaturesRegistry.class))
        );
    }

    /** */
    protected RollingUpgradeProcessor(GridKernalContext ctx, IgniteComponentFeatures coreFeatures) {
        super(ctx);

        enableProc = new ClusterVersionUpgradeEnableProcess();
        finalizeProc = new ClusterVersionFinalizationProcess();
        finalizeAbortProc = new ClusterVersionFinalizationAbortProcess();
        featureMgr = new IgniteFeatureManager(ctx, coreFeatures);
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
            log.info("Cluster version was successfully finalized [componentVersions=" + featureMgr.activeFeatures() + ']');
    }

    /** */
    public void abortClusterVersionFinalization() throws IgniteCheckedException {
        ctx.security().authorize(ADMIN_ROLLING_UPGRADE);

        if (!isVerUpgradeEnabled)
            return;

        finalizeAbortProc.start().get();

        if (log.isInfoEnabled())
            log.info("Cluster version finalization has been aborted");
    }

    /** */
    public IgniteFeatureManager features() {
        return featureMgr;
    }

    /** */
    IgniteComponentUpgradeState state(String cmpName) {
        synchronized (topGuard) {
            return detectComponentUpgradeState(clusterFeatures(), cmpName);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(
            ATTR_IGNITE_FEATURES,
            U.marshal(
                ctx.marshallerContext().jdkMarshaller(),
                featureMgr.localVersionFeatures().values().toArray(new IgniteComponentFeatures[0]))
        );

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
            dataBag.addGridCommonData(cmpId, collectRollingUpgradeClusterData());
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        RollingUpgradeClusterData gridData = data.commonData();

        isVerUpgradeEnabled = gridData.isVersionUpgradeEnabled;
        curFinalizeProcId = gridData.curFinalizeProcId;
        isNodeFenceActive = gridData.isNodeFenceActive;

        featureMgr.onGridDataReceived(new IgniteFeatures(gridData.activeFeatures));
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

            IgniteFeatures joiningNodeFeatures;

            try {
                joiningNodeFeatures = extractNodeFeatures(joiningNode);
            }
            catch (IgniteCheckedException e) {
                return new IgniteNodeValidationResult(
                    joiningNode.id(),
                    "Failed to resolve joining node features [joiningNode=" + joiningNode + ", errMsg=" + e.getMessage() + ']');
            }

            if (isVerUpgradeEnabled) {
                if (!joiningNode.isClient() && !joiningNodeFeatures.components().containsAll(featureMgr.activeFeatures().components())) {
                    return new IgniteNodeValidationResult(
                        joiningNode.id(),
                        "Some components active in the cluster are not configured on the joining server node" +
                            " [clusterComponents=" + featureMgr.activeFeatures().components() +
                            ", joiningNodeComponents=" + joiningNodeFeatures.components() +
                            ", joiningNode=" + joiningNode + ']'
                    );
                }

                Map<ClusterNode, IgniteFeatures> clusterFeatures = clusterFeatures();

                for (IgniteComponentFeatures rmtCmpFeatures : joiningNodeFeatures.values()) {
                    IgniteComponentUpgradeState state = detectComponentUpgradeState(clusterFeatures, rmtCmpFeatures.componentName());

                    if (!state.isCompatible(rmtCmpFeatures.version())) {
                        return new IgniteNodeValidationResult(
                            joiningNode.id(),
                            "The joining node is incompatible with the current state of the version Rolling Upgrade being " +
                                "in progress. Upgrade the joining node component versions and retry the node join procedure" +
                                " [rollingUpgradeState=" + state +
                                ", joiningNodeComponentVer=" + rmtCmpFeatures.version() +
                                ", joiningNode=" + joiningNode + ']');
                    }

                    IgniteComponentFeatures locCmpFeatures = featureMgr.activeComponentFeatures(rmtCmpFeatures.componentName());

                    if (locCmpFeatures != null && !locCmpFeatures.isUpgradableTo(rmtCmpFeatures)) {
                        return new IgniteNodeValidationResult(
                            joiningNode.id(),
                            "Ignite component Rolling Upgrade is not supported between the component version active in the cluster " +
                                "and the version running on the joining node. Refer to the documentation for supported Rolling Upgrade " +
                                "version combinations, and then retry the node join operation" +
                                " [componentName=" + locCmpFeatures.componentName() +
                                ", clusterComponentVer=" + locCmpFeatures.version() +
                                ", joiningNodeComponentVer=" + rmtCmpFeatures.version() +
                                ", joiningNode=" + joiningNode + ']');
                    }
                }
            }
            else if (!joiningNodeComponentVersionsMatchCluster(joiningNode, joiningNodeFeatures)) {
                return new IgniteNodeValidationResult(
                    joiningNode.id(),
                    "One or more component versions on the joining node differ from the corresponding versions active in the cluster" +
                        " [clusterComponentVers=" + featureMgr.activeFeatures() +
                        ", joiningNodeComponentVers=" + joiningNodeFeatures +
                        ", joiningNode=" + joiningNode + ']');
            }

            joiningNodes.add(joiningNode);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        String errMsg = "Client node has disconnected";

        enableProc.abort(errMsg);
        finalizeProc.abort(errMsg);
        finalizeAbortProc.abort(errMsg);
    }

    /** */
    private IgniteComponentUpgradeState detectComponentUpgradeState(Map<ClusterNode, IgniteFeatures> clusterFeatures, String cmpName) {
        SortedSet<IgniteProductVersion> clusterCmpVersions = distinctClusterComponentVersions(clusterFeatures, cmpName);

        assert !clusterCmpVersions.isEmpty() && clusterCmpVersions.size() <= 2;

        IgniteProductVersion minCmpVer = clusterCmpVersions.first();
        IgniteProductVersion maxCmpVer = clusterCmpVersions.last();

        if (!Objects.equals(minCmpVer, maxCmpVer))
            return new IgniteComponentUpgradeState(cmpName, minCmpVer, maxCmpVer, false);

        IgniteComponentFeatures activeCompFeatures = featureMgr.activeComponentFeatures(cmpName);

        IgniteProductVersion logicalCmpVer = activeCompFeatures == null ? null : activeCompFeatures.version();

        return new IgniteComponentUpgradeState(
            cmpName,
            logicalCmpVer,
            Objects.equals(logicalCmpVer, maxCmpVer) ? null : maxCmpVer,
            true);
    }

    /** */
    private SortedSet<IgniteProductVersion> distinctClusterComponentVersions(
        Map<ClusterNode, IgniteFeatures> clusterFeatures,
        String cmpName
    ) {
        SortedSet<IgniteProductVersion> distinctCmpVersions = new TreeSet<>(Comparator.nullsFirst(Comparator.naturalOrder()));

        clusterFeatures.forEach((node, nodeFeatures) -> {
            IgniteComponentFeatures cmpFeatures = nodeFeatures.componentFeatures(cmpName);

            if (node.isClient() && cmpFeatures == null)
                return; // Components are optional on client nodes, even when they are configured on servers.

            distinctCmpVersions.add(cmpFeatures == null ? null : cmpFeatures.version());
        });

        return distinctCmpVersions;
    }

    /** */
    private boolean joiningNodeComponentVersionsMatchCluster(ClusterNode joiningNode, IgniteFeatures joiningNodeFeatures) {
        IgniteFeatures locFeatures = featureMgr.localVersionFeatures();

        return joiningNode.isClient()
            ? locFeatures.containsAll(joiningNodeFeatures)
            : locFeatures.equals(joiningNodeFeatures);
    }

    /** */
    private boolean isReadyForVersionFinalization() {
        Map<ClusterNode, IgniteFeatures> clusterFeatures = clusterFeatures();

        Set<IgniteFeatures> distinctFeaturesOnServers = new HashSet<>();
        Set<IgniteFeatures> distinctFeaturesOnClients = new HashSet<>();

        clusterFeatures.forEach((node, features) -> {
            if (!node.isClient())
                distinctFeaturesOnServers.add(features);
            else
                distinctFeaturesOnClients.add(features);
        });

        if (distinctFeaturesOnServers.size() != 1)
            return false;

        IgniteFeatures srvFeatures = F.first(distinctFeaturesOnServers);

        return distinctFeaturesOnClients.stream().allMatch(srvFeatures::containsAll);
    }

    /** */
    private RollingUpgradeClusterData collectRollingUpgradeClusterData() {
        return new RollingUpgradeClusterData(
            isVerUpgradeEnabled,
            curFinalizeProcId,
            isNodeFenceActive,
            featureMgr.activeFeatures().values()
        );
    }

    /** */
    private IgniteFeatures extractNodeFeatures(ClusterNode node) throws IgniteCheckedException {
        byte[] attrVal = node.attribute(ATTR_IGNITE_FEATURES);

        IgniteComponentFeatures[] nodeFeatures = U.unmarshal(
            ctx.marshallerContext().jdkMarshaller(),
            attrVal,
            U.resolveClassLoader(ctx.config()));

        return new IgniteFeatures(List.of(nodeFeatures));
    }

    /** */
    private static Throwable firstError(Map<UUID, Throwable> errors) {
        return F.isEmpty(errors) ? null : F.firstValue(errors);
    }

    /** */
    private Map<ClusterNode, IgniteFeatures> clusterFeatures() {
        assert Thread.holdsLock(topGuard);

        try {
            Map<ClusterNode, IgniteFeatures> res = new HashMap<>();

            for (ClusterNode node : clusterNodes())
                res.put(node, extractNodeFeatures(node));

            return res;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to resolve cluster features", e);

            throw new IgniteException("Failed to resolve cluster features", e);
        }
    }

    /** */
    private Set<ClusterNode> clusterNodes() {
        assert Thread.holdsLock(topGuard);

        Set<ClusterNode> clusterNodes = new HashSet<>(ctx.discovery().discoverySpiRemoteNodes());

        clusterNodes.add(ctx.discovery().localNode());
        clusterNodes.addAll(joiningNodes);

        return clusterNodes;
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

            if (reqId.equals(curFinalizeProcId))
                curFinalizeProcId = null;

            super.finishProcess(reqId, err);
        }

        /** {@inheritDoc} */
        @Override protected void abort(String reason) {
            U.warn(log, "Cluster version finalization process has been aborted [procId=" + curFinalizeProcId + ", reason=" + reason + ']');

            curFinalizeProcId = null;
            isNodeFenceActive = false;

            super.abort(reason);
        }

        /** */
        boolean isInProgress() {
            return isVerUpgradeEnabled && curFinalizeProcId != null;
        }

        /** */
        private IgniteInternalFuture<Message> executePreparePhase(UUID reqId, Message req) {
            if (curFinalizeProcId != null) {
                U.error(log, "Failed to handle cluster version finalization request. Another process is " +
                    "already in progress [curProcId=" + reqId + ", activeProcId=" + curFinalizeProcId + ']');

                return new GridFinishedFuture<>(new IgniteException("Cluster version finalization process is already in progress"));
            }

            curFinalizeProcId = reqId;

            synchronized (topGuard) {
                if (!isReadyForVersionFinalization())
                    return new GridFinishedFuture<>(new IgniteException(
                    "Cluster version finalization failed. The cluster contains nodes running" +
                        " different versions of one or more components. Retry the operation after upgrading" +
                        " all cluster node components to the same version" +
                        " [clusterFeatures=" + clusterFeatures().entrySet().stream().collect(
                        Collectors.toMap(e -> e.getKey().id(), Map.Entry::getValue)) + ']'));

                isNodeFenceActive = true;

                return new GridFinishedFuture<>();
            }
        }

        /** */
        private void finishPreparePhase(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            if (!F.isEmpty(errors)) {
                if (reqId.equals(curFinalizeProcId))
                    isNodeFenceActive = false;

                finishProcess(reqId, firstError(errors));
            }
            else if (reqId.equals(curFinalizeProcId) && U.isLocalNodeCoordinator(ctx.discovery()))
                completePhase.start(reqId, null);
        }

        /** */
        private IgniteInternalFuture<Message> executeCompletePhase(UUID reqId, Message req) {
            if (!reqId.equals(curFinalizeProcId)) {
                // This condition is guaranteed to occur only when cluster version finalization is aborted
                // after the prepare phase has completed successfully but before the completion phase begins.
                // Aborting cluster version finalization is mutually exclusive with the finalization completion
                // phase and is applied consistently across all cluster nodes.
                return new GridFinishedFuture<>(new IgniteException(
                    "Failed to complete the cluster version finalization operation." +
                        " The operation may have been aborted by an administrator. Retry the operation if possible"
                ));
            }

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
    private class ClusterVersionFinalizationAbortProcess extends AbstractProcess {
        /** */
        private final DistributedProcess<Message, Message> distributedProc;

        /** */
        public ClusterVersionFinalizationAbortProcess() {
            distributedProc = new DistributedProcess<>(
                ctx,
                RU_ABORT_VERSION_FINALIZATION,
                this::execute,
                this::finish,
                (reqId, req) -> new InitMessage<>(reqId, RU_ABORT_VERSION_FINALIZATION, req, true));
        }

        /** {@inheritDoc} */
        @Override protected UUID startInternal() {
            UUID reqId = UUID.randomUUID();

            distributedProc.start(reqId, null);

            return reqId;
        }

        /** */
        private IgniteInternalFuture<Message> execute(UUID ignored, Message req) {
            if (finalizeProc.isInProgress())
                finalizeProc.abort("Operation has been aborted by administrator");

            return new GridFinishedFuture<>();
        }

        /** */
        private void finish(UUID reqId, Map<UUID, Message> responses, Map<UUID, Throwable> errors) {
            finishProcess(reqId, firstError(errors));
        }
    }
}
