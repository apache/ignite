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

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponse;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cluster.baseline.autoadjust.BaselineAutoAdjustStatus;
import org.apache.ignite.internal.processors.cluster.baseline.autoadjust.ChangeTopologyWatcher;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.STATE_PROC;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.extractDataStorage;

/**
 *
 */
public class GridClusterStateProcessor extends GridProcessorAdapter implements IGridClusterStateProcessor, MetastorageLifecycleListener {
    /** */
    private static final String METASTORE_CURR_BLT_KEY = "metastoreBltKey";

    /** */
    private boolean inMemoryMode;

    /**
     * Compatibility mode flag. When node detects it runs in heterogeneous cluster (nodes of different versions),
     * it should skip baseline topology operations.
     */
    private volatile boolean compatibilityMode;

    /** */
    private volatile DiscoveryDataClusterState globalState;

    /** */
    private final BaselineTopologyHistory bltHist = new BaselineTopologyHistory();

    /** Local action future. */
    private final AtomicReference<GridChangeGlobalStateFuture> stateChangeFut = new AtomicReference<>();

    /** */
    private final ConcurrentMap<UUID, GridFutureAdapter<Void>> transitionFuts = new ConcurrentHashMap<>();

    /** Future initialized if node joins when cluster state change is in progress. */
    private TransitionOnJoinWaitFuture joinFut;

    /** Process. */
    @GridToStringExclude
    private GridCacheProcessor cacheProc;

    /** Shared context. */
    @GridToStringExclude
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** Fully initialized metastorage. */
    @GridToStringExclude
    private ReadWriteMetastorage metastorage;

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** Watcher of topology change for baseline auto-adjust. */
    private ChangeTopologyWatcher changeTopologyWatcher;

    /** Distributed baseline configuration. */
    private DistributedBaselineConfiguration distributedBaselineConfiguration;

    /** Minimal IgniteProductVersion supporting BaselineTopology */
    private static final IgniteProductVersion MIN_BLT_SUPPORTING_VER = IgniteProductVersion.fromString("2.4.0");

    /** Listener. */
    private final GridLocalEventListener lsr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt != null;

            final DiscoveryEvent e = (DiscoveryEvent)evt;

            assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED : this;

            final GridChangeGlobalStateFuture f = stateChangeFut.get();

            if (f != null) {
                f.initFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> fut) {
                        f.onNodeLeft(e);
                    }
                });
            }
        }
    };

    /**
     * @param ctx Kernal context.
     */
    public GridClusterStateProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);

        distributedBaselineConfiguration = new DistributedBaselineConfiguration(
            ctx.internalSubscriptionProcessor(),
            ctx,
            ctx.log(DistributedBaselineConfiguration.class)
        );
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        if (!isBaselineAutoAdjustEnabled() || baselineAutoAdjustTimeout() != 0)
            return null;

        Collection<ClusterNode> nodes = ctx.discovery().aliveServerNodes();

        //Any node allowed to join if cluster has at least one persist node.
        if (nodes.stream().anyMatch(serNode -> CU.isPersistenceEnabled(extractDataStorage(
            serNode,
            ctx.marshallerContext().jdkMarshaller(),
            U.resolveClassLoader(ctx.config()))
        )))
            return null;

        DataStorageConfiguration crdDsCfg = extractDataStorage(
            node,
            ctx.marshallerContext().jdkMarshaller(),
            U.resolveClassLoader(ctx.config())
        );

        if (!CU.isPersistenceEnabled(crdDsCfg))
            return null;

        return new IgniteNodeValidationResult(
            node.id(),
            "Joining persistence node to in-memory cluster couldn't be allowed " +
            "due to baseline auto-adjust is enabled and timeout equal to 0"
        );
    }

    /**
     * @return {@code True} if {@link IGridClusterStateProcessor} has detected that cluster is working
     * in compatibility mode (nodes of different versions are joined to the cluster).
     */
    public boolean compatibilityMode() {
        return compatibilityMode;
    }

    /** {@inheritDoc} */
    @Override public boolean publicApiReadOnlyMode() {
        return globalState.readOnly();
    }

    /** {@inheritDoc} */
    @Override public long readOnlyModeStateChangeTime() {
        return globalState.readOnlyModeChangeTime();
    }

    /** {@inheritDoc} */
    @Override public boolean publicApiActiveState(boolean waitForTransition) {
        return publicApiActiveStateAsync(waitForTransition).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> publicApiActiveStateAsync(boolean asyncWaitForTransition) {
        if (ctx.isDaemon())
            return sendComputeCheckGlobalState();

        DiscoveryDataClusterState globalState = this.globalState;

        assert globalState != null;

        if (globalState.transition() && globalState.active()) {
            Boolean transitionRes = globalState.transitionResult();

            if (transitionRes != null)
                return new IgniteFinishedFutureImpl<>(transitionRes);
            else {
                GridFutureAdapter<Void> fut = transitionFuts.get(globalState.transitionRequestId());

                if (fut != null) {
                    if (asyncWaitForTransition) {
                         return new IgniteFutureImpl<>(fut.chain(new C1<IgniteInternalFuture<Void>, Boolean>() {
                            @Override public Boolean apply(IgniteInternalFuture<Void> fut) {
                                Boolean res = globalState.transitionResult();

                                assert res != null;

                                return res;
                            }
                        }));
                    }
                    else
                        return new IgniteFinishedFutureImpl<>(globalState.baselineChanged());
                }

                transitionRes = globalState.transitionResult();

                assert transitionRes != null;

                return new IgniteFinishedFutureImpl<>(transitionRes);
            }
        }
        else
            return new IgniteFinishedFutureImpl<>(globalState.active());
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        BaselineTopology blt = (BaselineTopology) metastorage.read(METASTORE_CURR_BLT_KEY);

        if (blt != null) {
            if (log.isInfoEnabled())
                U.log(log, "Restoring history for BaselineTopology[id=" + blt.id() + "]");

            bltHist.restoreHistory(metastorage, blt.id());
        }

        onStateRestored(blt);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastorage = metastorage;

        if (compatibilityMode) {
            if (log.isInfoEnabled())
                log.info("BaselineTopology won't be stored as this node is running in compatibility mode");

            return;
        }

        writeBaselineTopology(globalState.baselineTopology(), null);

        bltHist.flushHistoryItems(metastorage);
    }

    /**
     * Resets branching history on current BaselineTopology.
     *
     * @throws IgniteCheckedException If write to metastore has failed.
     */
    public void resetBranchingHistory(long newBranchingHash) throws IgniteCheckedException {
        if (!compatibilityMode()) {
            globalState.baselineTopology().resetBranchingHistory(newBranchingHash);

            writeBaselineTopology(globalState.baselineTopology(), null);

            U.log(log,
                String.format("Branching history of current BaselineTopology is reset to the value %d", newBranchingHash));
        }
    }

    /**
     * @param blt Blt.
     */
    private void writeBaselineTopology(BaselineTopology blt, BaselineTopologyHistoryItem prevBltHistItem) throws IgniteCheckedException {
        assert metastorage != null;

        if (inMemoryMode)
            return;

        sharedCtx.database().checkpointReadLock();

        try {
            if (blt != null) {
                if (log.isInfoEnabled()) {
                    U.log(log, "Writing BaselineTopology[id=" + blt.id() + "]");

                    if (prevBltHistItem != null)
                        U.log(log, "Writing BaselineTopologyHistoryItem[id=" + prevBltHistItem.id() + "]");
                }

                bltHist.writeHistoryItem(metastorage, prevBltHistItem);

                metastorage.write(METASTORE_CURR_BLT_KEY, blt);
            }
            else {
                if (log.isInfoEnabled())
                    U.log(log, "Removing BaselineTopology and history");

                metastorage.remove(METASTORE_CURR_BLT_KEY);

                bltHist.removeHistory(metastorage);
            }
        }
        finally {
            sharedCtx.database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        inMemoryMode = !CU.isPersistenceEnabled(ctx.config());

        // Start first node as inactive if persistence is enabled.
        boolean activeOnStart = inMemoryMode && ctx.config().isActiveOnStart();

        globalState = DiscoveryDataClusterState.createState(activeOnStart, false, null);

        ctx.event().addLocalEventListener(lsr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        ctx.event().addLocalEventListener(
            changeTopologyWatcher = new ChangeTopologyWatcher(ctx),
            EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED
        );
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        GridChangeGlobalStateFuture fut = this.stateChangeFut.get();

        if (fut != null)
            fut.onDone(new NodeStoppingException("Failed to wait for cluster state change, node is stopping."));

        super.onKernalStop(cancel);
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<Boolean> onLocalJoin(DiscoCache discoCache) {
        final DiscoveryDataClusterState state = globalState;

        if (state.active())
            checkLocalNodeInBaseline(state.baselineTopology());

        if (state.transition()) {
            joinFut = new TransitionOnJoinWaitFuture(state, discoCache);

            return joinFut;
        }
        else if (!ctx.clientNode()
                && !ctx.isDaemon()
                && ctx.config().isAutoActivationEnabled()
                && !state.active()
                && !inMemoryMode
                && isBaselineSatisfied(state.baselineTopology(), discoCache.serverNodes())
        )
            changeGlobalState(true, state.baselineTopology().currentBaseline(), false);

        return null;
    }

    /**
     * Checks whether local node participating in Baseline Topology and warn if not.
     */
    private void checkLocalNodeInBaseline(BaselineTopology blt) {
        if (blt == null || blt.consistentIds() == null || ctx.clientNode() || ctx.isDaemon())
            return;

        if (!blt.consistentIds().contains(ctx.discovery().localNode().consistentId())) {
            U.quietAndInfo(log, "Local node is not included in Baseline Topology and will not be used " +
                "for data storage. Use control.(sh|bat) script or IgniteCluster interface to include " +
                "the node to Baseline Topology.");
        }
    }

    /**
     * Checks whether all conditions to meet BaselineTopology are satisfied.
     */
    private boolean isBaselineSatisfied(BaselineTopology blt, List<ClusterNode> serverNodes) {
        if (blt == null)
            return false;

        if (blt.consistentIds() == null)
            return false;

        if (//only node participating in BaselineTopology is allowed to send activation command...
            blt.consistentIds().contains(ctx.discovery().localNode().consistentId())
                //...and with this node BaselineTopology is reached
                && blt.isSatisfied(serverNodes))
            return true;

        return false;
    }

    /** {@inheritDoc} */
    @Override @Nullable public ChangeGlobalStateFinishMessage onNodeLeft(ClusterNode node) {
        if (globalState.transition()) {
            Set<UUID> nodes = globalState.transitionNodes();

            if (nodes.remove(node.id()) && nodes.isEmpty()) {
                U.warn(log, "Failed to change cluster state, all participating nodes failed. " +
                    "Switching to inactive state.");

                ChangeGlobalStateFinishMessage msg =
                    new ChangeGlobalStateFinishMessage(globalState.transitionRequestId(), false, false);

                onStateFinishMessage(msg);

                return msg;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onStateFinishMessage(ChangeGlobalStateFinishMessage msg) {
        DiscoveryDataClusterState state = globalState;

        if (msg.requestId().equals(state.transitionRequestId())) {
            log.info("Received state change finish message: " + msg.clusterActive());

            globalState = state.finish(msg.success());

            afterStateChangeFinished(msg.id(), msg.success());

            ctx.cache().onStateChangeFinish(msg);

            boolean prev = ctx.cache().context().readOnlyMode();

            if (prev != globalState.readOnly()) {
                ctx.cache().context().readOnlyMode(globalState.readOnly());

                if (globalState.readOnly())
                    log.info("Read-only mode is enabled");
                else
                    log.info("Read-only mode is disabled");
            }

            TransitionOnJoinWaitFuture joinFut = this.joinFut;

            if (joinFut != null)
                joinFut.onDone(false);

            GridFutureAdapter<Void> transitionFut = transitionFuts.remove(state.transitionRequestId());

            if (transitionFut != null) {
                state.setTransitionResult(msg.requestId(), msg.clusterActive());

                transitionFut.onDone();
            }
        }
        else
            U.warn(log, "Received state finish message with unexpected ID: " + msg);
    }

    /** */
    protected void afterStateChangeFinished(IgniteUuid msgId, boolean success) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public boolean onStateChangeMessage(
        AffinityTopologyVersion topVer,
        ChangeGlobalStateMessage msg,
        DiscoCache discoCache
    ) {
        DiscoveryDataClusterState state = globalState;

        U.log(
            log,
            "Received " + prettyStr(msg.activate(), msg.readOnly(), readOnlyChanged(state, msg.readOnly())) +
                " request with BaselineTopology" +
                (msg.baselineTopology() == null ? ": null" : "[id=" + msg.baselineTopology().id() + "]") +
                " initiator node ID: " + msg.initiatorNodeId()
        );

        if (msg.baselineTopology() != null)
            compatibilityMode = false;

        if (state.transition()) {
            if (isApplicable(msg, state)) {
                GridChangeGlobalStateFuture fut = changeStateFuture(msg);

                if (fut != null)
                    fut.onDone(concurrentStateChangeError(msg.activate()));
            }
            else {
                final GridChangeGlobalStateFuture stateFut = changeStateFuture(msg);

                GridFutureAdapter<Void> transitionFut = transitionFuts.get(state.transitionRequestId());

                if (stateFut != null && transitionFut != null) {
                    transitionFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                        @Override public void apply(IgniteInternalFuture<Void> fut) {
                            try {
                                fut.get();

                                stateFut.onDone();
                            }
                            catch (Exception ex) {
                                stateFut.onDone(ex);
                            }

                        }
                    });
                }
            }
        }
        else {
            if (isApplicable(msg, state)) {
                ExchangeActions exchangeActions;

                try {
                    exchangeActions = ctx.cache().onStateChangeRequest(msg, topVer, state);
                }
                catch (IgniteCheckedException e) {
                    GridChangeGlobalStateFuture fut = changeStateFuture(msg);

                    if (fut != null)
                        fut.onDone(e);

                    return false;
                }

                Set<UUID> nodeIds = U.newHashSet(discoCache.allNodes().size());

                for (ClusterNode node : discoCache.allNodes())
                    nodeIds.add(node.id());

                GridChangeGlobalStateFuture fut = changeStateFuture(msg);

                if (fut != null)
                    fut.setRemaining(nodeIds, topVer.nextMinorVersion());

                if (log.isInfoEnabled())
                    log.info("Started state transition: " + msg.activate());

                BaselineTopologyHistoryItem bltHistItem = BaselineTopologyHistoryItem.fromBaseline(
                    globalState.baselineTopology());

                transitionFuts.put(msg.requestId(), new GridFutureAdapter<Void>());

                DiscoveryDataClusterState prevState = globalState;

                globalState = DiscoveryDataClusterState.createTransitionState(
                    prevState,
                    msg.activate(),
                    msg.readOnly(),
                    msg.activate() ? msg.baselineTopology() : prevState.baselineTopology(),
                    msg.requestId(),
                    topVer,
                    nodeIds
                );

                if (msg.forceChangeBaselineTopology())
                    globalState.setTransitionResult(msg.requestId(), msg.activate());

                AffinityTopologyVersion stateChangeTopVer = topVer.nextMinorVersion();

                StateChangeRequest req = new StateChangeRequest(msg, bltHistItem, msg.activate() != state.active(), stateChangeTopVer);

                exchangeActions.stateChangeRequest(req);

                msg.exchangeActions(exchangeActions);

                return true;
            }
            else {
                // State already changed.
                GridChangeGlobalStateFuture stateFut = changeStateFuture(msg);

                if (stateFut != null)
                    stateFut.onDone();
            }
        }

        return false;
    }

    /**
     * @param msg State change message.
     * @param state Current cluster state.
     * @return {@code True} if state change from message can be applied to the current state.
     */
    protected boolean isApplicable(ChangeGlobalStateMessage msg, DiscoveryDataClusterState state) {
        return !isEquivalent(msg, state);
    }

    /**
     * @param msg State change message.
     * @param state Current cluster state.
     * @return {@code True} if states are equivalent.
     */
    protected static boolean isEquivalent(ChangeGlobalStateMessage msg, DiscoveryDataClusterState state) {
        return msg.activate() == state.active() &&
            msg.readOnly() == state.readOnly() &&
            BaselineTopology.equals(msg.baselineTopology(), state.baselineTopology());
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataClusterState clusterState() {
        return globalState;
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataClusterState pendingState(ChangeGlobalStateMessage stateMsg) {
        return DiscoveryDataClusterState.createState(
            stateMsg.activate() || stateMsg.forceChangeBaselineTopology(),
            stateMsg.readOnly(),
            stateMsg.baselineTopology()
        );
    }

    /**
     * @param msg State change message.
     * @return Local future for state change process.
     */
    @Nullable private GridChangeGlobalStateFuture changeStateFuture(ChangeGlobalStateMessage msg) {
        return changeStateFuture(msg.initiatorNodeId(), msg.requestId());
    }

    /**
     * @param initiatorNode Node initiated state change process.
     * @param reqId State change request ID.
     * @return Local future for state change process.
     */
    @Nullable private GridChangeGlobalStateFuture changeStateFuture(UUID initiatorNode, UUID reqId) {
        assert initiatorNode != null;
        assert reqId != null;

        if (initiatorNode.equals(ctx.localNodeId())) {
            GridChangeGlobalStateFuture fut = stateChangeFut.get();

            if (fut != null && fut.requestId.equals(reqId))
                return fut;
        }

        return null;
    }

    /**
     * @param activate New state.
     * @return State change error.
     */
    protected IgniteCheckedException concurrentStateChangeError(boolean activate) {
        return new IgniteCheckedException("Failed to " + prettyStr(activate) +
            ", because another state change operation is currently in progress: " + prettyStr(!activate));
    }

    /** {@inheritDoc} */
    @Override public void cacheProcessorStarted() {
        cacheProc = ctx.cache();
        sharedCtx = cacheProc.context();

        sharedCtx.io().addCacheHandler(
            0, GridChangeGlobalStateMessageResponse.class,
            this::processChangeGlobalStateResponse
        );
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if (sharedCtx != null)
            sharedCtx.io().removeHandler(false, 0, GridChangeGlobalStateMessageResponse.class);

        ctx.event().removeLocalEventListener(lsr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        IgniteCheckedException stopErr = new IgniteCheckedException(
            "Node is stopping: " + ctx.igniteInstanceName());

        GridChangeGlobalStateFuture f = stateChangeFut.get();

        if (f != null)
            f.onDone(stopErr);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.STATE_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        try {
            byte[] marshalledState = marsh.marshal(globalState);

            dataBag.addJoiningNodeData(discoveryDataType().ordinal(), marshalledState);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        DiscoveryDataBag.JoiningNodeDiscoveryData joiningNodeData = dataBag.newJoinerDiscoveryData(STATE_PROC.ordinal());

        if (joiningNodeData != null && !joiningNodeData.hasJoiningNodeData())
            compatibilityMode = true; //compatibility mode: only old nodes don't send any data on join

        if (dataBag.commonDataCollectedFor(STATE_PROC.ordinal()) || joiningNodeData == null)
            return;

        if (!joiningNodeData.hasJoiningNodeData() || compatibilityMode) {
            //compatibility mode: old nodes don't send any data on join, so coordinator of new version
            //doesn't send BaselineTopology history, only its current globalState
            dataBag.addGridCommonData(STATE_PROC.ordinal(), globalState);

            return;
        }

        DiscoveryDataClusterState joiningNodeState = null;

        try {
            if (joiningNodeData.joiningNodeData() != null)
                joiningNodeState = marsh.unmarshal(
                    (byte[])joiningNodeData.joiningNodeData(),
                    U.resolveClassLoader(ctx.config())
                );
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to unmarshal disco data from joining node: " + joiningNodeData.joiningNodeId());

            return;
        }

        BaselineTopologyHistory historyToSend = null;

        if (!bltHist.isEmpty()) {
            if (joiningNodeState != null && joiningNodeState.baselineTopology() != null) {
                int lastId = joiningNodeState.baselineTopology().id();

                historyToSend = bltHist.tailFrom(lastId);
            }
            else
                historyToSend = bltHist;
        }

        dataBag.addGridCommonData(STATE_PROC.ordinal(), new BaselineStateAndHistoryData(globalState, historyToSend));
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        if (data.commonData() instanceof DiscoveryDataClusterState) {
            if (globalState != null && globalState.baselineTopology() != null)
                //node with BaselineTopology is not allowed to join mixed cluster
                // (where some nodes don't support BaselineTopology)
                throw new IgniteException("Node with BaselineTopology cannot join" +
                    " mixed cluster running in compatibility mode");

            globalState = (DiscoveryDataClusterState) data.commonData();

            compatibilityMode = true;

            ctx.cache().context().readOnlyMode(globalState.readOnly());

            return;
        }

        BaselineStateAndHistoryData stateDiscoData = (BaselineStateAndHistoryData)data.commonData();

        if (stateDiscoData != null) {
            DiscoveryDataClusterState state = stateDiscoData.globalState;

            if (state.transition())
                transitionFuts.put(state.transitionRequestId(), new GridFutureAdapter<Void>());

            globalState = state;

            if (stateDiscoData.recentHistory != null) {
                for (BaselineTopologyHistoryItem item : stateDiscoData.recentHistory.history())
                    bltHist.bufferHistoryItemForStore(item);
            }

            ctx.cache().context().readOnlyMode(globalState.readOnly());
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> changeGlobalState(
        final boolean activate,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology
    ) {
        return changeGlobalState(activate, baselineNodes, forceChangeBaselineTopology, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> changeGlobalState(
        boolean activate,
        boolean readOnly,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology
    ) {
        return changeGlobalState(activate, readOnly, baselineNodes, forceChangeBaselineTopology, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> changeGlobalState(boolean readOnly) {
        if (!publicApiActiveState(false))
            return new GridFinishedFuture<>(new IgniteException("Cluster not active"));

        DiscoveryDataClusterState state = globalState;

        List<BaselineNode> bltNodes = state.hasBaselineTopology() ? state.baselineTopology().currentBaseline() : null;

        return changeGlobalState(state.active(), readOnly, bltNodes, false, false);
    }

    /**
     * @param activate New activate state.
     * @param baselineNodes New BLT nodes.
     * @param forceChangeBaselineTopology Force change BLT.
     * @param isAutoAdjust Auto adjusting flag.
     * @return Global change state future.
     */
    public IgniteInternalFuture<?> changeGlobalState(
        final boolean activate,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology,
        boolean isAutoAdjust
    ) {
        boolean readOnly = activate && globalState.readOnly();

        return changeGlobalState(activate, readOnly, baselineNodes, forceChangeBaselineTopology, isAutoAdjust);
    }

    /**
     * @param activate New activate state.
     * @param readOnly Read-only mode.
     * @param baselineNodes New BLT nodes.
     * @param forceChangeBaselineTopology Force change BLT.
     * @param isAutoAdjust Auto adjusting flag.
     * @return Global change state future.
     */
    public IgniteInternalFuture<?> changeGlobalState(
        final boolean activate,
        boolean readOnly,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology,
        boolean isAutoAdjust
    ) {
        BaselineTopology newBlt = (compatibilityMode && !forceChangeBaselineTopology) ? null :
            calculateNewBaselineTopology(activate, baselineNodes, forceChangeBaselineTopology);

        return changeGlobalState0(activate, readOnly, newBlt, forceChangeBaselineTopology, isAutoAdjust);
    }

    /**
     *
     */
    private BaselineTopology calculateNewBaselineTopology(
        final boolean activate,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology
    ) {
        BaselineTopology newBlt;

        BaselineTopology currentBlt = globalState.baselineTopology();

        int newBltId = 0;

        if (currentBlt != null)
            newBltId = activate ? currentBlt.id() + 1 : currentBlt.id();

        if (baselineNodes != null && !baselineNodes.isEmpty()) {
            List<BaselineNode> baselineNodes0 = new ArrayList<>();

            for (BaselineNode node : baselineNodes) {
                if (node instanceof ClusterNode) {
                    ClusterNode clusterNode = (ClusterNode) node;

                    if (!clusterNode.isClient() && !clusterNode.isDaemon())
                        baselineNodes0.add(node);
                }
                else
                    baselineNodes0.add(node);
            }

            baselineNodes = baselineNodes0;
        }

        if (forceChangeBaselineTopology)
            newBlt = BaselineTopology.build(baselineNodes, newBltId);
        else if (activate) {
            if (baselineNodes == null)
                baselineNodes = baselineNodes();

            if (currentBlt == null)
                newBlt = BaselineTopology.build(baselineNodes, newBltId);
            else {
                newBlt = currentBlt;

                newBlt.updateHistory(baselineNodes);
            }
        }
        else
            newBlt = null;

        return newBlt;
    }

    /** */
    private Collection<BaselineNode> baselineNodes() {
        List<ClusterNode> clNodes = ctx.discovery().serverNodes(AffinityTopologyVersion.NONE);

        ArrayList<BaselineNode> bltNodes = new ArrayList<>(clNodes.size());

        for (ClusterNode clNode : clNodes)
            bltNodes.add(clNode);

        return bltNodes;
    }

    /** */
    private IgniteInternalFuture<?> changeGlobalState0(
        final boolean activate,
        boolean readOnly,
        BaselineTopology blt,
        boolean forceChangeBaselineTopology,
        boolean isAutoAdjust
    ) {
        boolean isBaselineAutoAdjustEnabled = isBaselineAutoAdjustEnabled();

        if (forceChangeBaselineTopology && isBaselineAutoAdjustEnabled != isAutoAdjust)
            throw new BaselineAdjustForbiddenException(isBaselineAutoAdjustEnabled);

        if (ctx.isDaemon() || ctx.clientNode()) {
            GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

            sendComputeChangeGlobalState(activate, readOnly, blt, forceChangeBaselineTopology, fut);

            return fut;
        }

        if (cacheProc.transactions().tx() != null || sharedCtx.lockedTopologyVersion(null) != null) {
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to " +
                    prettyStr(activate, readOnly, readOnlyChanged(globalState, readOnly)) +
                    " (must invoke the method outside of an active transaction).")
            );
        }

        DiscoveryDataClusterState curState = globalState;

        if (!curState.transition() &&
            curState.active() == activate &&
            curState.readOnly() == readOnly
            && (!activate || BaselineTopology.equals(curState.baselineTopology(), blt)))
            return new GridFinishedFuture<>();

        GridChangeGlobalStateFuture startedFut = null;

        GridChangeGlobalStateFuture fut = stateChangeFut.get();

        while (fut == null || fut.isDone()) {
            fut = new GridChangeGlobalStateFuture(UUID.randomUUID(), activate, readOnly, ctx);

            if (stateChangeFut.compareAndSet(null, fut)) {
                startedFut = fut;

                break;
            }
            else
                fut = stateChangeFut.get();
        }

        if (startedFut == null) {
            if (fut.activate != activate && fut.readOnly != readOnly) {
                return new GridFinishedFuture<>(
                    new IgniteCheckedException(
                        "Failed to " + prettyStr(activate, readOnly, readOnlyChanged(globalState, readOnly)) +
                        ", because another state change operation is currently in progress: " +
                            prettyStr(fut.activate, fut.readOnly, readOnlyChanged(globalState, fut.readOnly))
                    )
                );
            }
            else
                return fut;
        }

        List<StoredCacheData> storedCfgs = null;

        if (activate && !inMemoryMode) {
            try {
                Map<String, StoredCacheData> cfgs = ctx.cache().context().pageStore().readCacheConfigurations();

                if (!F.isEmpty(cfgs)) {
                    storedCfgs = new ArrayList<>(cfgs.values());

                    IgniteDiscoverySpi spi = (IgniteDiscoverySpi) ctx.discovery().getInjectedDiscoverySpi();

                    boolean splittedCacheCfgs = spi.allNodesSupport(IgniteFeatures.SPLITTED_CACHE_CONFIGURATIONS);

                    storedCfgs = storedCfgs.stream()
                        .map(storedCacheData -> splittedCacheCfgs
                            ? storedCacheData.withSplittedCacheConfig(ctx.cache().splitter())
                            : storedCacheData.withOldCacheConfig(ctx.cache().enricher())
                        )
                        .collect(Collectors.toList());
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to read stored cache configurations: " + e, e);

                startedFut.onDone(e);

                return startedFut;
            }
        }

        ChangeGlobalStateMessage msg = new ChangeGlobalStateMessage(startedFut.requestId,
            ctx.localNodeId(),
            storedCfgs,
            activate,
            readOnly,
            blt,
            forceChangeBaselineTopology,
            System.currentTimeMillis()
        );

        IgniteInternalFuture<?> resFut = wrapStateChangeFuture(startedFut, msg);

        try {
            U.log(
                log,
                "Sending " + prettyStr(activate, readOnly, readOnlyChanged(globalState, readOnly)) +
                    " request with BaselineTopology " + blt
            );

            ctx.discovery().sendCustomEvent(msg);

            if (ctx.isStopping()) {
                startedFut.onDone(
                    new IgniteCheckedException(
                        "Failed to execute " +
                            prettyStr(activate, readOnly, readOnlyChanged(globalState, readOnly)) +
                            " request , node is stopping."
                    )
                );
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send global state change request: " + activate, e);

            startedFut.onDone(e);
        }

        return resFut;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(
        ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData
    ) {
        if (node.isClient() || node.isDaemon())
            return null;

        if (globalState.readOnly() && !IgniteFeatures.nodeSupports(node, IgniteFeatures.CLUSTER_READ_ONLY_MODE)) {
            String msg = "Node not supporting cluster read-only mode is not allowed to join the cluster with enabled" +
                " read-only mode";

            return new IgniteNodeValidationResult(node.id(), msg, msg);
        }

        if (discoData.joiningNodeData() == null) {
            if (globalState.baselineTopology() != null) {
                String msg = "Node not supporting BaselineTopology" +
                    " is not allowed to join the cluster with BaselineTopology";

                return new IgniteNodeValidationResult(node.id(), msg);
            }

            return null;
        }

        DiscoveryDataClusterState joiningNodeState;

        try {
            joiningNodeState = marsh.unmarshal((byte[]) discoData.joiningNodeData(), Thread.currentThread().getContextClassLoader());
        } catch (IgniteCheckedException e) {
            String msg = "Error on unmarshalling discovery data " +
                "from node " + node.consistentId() + ": " + e.getMessage() +
                "; node is not allowed to join";

            return new IgniteNodeValidationResult(node.id(), msg);
        }

        if (joiningNodeState == null || joiningNodeState.baselineTopology() == null)
            return null;

        if (globalState == null || globalState.baselineTopology() == null) {
            if (joiningNodeState != null && joiningNodeState.baselineTopology() != null) {
                String msg = "Node with set up BaselineTopology is not allowed to join cluster without one: " + node.consistentId();

                return new IgniteNodeValidationResult(node.id(), msg);
            }
        }

        if (globalState.transition() && globalState.previousBaselineTopology() == null) {
            //case when cluster is activating for the first time and other node with existing baseline topology
            //tries to join

            String msg = "Node with set up BaselineTopology is not allowed " +
                "to join cluster in the process of first activation: " + node.consistentId();

            return new IgniteNodeValidationResult(node.id(), msg);
        }

        BaselineTopology clusterBlt;

        if (globalState.transition())
            clusterBlt = globalState.previousBaselineTopology();
        else
            clusterBlt = globalState.baselineTopology();

        BaselineTopology joiningNodeBlt = joiningNodeState.baselineTopology();

        String recommendation = " Consider cleaning persistent storage of the node and adding it to the cluster again.";

        if (joiningNodeBlt.id() > clusterBlt.id()) {
            String msg = "BaselineTopology of joining node ("
                + node.consistentId()
                + ") is not compatible with BaselineTopology in the cluster."
                + " Joining node BlT id (" + joiningNodeBlt.id()
                + ") is greater than cluster BlT id (" + clusterBlt.id() + ")."
                + " New BaselineTopology was set on joining node with set-baseline command."
                + recommendation;

            return new IgniteNodeValidationResult(node.id(), msg);
        }

        if (joiningNodeBlt.id() == clusterBlt.id()) {
            if (!clusterBlt.isCompatibleWith(joiningNodeBlt)) {
                String msg = "BaselineTopology of joining node ("
                    + node.consistentId()
                    + ") is not compatible with BaselineTopology in the cluster."
                    + " Branching history of cluster BlT (" + clusterBlt.branchingHistory()
                    + ") doesn't contain branching point hash of joining node BlT ("
                    + joiningNodeBlt.branchingPointHash()
                    + ")." + recommendation;

                return new IgniteNodeValidationResult(node.id(), msg);
            }
        }
        else if (joiningNodeBlt.id() < clusterBlt.id()) {
            if (!bltHist.isCompatibleWith(joiningNodeBlt)) {
                String msg = "BaselineTopology of joining node ("
                    + node.consistentId()
                    + ") is not compatible with BaselineTopology in the cluster."
                    + " BlT id of joining node (" + joiningNodeBlt.id()
                    + ") less than BlT id of cluster (" + clusterBlt.id()
                    + ") but cluster's BaselineHistory doesn't contain branching point hash of joining node BlT ("
                    + joiningNodeBlt.branchingPointHash()
                    + ")." + recommendation;

                return new IgniteNodeValidationResult(node.id(), msg);
            }
        }

        return null;
    }

    /**
     * @param fut Original state change future.
     * @param msg State change message.
     * @return Wrapped state change future.
     */
    protected IgniteInternalFuture<?> wrapStateChangeFuture(IgniteInternalFuture fut, ChangeGlobalStateMessage msg) {
        return fut;
    }

    /**
     * @param activate New cluster state.
     * @param readOnly New read-only mode.
     * @param resFut State change future.
     */
    private void sendComputeChangeGlobalState(
        boolean activate,
        boolean readOnly,
        BaselineTopology blt,
        boolean forceBlt,
        final GridFutureAdapter<Void> resFut
    ) {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        U.log(
            log,
            "Sending " + prettyStr(activate, readOnly, readOnlyChanged(globalState, readOnly)) +
                " request from node [id=" + ctx.localNodeId() +
                ", topVer=" + topVer +
                ", client=" + ctx.clientNode() +
                ", daemon=" + ctx.isDaemon() + "]"
        );

        IgniteCompute comp = ((ClusterGroupAdapter)ctx.cluster().get().forServers()).compute();

        IgniteFuture<Void> fut =
            comp.runAsync(new ClientChangeGlobalStateComputeRequest(activate, readOnly, blt, forceBlt));

        fut.listen(new CI1<IgniteFuture>() {
            @Override public void apply(IgniteFuture fut) {
                try {
                    fut.get();

                    resFut.onDone();
                }
                catch (Exception e) {
                    resFut.onDone(e);
                }
            }
        });
    }

    /**
     *  Check cluster state.
     *
     *  @return Cluster state, {@code True} if cluster active, {@code False} if inactive.
     */
    private IgniteFuture<Boolean> sendComputeCheckGlobalState() {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        if (log.isInfoEnabled()) {
            log.info("Sending check cluster state request from node [id=" + ctx.localNodeId() +
                ", topVer=" + topVer +
                ", client=" + ctx.clientNode() +
                ", daemon" + ctx.isDaemon() + "]");
        }

        ClusterGroupAdapter clusterGroupAdapter = (ClusterGroupAdapter)ctx.cluster().get().forServers();

        if (F.isEmpty(clusterGroupAdapter.nodes()))
            return new IgniteFinishedFutureImpl<>(false);

        IgniteCompute comp = clusterGroupAdapter.compute();

        return comp.callAsync(new CheckGlobalStateComputeRequest());
    }

    /** {@inheritDoc} */
    @Override public void onStateChangeError(Map<UUID, Exception> errs, StateChangeRequest req) {
        assert !F.isEmpty(errs);

        // Revert caches start if activation request fail.
        if (req.activeChanged()) {
            if (req.activate()) {
                try {
                    cacheProc.onKernalStopCaches(true);

                    cacheProc.stopCaches(true);

                    sharedCtx.affinity().clearGroupHoldersAndRegistry();

                    if (!ctx.clientNode())
                        sharedCtx.deactivate();
                }
                catch (Exception e) {
                    U.error(log, "Failed to revert activation request changes", e);
                }
            }
            else {
                //todo https://issues.apache.org/jira/browse/IGNITE-5480
            }
        }

        GridChangeGlobalStateFuture fut = changeStateFuture(req.initiatorNodeId(), req.requestId());

        if (fut != null) {
            IgniteCheckedException e = new IgniteCheckedException(
                "Failed to " + prettyStr(req.activate(), req.readOnly(), readOnlyChanged(globalState, req.readOnly())),
                null,
                false
            );

            for (Map.Entry<UUID, Exception> entry : errs.entrySet())
                e.addSuppressed(entry.getValue());

            fut.onDone(e);
        }
    }

    /**
     * @param req State change request.
     */
    private void onFinalActivate(final StateChangeRequest req) {
        ctx.dataStructures().onBeforeActivate();

        checkLocalNodeInBaseline(globalState.baselineTopology());

        ctx.closure().runLocalSafe(new Runnable() {
            @Override public void run() {
                boolean client = ctx.clientNode();

                try {
                    if (ctx.service() instanceof GridServiceProcessor) {
                        GridServiceProcessor srvcProc = (GridServiceProcessor)ctx.service();

                        srvcProc.onUtilityCacheStarted();

                        srvcProc.onActivate(ctx);
                    }

                    ctx.dataStructures().onActivate(ctx);

                    ctx.igfs().onActivate(ctx);

                    ctx.task().onActivate(ctx);

                    ctx.encryption().onActivate(ctx);

                    distributedBaselineConfiguration.onActivate();

                    if (log.isInfoEnabled())
                        log.info("Successfully performed final activation steps [nodeId="
                            + ctx.localNodeId() + ", client=" + client + ", topVer=" + req.topologyVersion() + "]");
                }
                catch (Exception ex) {
                    throw new IgniteException(ex);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onStateChangeExchangeDone(StateChangeRequest req) {
        try {
            if (req.activeChanged()) {
                if (req.activate())
                    onFinalActivate(req);

                globalState.setTransitionResult(req.requestId(), req.activate());
            }

            sendChangeGlobalStateResponse(req.requestId(), req.initiatorNodeId(), null);
        }
        catch (Exception ex) {
            Exception e = new IgniteCheckedException("Failed to perform final activation steps", ex);

            U.error(log, "Failed to perform final activation steps [nodeId=" + ctx.localNodeId() +
                ", client=" + ctx.clientNode() + ", topVer=" + req.topologyVersion() + "]", ex);

            sendChangeGlobalStateResponse(req.requestId(), req.initiatorNodeId(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onBaselineTopologyChanged(
        BaselineTopology blt,
        BaselineTopologyHistoryItem prevBltHistItem
    ) throws IgniteCheckedException {
        if (compatibilityMode) {
            if (log.isInfoEnabled())
                log.info("BaselineTopology won't be stored as this node is running in compatibility mode");

            return;
        }

        writeBaselineTopology(blt, prevBltHistItem);
    }

    /**
     * @param reqId Request ID.
     * @param initNodeId Initialize node id.
     * @param ex Exception.
     */
    private void sendChangeGlobalStateResponse(UUID reqId, UUID initNodeId, Exception ex) {
        assert reqId != null;
        assert initNodeId != null;

        GridChangeGlobalStateMessageResponse res = new GridChangeGlobalStateMessageResponse(reqId, ex);

        try {
            if (log.isDebugEnabled())
                log.debug("Sending global state change response [nodeId=" + ctx.localNodeId() +
                    ", topVer=" + ctx.discovery().topologyVersionEx() + ", res=" + res + "]");

            if (ctx.localNodeId().equals(initNodeId))
                processChangeGlobalStateResponse(ctx.localNodeId(), res);
            else
                sharedCtx.io().send(initNodeId, res, SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to send change global state response, node left [node=" + initNodeId +
                    ", res=" + res + ']');
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send change global state response [node=" + initNodeId + ", res=" + res + ']', e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processChangeGlobalStateResponse(final UUID nodeId, final GridChangeGlobalStateMessageResponse msg) {
        assert nodeId != null;
        assert msg != null;

        if (log.isDebugEnabled()) {
            log.debug("Received activation response [requestId=" + msg.getRequestId() +
                ", nodeId=" + nodeId + "]");
        }

        UUID requestId = msg.getRequestId();

        final GridChangeGlobalStateFuture fut = stateChangeFut.get();

        if (fut != null && requestId.equals(fut.requestId)) {
            if (fut.initFut.isDone())
                fut.onResponse(nodeId, msg);
            else {
                fut.initFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        // initFut is completed from discovery thread, process response from other thread.
                        ctx.getSystemExecutorService().execute(new Runnable() {
                            @Override public void run() {
                                fut.onResponse(nodeId, msg);
                            }
                        });
                    }
                });
            }
        }
    }

    /** */
    private void onStateRestored(BaselineTopology blt) {
        DiscoveryDataClusterState state = globalState;

        if (!state.active() && !state.transition() && state.baselineTopology() == null) {
            DiscoveryDataClusterState newState = DiscoveryDataClusterState.createState(false, false, blt);

            globalState = newState;
        }
    }

    /**
     * Update baseline locally if cluster is not persistent and baseline autoadjustment is enabled with zero timeout.
     *
     * @param nodeId Id of the node that initiated the operation (joined/left/failed).
     * @param topSnapshot Topology snapshot from the discovery message.
     * @param discoCache Discovery cache from the discovery manager.
     * @param topVer Topology version.
     * @param minorTopVer Minor topology version.
     * @return {@code true} if baseline was changed and discovery cache recalculation is required.
     */
    public boolean autoAdjustInMemoryClusterState(
        UUID nodeId,
        Collection<ClusterNode> topSnapshot,
        DiscoCache discoCache,
        long topVer,
        int minorTopVer
    ) {
        IgniteClusterImpl cluster = ctx.cluster().get();

        DiscoveryDataClusterState oldState = globalState;

        boolean isInMemoryCluster = CU.isInMemoryCluster(
            ctx.discovery().allNodes(),
            ctx.marshallerContext().jdkMarshaller(),
            U.resolveClassLoader(ctx.config())
        );

        boolean autoAdjustBaseline = isInMemoryCluster
            && oldState.active()
            && !oldState.transition()
            && cluster.isBaselineAutoAdjustEnabled()
            && cluster.baselineAutoAdjustTimeout() == 0L;

        if (autoAdjustBaseline) {
            BaselineTopology oldBlt = oldState.baselineTopology();

            Collection<ClusterNode> bltNodes = topSnapshot.stream()
                .filter(n -> !n.isClient() && !n.isDaemon())
                .collect(Collectors.toList());

            if (!bltNodes.isEmpty()) {
                int newBltId = oldBlt == null ? 0 : oldBlt.id();

                BaselineTopology newBlt = BaselineTopology.build(bltNodes, newBltId);

                ChangeGlobalStateMessage changeGlobalStateMsg = new ChangeGlobalStateMessage(
                    nodeId,
                    nodeId,
                    null,
                    true,
                    oldState.readOnly(),
                    newBlt,
                    true,
                    System.currentTimeMillis()
                );

                AffinityTopologyVersion ver = new AffinityTopologyVersion(topVer, minorTopVer);

                onStateChangeMessage(ver, changeGlobalStateMsg, discoCache);

                ChangeGlobalStateFinishMessage finishMsg = new ChangeGlobalStateFinishMessage(nodeId, true, true);

                onStateFinishMessage(finishMsg);

                globalState.localBaselineAutoAdjustment(true);

                return true;
            }
        }

        return false;
    }

    /**
     * Add fake state change request into exchange actions if cluster is not persistent and baseline autoadjustment
     * is enabled with zero timeout.
     *
     * @param exchActs Current exchange actions.
     * @return New exchange actions.
     */
    public ExchangeActions autoAdjustExchangeActions(ExchangeActions exchActs) {
        DiscoveryDataClusterState clusterState = globalState;

        if (clusterState.localBaselineAutoAdjustment()) {
            BaselineTopology blt = clusterState.baselineTopology();

            ChangeGlobalStateMessage msg = new ChangeGlobalStateMessage(
                UUID.randomUUID(),
                ctx.localNodeId(),
                null,
                true,
                clusterState.readOnly(),
                blt,
                true,
                System.currentTimeMillis()
            );

            StateChangeRequest stateChangeReq = new StateChangeRequest(
                msg,
                BaselineTopologyHistoryItem.fromBaseline(blt),
                false,
                null
            );

            if (exchActs == null)
                exchActs = new ExchangeActions();

            exchActs.stateChangeRequest(stateChangeReq);
        }

        return exchActs;
    }

    /** {@inheritDoc} */
    @Override public void onExchangeFinishedOnCoordinator(IgniteInternalFuture exchangeFuture, boolean hasMovingPartitions) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public boolean evictionsAllowed() {
        return true;
    }

    /**
     * @return Value of manual baseline control or auto adjusting baseline. {@code True} If cluster in auto-adjust.
     * {@code False} If cluster in manuale.
     */
    public boolean isBaselineAutoAdjustEnabled() {
        return distributedBaselineConfiguration.isBaselineAutoAdjustEnabled();
    }

    /**
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline. {@code True} If
     * cluster in auto-adjust. {@code False} If cluster in manuale.
     * @throws IgniteException If operation failed.
     */
    public void baselineAutoAdjustEnabled(boolean baselineAutoAdjustEnabled) {
        baselineAutoAdjustEnabledAsync(baselineAutoAdjustEnabled).get();
    }

    /**
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline. {@code True} If
     * cluster in auto-adjust. {@code False} If cluster in manuale.
     * @return Future for await operation completion.
     */
    public IgniteFuture<?> baselineAutoAdjustEnabledAsync(boolean baselineAutoAdjustEnabled) {
        try {
            return new IgniteFutureImpl<>(
                distributedBaselineConfiguration.updateBaselineAutoAdjustEnabledAsync(baselineAutoAdjustEnabled));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last server topology change
     * (node join/left/fail).
     * @throws IgniteException If operation failed.
     */
    public long baselineAutoAdjustTimeout() {
        return distributedBaselineConfiguration.getBaselineAutoAdjustTimeout();
    }

    /**
     * @param baselineAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * server topology change (node join/left/fail).
     * @throws IgniteException If failed.
     */
    public void baselineAutoAdjustTimeout(long baselineAutoAdjustTimeout) {
        baselineAutoAdjustTimeoutAsync(baselineAutoAdjustTimeout).get();
    }

    /**
     * @param baselineAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * server topology change (node join/left/fail).
     * @return Future for await operation completion.
     */
    public IgniteFuture<?> baselineAutoAdjustTimeoutAsync(long baselineAutoAdjustTimeout) {
        A.ensure(baselineAutoAdjustTimeout >= 0, "timeout should be positive or zero");

        try {
            return new IgniteFutureImpl<>(
                distributedBaselineConfiguration.updateBaselineAutoAdjustTimeoutAsync(baselineAutoAdjustTimeout));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @return Baseline configuration.
     */
    public DistributedBaselineConfiguration baselineConfiguration() {
        return distributedBaselineConfiguration;
    }

    /**
     * @return Status of baseline auto-adjust.
     */
    public BaselineAutoAdjustStatus baselineAutoAdjustStatus(){
        return changeTopologyWatcher.getStatus();
    }

    /**
     * @param activate Activate.
     * @return Activate flag string.
     */
    private static String prettyStr(boolean activate) {
        return activate ? "activate" : "deactivate";
    }

    /**
     * @param activate Activate flag.
     * @param readOnly Read-only flag.
     * @param readOnlyChanged Read only state changed.
     * @return Activate or read-only message string.
     */
    private static String prettyStr(boolean activate, boolean readOnly, boolean readOnlyChanged) {
        return readOnlyChanged ? prettyStr(readOnly) + " read-only mode" : prettyStr(activate) + " cluster";
    }

    /**
     * @param curState Current cluster state.
     * @param newReadOnly New read-only mode value.
     * @return {@code True} if read-only mode changed and {@code False} otherwise.
     */
    private boolean readOnlyChanged(DiscoveryDataClusterState curState, boolean newReadOnly) {
        return curState.readOnly() != newReadOnly;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClusterStateProcessor.class, this);
    }

    /**
     *
     */
    private class GridChangeGlobalStateFuture extends GridFutureAdapter<Void> {
        /** Request id. */
        @GridToStringInclude
        private final UUID requestId;

        /** Activate. */
        private final boolean activate;

        /** Read only. */
        private final boolean readOnly;

        /** Nodes. */
        @GridToStringInclude
        private final Set<UUID> remaining = new HashSet<>();

        /** Responses. */
        @GridToStringInclude
        private final Map<UUID, GridChangeGlobalStateMessageResponse> responses = new HashMap<>();

        /** Context. */
        @GridToStringExclude
        private final GridKernalContext ctx;

        /** */
        @GridToStringExclude
        private final Object mux = new Object();

        /** */
        @GridToStringInclude
        private final GridFutureAdapter<?> initFut = new GridFutureAdapter<>();

        /** Grid logger. */
        @GridToStringExclude
        private final IgniteLogger log;

        /**
         * @param requestId State change request ID.
         * @param activate New cluster state.
         * @param ctx Context.
         */
        GridChangeGlobalStateFuture(UUID requestId, boolean activate, boolean readOnly, GridKernalContext ctx) {
            this.requestId = requestId;
            this.activate = activate;
            this.readOnly = readOnly;
            this.ctx = ctx;

            log = ctx.log(getClass());
        }

        /**
         * @param event Event.
         */
        void onNodeLeft(DiscoveryEvent event) {
            assert event != null;

            if (isDone())
                return;

            boolean allReceived = false;

            synchronized (mux) {
                if (remaining.remove(event.eventNode().id()))
                    allReceived = remaining.isEmpty();
            }

            if (allReceived)
                onAllReceived();
        }

        /**
         * @param nodesIds Node IDs.
         * @param topVer Current topology version.
         */
        void setRemaining(Set<UUID> nodesIds, AffinityTopologyVersion topVer) {
            if (log.isDebugEnabled()) {
                log.debug("Setup remaining node [id=" + ctx.localNodeId() +
                    ", client=" + ctx.clientNode() +
                    ", topVer=" + topVer +
                    ", nodes=" + nodesIds + "]");
            }

            synchronized (mux) {
                remaining.addAll(nodesIds);
            }

            initFut.onDone();
        }

        /**
         * @param nodeId Sender node ID.
         * @param msg Activation message response.
         */
        public void onResponse(UUID nodeId, GridChangeGlobalStateMessageResponse msg) {
            assert msg != null;

            if (isDone())
                return;

            boolean allReceived = false;

            synchronized (mux) {
                if (remaining.remove(nodeId))
                    allReceived = remaining.isEmpty();

                responses.put(nodeId, msg);
            }

            if (allReceived)
                onAllReceived();
        }

        /**
         *
         */
        private void onAllReceived() {
            IgniteCheckedException e = new IgniteCheckedException();

            boolean fail = false;

            for (Map.Entry<UUID, GridChangeGlobalStateMessageResponse> entry : responses.entrySet()) {
                GridChangeGlobalStateMessageResponse r = entry.getValue();

                if (r.getError() != null) {
                    fail = true;

                    e.addSuppressed(r.getError());
                }
            }

            if (fail)
                onDone(e);
            else
                onDone();
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                stateChangeFut.compareAndSet(this, null);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridChangeGlobalStateFuture.class, this);
        }
    }

    /**
     *
     */
    @GridInternal
    private static class ClientChangeGlobalStateComputeRequest implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final boolean activate;

        /** */
        private final boolean readOnly;

        /** */
        private final BaselineTopology baselineTopology;

        /** */
        private final boolean forceChangeBaselineTopology;

        /** Ignite. */
        @IgniteInstanceResource
        private IgniteEx ig;

        /**
         * @param activate New cluster state.
         */
        private ClientChangeGlobalStateComputeRequest(
            boolean activate,
            boolean readOnly,
            BaselineTopology blt,
            boolean forceBlt
        ) {
            this.activate = activate;
            this.readOnly = readOnly;
            this.baselineTopology = blt;
            this.forceChangeBaselineTopology = forceBlt;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                ig.context().state().changeGlobalState(
                    activate,
                    readOnly,
                    baselineTopology != null ? baselineTopology.currentBaseline() : null,
                    forceChangeBaselineTopology
                ).get();
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
        }
    }

    /**
     *
     */
    @GridInternal
    private static class CheckGlobalStateComputeRequest implements IgniteCallable<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ig;

        @Override public Boolean call() throws Exception {
            return ig.active();
        }
    }

    /**
     *
     */
    class TransitionOnJoinWaitFuture extends GridFutureAdapter<Boolean> {
        /** */
        private DiscoveryDataClusterState transitionState;

        /** */
        private final Set<UUID> transitionNodes;

        /**
         * @param state Current state.
         * @param discoCache Discovery data cache.
         */
        TransitionOnJoinWaitFuture(DiscoveryDataClusterState state, DiscoCache discoCache) {
            assert state.transition() : state;

            transitionNodes = U.newHashSet(state.transitionNodes().size());

            for (UUID nodeId : state.transitionNodes()) {
                if (discoCache.node(nodeId) != null)
                    transitionNodes.add(nodeId);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                joinFut = null;

                return true;
            }

            return false;
        }
    }

    /** */
    private static class BaselineStateAndHistoryData implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final DiscoveryDataClusterState globalState;

        /** */
        private final BaselineTopologyHistory recentHistory;

        /** */
        BaselineStateAndHistoryData(DiscoveryDataClusterState globalState, BaselineTopologyHistory recentHistory) {
            this.globalState = globalState;
            this.recentHistory = recentHistory;
        }
    }
}
