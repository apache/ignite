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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
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
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
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
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.STATE_PROC;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

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
    }

    /**
     * @return {@code True} if {@link IGridClusterStateProcessor} has detected that cluster is working
     * in compatibility mode (nodes of different versions are joined to the cluster).
     */
    public boolean compatibilityMode() {
        return compatibilityMode;
    }

    /** {@inheritDoc} */
    @Override public boolean publicApiActiveState(boolean waitForTransition) {
        if (ctx.isDaemon())
            return sendComputeCheckGlobalState();

        DiscoveryDataClusterState globalState = this.globalState;

        assert globalState != null;

        if (globalState.transition() && globalState.active()) {
            Boolean transitionRes = globalState.transitionResult();

            if (transitionRes != null)
                return transitionRes;
            else {
                if (waitForTransition) {
                    GridFutureAdapter<Void> fut = transitionFuts.get(globalState.transitionRequestId());

                    if (fut != null) {
                        try {
                            fut.get();
                        }
                        catch (IgniteCheckedException ex) {
                            throw new IgniteException(ex);
                        }
                    }

                    transitionRes = globalState.transitionResult();

                    assert transitionRes != null;

                    return transitionRes;
                }
                else
                    return false;
            }
        }
        else
            return globalState.active();
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

        globalState = DiscoveryDataClusterState.createState(activeOnStart, null);

        ctx.event().addLocalEventListener(lsr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        GridChangeGlobalStateFuture fut = this.stateChangeFut.get();

        if (fut != null)
            fut.onDone(new IgniteCheckedException("Failed to wait for cluster state change, node is stopping."));

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
                && isBaselineSatisfied(state.baselineTopology(), discoCache.serverNodes()))
                changeGlobalState0(true, state.baselineTopology(), false);

        return null;
    }

    /**
     * Checks whether local node participating in Baseline Topology and warn if not.
     */
    private void checkLocalNodeInBaseline(BaselineTopology blt) {
        if (blt == null || blt.consistentIds() == null || ctx.clientNode() || ctx.isDaemon())
            return;

        if (!CU.isPersistenceEnabled(ctx.config()))
            return;

        if (!blt.consistentIds().contains(ctx.discovery().localNode().consistentId())) {
            U.quietAndInfo(log, "Local node is not included in Baseline Topology and will not be used " +
                "for persistent data storage. Use control.(sh|bat) script or IgniteCluster interface to include " +
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

            globalState = globalState.finish(msg.success());

            afterStateChangeFinished(msg.id(), msg.success());

            ctx.cache().onStateChangeFinish(msg);

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

        if (log.isInfoEnabled())
            U.log(log, "Received " + prettyStr(msg.activate()) + " request with BaselineTopology" +
                (msg.baselineTopology() == null ? ": null"
                    : "[id=" + msg.baselineTopology().id() + "]"));

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
        return (msg.activate() == state.active() && BaselineTopology.equals(msg.baselineTopology(), state.baselineTopology()));
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataClusterState clusterState() {
        return globalState;
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataClusterState pendingState(ChangeGlobalStateMessage stateMsg) {
        return DiscoveryDataClusterState.createState(stateMsg.activate() || stateMsg.forceChangeBaselineTopology(),
            stateMsg.baselineTopology());
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
            new CI2<UUID, GridChangeGlobalStateMessageResponse>() {
                @Override public void apply(UUID nodeId, GridChangeGlobalStateMessageResponse msg) {
                    processChangeGlobalStateResponse(nodeId, msg);
                }
            });
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
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> changeGlobalState(
        final boolean activate,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology
    ) {
        if (inMemoryMode)
            return changeGlobalState0(activate, null, false);

        BaselineTopology newBlt = (compatibilityMode && !forceChangeBaselineTopology) ? null :
            calculateNewBaselineTopology(activate, baselineNodes, forceChangeBaselineTopology);

        return changeGlobalState0(activate, newBlt, forceChangeBaselineTopology);
    }

    /**
     *
     */
    private BaselineTopology calculateNewBaselineTopology(final boolean activate,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology) {
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
    private IgniteInternalFuture<?> changeGlobalState0(final boolean activate,
        BaselineTopology blt, boolean forceChangeBaselineTopology) {
        if (ctx.isDaemon() || ctx.clientNode()) {
            GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

            sendComputeChangeGlobalState(activate, blt, forceChangeBaselineTopology, fut);

            return fut;
        }

        if (cacheProc.transactions().tx() != null || sharedCtx.lockedTopologyVersion(null) != null) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Failed to " + prettyStr(activate) +
                " cluster (must invoke the method outside of an active transaction)."));
        }

        DiscoveryDataClusterState curState = globalState;

        if (!curState.transition() && curState.active() == activate && BaselineTopology.equals(curState.baselineTopology(), blt))
            return new GridFinishedFuture<>();

        GridChangeGlobalStateFuture startedFut = null;

        GridChangeGlobalStateFuture fut = stateChangeFut.get();

        while (fut == null || fut.isDone()) {
            fut = new GridChangeGlobalStateFuture(UUID.randomUUID(), activate, ctx);

            if (stateChangeFut.compareAndSet(null, fut)) {
                startedFut = fut;

                break;
            }
            else
                fut = stateChangeFut.get();
        }

        if (startedFut == null) {
            if (fut.activate != activate) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to " + prettyStr(activate) +
                    ", because another state change operation is currently in progress: " + prettyStr(fut.activate)));
            }
            else
                return fut;
        }

        List<StoredCacheData> storedCfgs = null;

        if (activate && CU.isPersistenceEnabled(ctx.config())) {
            try {
                Map<String, StoredCacheData> cfgs = ctx.cache().context().pageStore().readCacheConfigurations();

                if (!F.isEmpty(cfgs))
                    storedCfgs = new ArrayList<>(cfgs.values());
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
            blt,
            forceChangeBaselineTopology,
            System.currentTimeMillis());

        try {
            if (log.isInfoEnabled())
                U.log(log, "Sending " + prettyStr(activate) + " request with BaselineTopology " + blt);

            ctx.discovery().sendCustomEvent(msg);

            if (ctx.isStopping())
                startedFut.onDone(new IgniteCheckedException("Failed to execute " + prettyStr(activate) + " request, " +
                    "node is stopping."));
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send global state change request: " + activate, e);

            startedFut.onDone(e);
        }

        return wrapStateChangeFuture(startedFut, msg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node, DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        if (node.isClient() || node.isDaemon())
            return null;

        if (discoData.joiningNodeData() == null) {
            if (globalState.baselineTopology() != null) {
                String msg = "Node not supporting BaselineTopology" +
                    " is not allowed to join the cluster with BaselineTopology";

                return new IgniteNodeValidationResult(node.id(), msg, msg);
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

            return new IgniteNodeValidationResult(node.id(), msg , msg);
        }

        if (joiningNodeState == null || joiningNodeState.baselineTopology() == null)
            return null;

        if (globalState == null || globalState.baselineTopology() == null) {
            if (joiningNodeState != null && joiningNodeState.baselineTopology() != null) {
                String msg = "Node with set up BaselineTopology is not allowed to join cluster without one: " + node.consistentId();

                return new IgniteNodeValidationResult(node.id(), msg, msg);
            }
        }

        if (globalState.transition() && globalState.previousBaselineTopology() == null) {
            //case when cluster is activating for the first time and other node with existing baseline topology
            //tries to join

            String msg = "Node with set up BaselineTopology is not allowed " +
                "to join cluster in the process of first activation: " + node.consistentId();

            return new IgniteNodeValidationResult(node.id(), msg, msg);
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

            return new IgniteNodeValidationResult(node.id(), msg, msg);
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

                return new IgniteNodeValidationResult(node.id(), msg, msg);
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

                return new IgniteNodeValidationResult(node.id(), msg, msg);
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
     * @param resFut State change future.
     */
    private void sendComputeChangeGlobalState(
        boolean activate,
        BaselineTopology blt,
        boolean forceBlt,
        final GridFutureAdapter<Void> resFut
    ) {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        if (log.isInfoEnabled()) {
            log.info("Sending " + prettyStr(activate) + " request from node [id=" + ctx.localNodeId() +
                ", topVer=" + topVer +
                ", client=" + ctx.clientNode() +
                ", daemon=" + ctx.isDaemon() + "]");
        }

        IgniteCompute comp = ((ClusterGroupAdapter)ctx.cluster().get().forServers()).compute();

        IgniteFuture<Void> fut = comp.runAsync(new ClientChangeGlobalStateComputeRequest(activate, blt, forceBlt));

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
    private boolean sendComputeCheckGlobalState() {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        if (log.isInfoEnabled()) {
            log.info("Sending check cluster state request from node [id=" + ctx.localNodeId() +
                ", topVer=" + topVer +
                ", client=" + ctx.clientNode() +
                ", daemon" + ctx.isDaemon() + "]");
        }

        ClusterGroupAdapter clusterGroupAdapter = (ClusterGroupAdapter)ctx.cluster().get().forServers();

        if (F.isEmpty(clusterGroupAdapter.nodes()))
            return false;

        IgniteCompute comp = clusterGroupAdapter.compute();

        return comp.call(new IgniteCallable<Boolean>() {
            @IgniteInstanceResource
            private Ignite ig;

            @Override public Boolean call() throws Exception {
                return ig.active();
            }
        });
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

                    sharedCtx.affinity().removeAllCacheInfo();

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
                "Failed to " + prettyStr(req.activate()) + " cluster",
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

                Exception e = null;

                try {
                    ctx.service().onUtilityCacheStarted();

                    ctx.service().onActivate(ctx);

                    ctx.dataStructures().onActivate(ctx);

                    ctx.igfs().onActivate(ctx);

                    ctx.task().onActivate(ctx);

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
    @Override public void onBaselineTopologyChanged
    (
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
            DiscoveryDataClusterState newState = DiscoveryDataClusterState.createState(false, blt);

            globalState = newState;
        }
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
     * @param activate Activate.
     * @return Activate flag string.
     */
    private static String prettyStr(boolean activate) {
        return activate ? "activate" : "deactivate";
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
        GridChangeGlobalStateFuture(UUID requestId, boolean activate, GridKernalContext ctx) {
            this.requestId = requestId;
            this.activate = activate;
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
    private static class ClientChangeGlobalStateComputeRequest implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final boolean activate;

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
        private ClientChangeGlobalStateComputeRequest(boolean activate, BaselineTopology blt, boolean forceBlt) {
            this.activate = activate;
            this.baselineTopology = blt;
            this.forceChangeBaselineTopology = forceBlt;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                ig.context().state().changeGlobalState(
                    activate,
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
