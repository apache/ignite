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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import org.apache.ignite.lang.IgniteRunnable;
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
public class GridClusterStateProcessorImpl extends GridProcessorAdapter implements GridClusterStateProcessor, MetastorageLifecycleListener {
    /** */
    private static final String METASTORE_CURR_BLT_KEY = "metastoreBltKey";

    /** */
    private volatile DiscoveryDataClusterState globalState;

    /** */
    private final BaselineTopologyHistory bltHist = new BaselineTopologyHistory();

    /** Local action future. */
    private final AtomicReference<GridChangeGlobalStateFuture> stateChangeFut = new AtomicReference<>();

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
    public GridClusterStateProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public boolean publicApiActiveState() {
        if (ctx.isDaemon())
            return sendComputeCheckGlobalState();

        DiscoveryDataClusterState globalState = this.globalState;

        assert globalState != null;

        if (globalState.transition()) {
            Boolean transitionRes = globalState.transitionResult();

            if (transitionRes != null)
                return transitionRes;
            else
                return false;
        }
        else
            return globalState.active();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        BaselineTopology blt = (BaselineTopology) metastorage.read(METASTORE_CURR_BLT_KEY);

        if (blt != null)
            bltHist.restoreHistory(metastorage, blt.id());

        onStateRestored(blt);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastorage = metastorage;

        saveBaselineTopology(globalState.baselineTopology(), null);
    }

    /**
     * @param blt Blt.
     */
    private void saveBaselineTopology(BaselineTopology blt, BaselineTopologyHistoryItem prevBltHistItem) throws IgniteCheckedException {
        assert metastorage != null;

        bltHist.addHistoryItem(metastorage, prevBltHistItem);

        if (blt != null)
            metastorage.write(METASTORE_CURR_BLT_KEY, blt);
        else
            metastorage.remove(METASTORE_CURR_BLT_KEY);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // Start first node as inactive if persistence is enabled.
        boolean activeOnStart = !CU.isPersistenceEnabled(ctx.config()) && ctx.config().isActiveOnStart();

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

        if (state.transition()) {
            joinFut = new TransitionOnJoinWaitFuture(state, discoCache);

            return joinFut;
        }
        else if (!ctx.clientNode() && !ctx.isDaemon() && !state.active() && state.baselineTopology() != null &&
            state.baselineTopology().isSatisfied(discoCache.serverNodes())) {
            changeGlobalState0(true, state.baselineTopology());
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public ChangeGlobalStateFinishMessage onNodeLeft(ClusterNode node) {
        if (globalState.transition()) {
            Set<UUID> nodes = globalState.transitionNodes();

            if (nodes.remove(node.id()) && nodes.isEmpty()) {
                U.warn(log, "Failed to change cluster state, all participating nodes failed. " +
                    "Switching to inactive state.");

                ChangeGlobalStateFinishMessage msg =
                    new ChangeGlobalStateFinishMessage(globalState.transitionRequestId(), false, null);

                onStateFinishMessage(msg);

                return msg;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onStateFinishMessage(ChangeGlobalStateFinishMessage msg) {
        if (msg.requestId().equals(globalState.transitionRequestId())) {
            log.info("Received state change finish message: " + msg.clusterActive());

            globalState = DiscoveryDataClusterState.createState(msg.clusterActive(), msg.baselineTopology());

            ctx.cache().onStateChangeFinish(msg);

            TransitionOnJoinWaitFuture joinFut = this.joinFut;

            if (joinFut != null)
                joinFut.onDone(false);
        }
        else
            U.warn(log, "Received state finish message with unexpected ID: " + msg);
    }

    /** {@inheritDoc} */
    @Override public boolean onStateChangeMessage(AffinityTopologyVersion topVer,
        ChangeGlobalStateMessage msg,
        DiscoCache discoCache) {
        DiscoveryDataClusterState state = globalState;

        if (state.transition()) {
            if (isApplicable(msg, state)) {
                GridChangeGlobalStateFuture fut = changeStateFuture(msg);

                if (fut != null)
                    fut.onDone(concurrentStateChangeError(msg.activate()));
            }
            else {
                final GridChangeGlobalStateFuture stateFut = changeStateFuture(msg);

                if (stateFut != null) {
                    IgniteInternalFuture<?> exchFut = ctx.cache().context().exchange().affinityReadyFuture(
                        state.transitionTopologyVersion());

                    if (exchFut == null)
                        exchFut = new GridFinishedFuture<>();

                    exchFut.listen(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> exchFut) {
                            stateFut.onDone();
                        }
                    });
                }
            }
        }
        else {
            if (isApplicable(msg, state)) {
                ExchangeActions exchangeActions;

                try {
                    exchangeActions = ctx.cache().onStateChangeRequest(msg, topVer);
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

                BaselineTopologyHistoryItem bltHistItem = BaselineTopologyHistoryItem.fromBaseline(globalState.baselineTopology());

                globalState = DiscoveryDataClusterState.createTransitionState(msg.activate(),
                    msg.baselineTopology(),
                    msg.requestId(),
                    topVer,
                    nodeIds);

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
    private static boolean isApplicable(ChangeGlobalStateMessage msg, DiscoveryDataClusterState state) {
        if (msg.activate() != state.active())
            return true;

        if ((state.baselineTopology() == null) != (msg.baselineTopology() == null))
            return true;

        if (state.baselineTopology() == null && msg.baselineTopology() == null)
            return false;

        return !msg.baselineTopology().equals(state.baselineTopology());
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataClusterState clusterState() {
        return globalState;
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
    private IgniteCheckedException concurrentStateChangeError(boolean activate) {
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
        dataBag.addJoiningNodeData(discoveryDataType().ordinal(), globalState);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!dataBag.commonDataCollectedFor(STATE_PROC.ordinal()))
            dataBag.addGridCommonData(STATE_PROC.ordinal(), globalState);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        DiscoveryDataClusterState state = (DiscoveryDataClusterState)data.commonData();

        if (state != null) {
            DiscoveryDataClusterState curState = globalState;

            //TODO this exception has to be eliminated as joining node makes compatibility checks earlier in join process
//            if (curState != null && curState.baselineTopology() != null && state.baselineTopology() != null &&
//                !BaselineTopology.equals(curState.baselineTopology(), state.baselineTopology()))
//                throw new IgniteException("Baseline topology mismatch: " + curState.baselineTopology() + " " + state.baselineTopology());

            globalState = state;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> changeGlobalState(final boolean activate) {
        return changeGlobalState(activate, null, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> changeGlobalState(final boolean activate,
        Collection<BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology) {
        BaselineTopology newBlt;

        BaselineTopology currentBlt = globalState.baselineTopology();

        int newBltId = currentBlt == null ? 0 : currentBlt.id() + 1;

        if (forceChangeBaselineTopology)
            newBlt = BaselineTopology.build(baselineNodes, newBltId);
        else if (activate
            && baselineNodes != null
            && currentBlt != null)
        {
            newBlt = currentBlt;

            newBlt.updateHistory(baselineNodes);
        }
        else
            newBlt = BaselineTopology.build(baselineNodes, newBltId);

        return changeGlobalState0(activate, newBlt);
    }

    /** */
    private IgniteInternalFuture<?> changeGlobalState0(final boolean activate,
            BaselineTopology blt) {
        if (ctx.isDaemon() || ctx.clientNode()) {
            GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

            sendComputeChangeGlobalState(activate, fut);

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
            blt);

        try {
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
        if (globalState == null
            || globalState.baselineTopology() == null
            || discoData.joiningNodeData() == null
            || ((DiscoveryDataClusterState) discoData.joiningNodeData()).baselineTopology() == null)
            return null;

        BaselineTopology joiningNodeBlt = ((DiscoveryDataClusterState) discoData.joiningNodeData()).baselineTopology();
        BaselineTopology clusterBlt = globalState.baselineTopology();

        String msg = "BaselineTopology of joining node is not compatible with BaselineTopology in cluster.";

        if (joiningNodeBlt.id() > clusterBlt.id())
            return new IgniteNodeValidationResult(node.id(), msg, msg);

        if (joiningNodeBlt.id() == clusterBlt.id()) {
            if (!clusterBlt.isCompatibleWith(joiningNodeBlt))
                return new IgniteNodeValidationResult(node.id(), msg, msg);
        }
        else if (joiningNodeBlt.id() < clusterBlt.id()) {
            if (!bltHist.isCompatibleWith(joiningNodeBlt))
                return new IgniteNodeValidationResult(node.id(), msg, msg);
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
    private void sendComputeChangeGlobalState(boolean activate, final GridFutureAdapter<Void> resFut) {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        if (log.isInfoEnabled()) {
            log.info("Sending " + prettyStr(activate) + " request from node [id=" + ctx.localNodeId() +
                ", topVer=" + topVer +
                ", client=" + ctx.clientNode() +
                ", daemon=" + ctx.isDaemon() + "]");
        }

        IgniteCompute comp = ((ClusterGroupAdapter)ctx.cluster().get().forServers()).compute();

        IgniteFuture<Void> fut = comp.runAsync(new ClientChangeGlobalStateComputeRequest(activate));

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
        IgniteCompute comp = ((ClusterGroupAdapter)ctx.cluster().get().forServers()).compute();

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
    @Override public void onBaselineTopologyChanged(BaselineTopology blt, BaselineTopologyHistoryItem prevBltHistItem) throws IgniteCheckedException {
        saveBaselineTopology(blt, prevBltHistItem);
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

    /**
     * @param activate Activate.
     * @return Activate flag string.
     */
    private static String prettyStr(boolean activate) {
        return activate ? "activate" : "deactivate";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClusterStateProcessorImpl.class, this);
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

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ig;

        /**
         * @param activate New cluster state.
         */
        private ClientChangeGlobalStateComputeRequest(boolean activate) {
            this.activate = activate;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ig.active(activate);
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
}
