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
import java.lang.reflect.Field;
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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.BaselineConfigurationChangedEvent;
import org.apache.ignite.events.ClusterStateChangeStartedEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
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
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
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
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_STATE_ON_START;
import static org.apache.ignite.events.EventType.EVT_BASELINE_AUTO_ADJUST_AWAITING_TIME_CHANGED;
import static org.apache.ignite.events.EventType.EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.STATE_PROC;
import static org.apache.ignite.internal.IgniteFeatures.CLUSTER_READ_ONLY_MODE;
import static org.apache.ignite.internal.IgniteFeatures.SAFE_CLUSTER_DEACTIVATION;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.extractDataStorage;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistentCache;

/**
 *
 */
public class GridClusterStateProcessor extends GridProcessorAdapter implements IGridClusterStateProcessor, MetastorageLifecycleListener {
    /** */
    private static final String METASTORE_CURR_BLT_KEY = "metastoreBltKey";

    /** Warning of unsafe cluster deactivation. */
    public static final String DATA_LOST_ON_DEACTIVATION_WARNING = "Deactivation stopped. Deactivation clears " +
        "in-memory caches (without persistence) including the system caches.";

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

        distributedBaselineConfiguration.listenAutoAdjustEnabled(makeEventListener(
            EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED
        ));

        distributedBaselineConfiguration.listenAutoAdjustTimeout(makeEventListener(
            EVT_BASELINE_AUTO_ADJUST_AWAITING_TIME_CHANGED
        ));
    }

    /** */
    private DistributePropertyListener<Object> makeEventListener(int evtType) {
        //noinspection CodeBlock2Expr
        return (name, oldVal, newVal) -> {
            ctx.getStripedExecutorService().execute(() -> {
                if (ctx.event().isRecordable(evtType)) {
                    ctx.event().record(new BaselineConfigurationChangedEvent(
                        ctx.discovery().localNode(),
                        evtType == EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED
                            ? "Baseline auto-adjust \"enabled\" flag has been changed"
                            : "Baseline auto-adjust timeout has been changed",
                        evtType,
                        distributedBaselineConfiguration.isBaselineAutoAdjustEnabled(),
                        distributedBaselineConfiguration.getBaselineAutoAdjustTimeout()
                    ));
                }
            });
        };
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
    @Override public ClusterState publicApiState(boolean waitForTransition) {
        return publicApiStateAsync(waitForTransition).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<ClusterState> publicApiStateAsync(boolean asyncWaitForTransition) {
        if (ctx.isDaemon())
            return sendComputeCheckGlobalState();

        DiscoveryDataClusterState globalState = this.globalState;

        assert globalState != null;

        if (globalState.transition() && globalState.state().active()) {
            ClusterState transitionRes = globalState.transitionResult();

            if (transitionRes != null)
                return new IgniteFinishedFutureImpl<>(transitionRes);
            else {
                GridFutureAdapter<Void> fut = transitionFuts.get(globalState.transitionRequestId());

                if (fut != null) {
                    if (asyncWaitForTransition) {
                        return new IgniteFutureImpl<>(fut.chain((C1<IgniteInternalFuture<Void>, ClusterState>)f -> {
                            ClusterState res = globalState.transitionResult();

                            assert res != null;

                            return res;
                        }));
                    }
                    else
                        return new IgniteFinishedFutureImpl<>(stateWithMinimalFeatures(globalState.lastState(), globalState.state()));
                }

                transitionRes = globalState.transitionResult();

                assert transitionRes != null;

                return new IgniteFinishedFutureImpl<>(transitionRes);
            }
        }
        else
            return new IgniteFinishedFutureImpl<>(globalState.state());
    }

    /** {@inheritDoc} */
    @Override public boolean publicApiActiveState(boolean waitForTransition) {
        return publicApiActiveStateAsync(waitForTransition).get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> publicApiActiveStateAsync(boolean asyncWaitForTransition) {
        return publicApiStateAsync(asyncWaitForTransition).chain(f -> f.get().active());
    }

    /** {@inheritDoc} */
    @Override public long lastStateChangeTime() {
        return globalState.lastStateChangeTime();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        BaselineTopology blt = (BaselineTopology)metastorage.read(METASTORE_CURR_BLT_KEY);

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
    private void writeBaselineTopology(BaselineTopology blt,
        BaselineTopologyHistoryItem prevBltHistItem) throws IgniteCheckedException {
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
        IgniteConfiguration cfg = ctx.config();

        inMemoryMode = !CU.isPersistenceEnabled(cfg);

        ClusterState stateOnStart;

        if (inMemoryMode) {
            stateOnStart = cfg.getClusterStateOnStart();

            boolean activeOnStartSet = getBooleanFieldFromConfig(cfg, "activeOnStartPropSetFlag", false);

            if (activeOnStartSet) {
                if (stateOnStart != null)
                    log.warning("Property `activeOnStart` will be ignored due to the property `clusterStateOnStart` is presented.");
                else
                    stateOnStart = cfg.isActiveOnStart() ? ACTIVE : INACTIVE;
            }
            else if (stateOnStart == null)
                stateOnStart = DFLT_STATE_ON_START;
        }
        else {
            // Start first node as inactive if persistence is enabled.
            stateOnStart = INACTIVE;

            if (cfg.getClusterStateOnStart() != null && getBooleanFieldFromConfig(cfg, "autoActivationPropSetFlag", false))
                log.warning("Property `autoActivation` will be ignored due to the property `clusterStateOnStart` is presented.");
        }

        globalState = DiscoveryDataClusterState.createState(stateOnStart, null);

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

        if (state.state().active())
            checkLocalNodeInBaseline(state.baselineTopology());

        if (state.transition()) {
            joinFut = new TransitionOnJoinWaitFuture(state, discoCache);

            return joinFut;
        }
        else {
            ClusterState targetState = ctx.config().getClusterStateOnStart();

            if (targetState == null)
                targetState = ctx.config().isAutoActivationEnabled() ? ACTIVE : INACTIVE;

            boolean serverNode = !ctx.clientNode() && !ctx.isDaemon();
            boolean activation = !state.state().active() && targetState.active();

            if (serverNode && activation && !inMemoryMode) {
                if (isBaselineSatisfied(state.baselineTopology(), discoCache.serverNodes()))
                    changeGlobalState(targetState, true, state.baselineTopology().currentBaseline(), false);
            }
        }

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
                    new ChangeGlobalStateFinishMessage(globalState.transitionRequestId(), INACTIVE, false);

                onStateFinishMessage(msg);

                return msg;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onStateFinishMessage(ChangeGlobalStateFinishMessage msg) {
        DiscoveryDataClusterState discoClusterState = globalState;

        if (msg.requestId().equals(discoClusterState.transitionRequestId())) {
            if (log.isInfoEnabled())
                log.info("Received state change finish message: " + msg.state());

            globalState = discoClusterState.finish(msg.success());

            afterStateChangeFinished(msg.id(), msg.success());

            ctx.cache().onStateChangeFinish(msg);

            ctx.durableBackgroundTasksProcessor().onStateChangeFinish(msg);

            if (discoClusterState.lastState() == ACTIVE_READ_ONLY || globalState.state() == ACTIVE_READ_ONLY)
                ctx.cache().context().readOnlyMode(globalState.state() == ACTIVE_READ_ONLY);

            log.info("Cluster state was changed from " + discoClusterState.lastState() + " to " + globalState.state());

            if (!globalState.state().active())
                ctx.cache().context().readOnlyMode(false);

            TransitionOnJoinWaitFuture joinFut = this.joinFut;

            if (joinFut != null)
                joinFut.onDone(false);

            GridFutureAdapter<Void> transitionFut = transitionFuts.get(discoClusterState.transitionRequestId());

            if (transitionFut != null) {
                discoClusterState.setTransitionResult(msg.requestId(), msg.state());

                transitionFuts.remove(discoClusterState.transitionRequestId());

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

        if (log.isInfoEnabled()) {
            String baseline = msg.baselineTopology() == null ? ": null" : "[id=" + msg.baselineTopology().id() + ']';

            U.log(
                log,
                "Received " + prettyStr(msg.state()) +
                    " request with BaselineTopology" + baseline +
                    " initiator node ID: " + msg.initiatorNodeId()
            );
        }

        if (msg.baselineTopology() != null)
            compatibilityMode = false;

        if (state.transition()) {
            if (isApplicable(msg, state)) {
                GridChangeGlobalStateFuture fut = changeStateFuture(msg);

                if (fut != null)
                    fut.onDone(concurrentStateChangeError(msg.state(), state.state()));
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
                if (msg.state() == INACTIVE && !msg.forceDeactivation() && hasInMemoryCache() &&
                    allNodesSupports(ctx.discovery().serverNodes(topVer), SAFE_CLUSTER_DEACTIVATION)) {
                    GridChangeGlobalStateFuture stateFut = changeStateFuture(msg);

                    if (stateFut != null) {
                        stateFut.onDone(new IgniteException(DATA_LOST_ON_DEACTIVATION_WARNING
                            + " To deactivate cluster pass flag 'force'."));
                    }

                    return false;
                }

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
                    log.info("Started state transition: " + prettyStr(msg.state()));

                BaselineTopologyHistoryItem bltHistItem = BaselineTopologyHistoryItem.fromBaseline(
                    state.baselineTopology());

                transitionFuts.put(msg.requestId(), new GridFutureAdapter<Void>());

                DiscoveryDataClusterState newState = globalState = DiscoveryDataClusterState.createTransitionState(
                    msg.state(),
                    state,
                    activate(state.state(), msg.state()) || msg.forceChangeBaselineTopology() ? msg.baselineTopology() : state.baselineTopology(),
                    msg.requestId(),
                    topVer,
                    nodeIds
                );

                if (msg.forceChangeBaselineTopology())
                    newState.setTransitionResult(msg.requestId(), msg.state());

                AffinityTopologyVersion stateChangeTopVer = topVer.nextMinorVersion();

                StateChangeRequest req = new StateChangeRequest(
                    msg,
                    bltHistItem,
                    state.state(),
                    stateChangeTopVer
                );

                exchangeActions.stateChangeRequest(req);

                msg.exchangeActions(exchangeActions);

                if (newState.state() != state.state()) {
                    if (ctx.event().isRecordable(EventType.EVT_CLUSTER_STATE_CHANGE_STARTED)) {
                        ctx.getStripedExecutorService().execute(
                            () -> ctx.event().record(new ClusterStateChangeStartedEvent(
                                state.state(),
                                newState.state(),
                                ctx.discovery().localNode(),
                                "Cluster state change started."
                            ))
                        );
                    }
                }

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
        return msg.state() == state.state()
            && BaselineTopology.equals(msg.baselineTopology(), state.baselineTopology());
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataClusterState clusterState() {
        return globalState;
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataClusterState pendingState(ChangeGlobalStateMessage stateMsg) {
        ClusterState state;

        if (stateMsg.state().active())
            state = stateMsg.state();
        else
            state = stateMsg.forceChangeBaselineTopology() ? ACTIVE : INACTIVE;

        return DiscoveryDataClusterState.createState(state, stateMsg.baselineTopology());
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
     * @param state New state.
     * @param transitionState State in transition.
     * @return State change error.
     */
    protected IgniteCheckedException concurrentStateChangeError(ClusterState state, ClusterState transitionState) {
        return new IgniteCheckedException("Failed to " + prettyStr(state) +
            ", because another state change operation is currently in progress: " + prettyStr(transitionState));
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

            globalState = (DiscoveryDataClusterState)data.commonData();

            compatibilityMode = true;

            ctx.cache().context().readOnlyMode(globalState.state() == ACTIVE_READ_ONLY);

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

            ctx.cache().context().readOnlyMode(globalState.state() == ACTIVE_READ_ONLY);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> changeGlobalState(
        ClusterState state,
        boolean forceDeactivation,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology
    ) {
        return changeGlobalState(state, forceDeactivation, baselineNodes, forceChangeBaselineTopology, false);
    }

    /** */
    private BaselineTopology calculateNewBaselineTopology(
        ClusterState state,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology
    ) {
        BaselineTopology newBlt;

        BaselineTopology currBlt = globalState.baselineTopology();

        int newBltId = 0;

        if (currBlt != null)
            newBltId = state.active() ? currBlt.id() + 1 : currBlt.id();

        if (baselineNodes != null && !baselineNodes.isEmpty()) {
            List<BaselineNode> baselineNodes0 = new ArrayList<>();

            for (BaselineNode node : baselineNodes) {
                if (node instanceof ClusterNode) {
                    ClusterNode clusterNode = (ClusterNode)node;

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
        else if (state.active()) {
            if (baselineNodes == null)
                baselineNodes = baselineNodes();

            if (currBlt == null)
                newBlt = BaselineTopology.build(baselineNodes, newBltId);
            else {
                newBlt = currBlt;

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

    /**
     * @param state New cluster state.
     * @param forceDeactivation If {@code true}, cluster deactivation will be forced.
     * @param baselineNodes New baseline nodes.
     * @param forceChangeBaselineTopology Force change baseline topology.
     * @param isAutoAdjust Auto adjusting baseline flag.
     * @return State change future.
     * @see ClusterState#INACTIVE
     */
    public IgniteInternalFuture<?> changeGlobalState(
        ClusterState state,
        boolean forceDeactivation,
        Collection<? extends BaselineNode> baselineNodes,
        boolean forceChangeBaselineTopology,
        boolean isAutoAdjust
    ) {
        BaselineTopology blt = (compatibilityMode && !forceChangeBaselineTopology) ?
            null :
            calculateNewBaselineTopology(state, baselineNodes, forceChangeBaselineTopology);

        boolean isBaselineAutoAdjustEnabled = isBaselineAutoAdjustEnabled();

        if (forceChangeBaselineTopology && isBaselineAutoAdjustEnabled != isAutoAdjust)
            throw new BaselineAdjustForbiddenException(isBaselineAutoAdjustEnabled);

        if (ctx.isDaemon() || ctx.clientNode())
            return sendComputeChangeGlobalState(state, forceDeactivation, blt, forceChangeBaselineTopology);

        if (cacheProc.transactions().tx() != null || sharedCtx.lockedTopologyVersion(null) != null) {
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to " + prettyStr(state) +
                    " (must invoke the method outside of an active transaction).")
            );
        }

        DiscoveryDataClusterState curState = globalState;

        if (!curState.transition() && curState.state() == state) {
            if (!state.active() || BaselineTopology.equals(curState.baselineTopology(), blt))
                return new GridFinishedFuture<>();
        }

        GridChangeGlobalStateFuture startedFut = null;

        GridChangeGlobalStateFuture fut = stateChangeFut.get();

        while (fut == null || fut.isDone()) {
            fut = new GridChangeGlobalStateFuture(UUID.randomUUID(), state, ctx);

            if (stateChangeFut.compareAndSet(null, fut)) {
                startedFut = fut;

                break;
            }
            else
                fut = stateChangeFut.get();
        }

        if (startedFut == null) {
            if (fut.state != state) {
                return new GridFinishedFuture<>(
                    new IgniteCheckedException(
                        "Failed to " + prettyStr(state) +
                            ", because another state change operation is currently in progress: " + prettyStr(fut.state)
                    )
                );
            }
            else
                return fut;
        }

        List<StoredCacheData> storedCfgs = null;

        if (activate(curState.state(), state) && !inMemoryMode) {
            try {
                Map<String, StoredCacheData> cfgs = ctx.cache().context().pageStore().readCacheConfigurations();

                if (!F.isEmpty(cfgs)) {
                    storedCfgs = new ArrayList<>(cfgs.values());

                    IgniteDiscoverySpi spi = (IgniteDiscoverySpi)ctx.discovery().getInjectedDiscoverySpi();

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

        ChangeGlobalStateMessage msg = new ChangeGlobalStateMessage(
            startedFut.requestId,
            ctx.localNodeId(),
            storedCfgs,
            state,
            forceDeactivation,
            blt,
            forceChangeBaselineTopology,
            System.currentTimeMillis()
        );

        IgniteInternalFuture<?> resFut = wrapStateChangeFuture(startedFut, msg);

        try {
            U.log(log, "Sending " + prettyStr(state) + " request with BaselineTopology " + blt);

            ctx.discovery().sendCustomEvent(msg);

            if (ctx.isStopping()) {
                startedFut.onDone(
                    new IgniteCheckedException("Failed to execute " + prettyStr(state) + " request , node is stopping.")
                );
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send global state change request: " + prettyStr(state), e);

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

        if (globalState.state() == ACTIVE_READ_ONLY && !IgniteFeatures.nodeSupports(node, CLUSTER_READ_ONLY_MODE)) {
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
            joiningNodeState = marsh.unmarshal((byte[])discoData.joiningNodeData(), Thread.currentThread().getContextClassLoader());
        }
        catch (IgniteCheckedException e) {
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
     * @param state New cluster state.
     * @param forceDeactivation If {@code true}, cluster deactivation will be forced.
     * @param blt New cluster state.
     * @param forceBlt New cluster state.
     */
    private IgniteInternalFuture<Void> sendComputeChangeGlobalState(
        ClusterState state,
        boolean forceDeactivation,
        BaselineTopology blt,
        boolean forceBlt
    ) {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        U.log(
            log,
            "Sending " + prettyStr(state) +
                " request from node [id=" + ctx.localNodeId() +
                ", topVer=" + topVer +
                ", client=" + ctx.clientNode() +
                ", daemon=" + ctx.isDaemon() + "]"
        );

        IgniteCompute comp = ((ClusterGroupAdapter)ctx.cluster().get().forServers()).compute();

        IgniteFuture<Void> fut = comp.runAsync(new ClientSetClusterStateComputeRequest(state, forceDeactivation, blt,
            forceBlt));

        return ((IgniteFutureImpl<Void>)fut).internalFuture();
    }

    /**
     *  Check cluster state.
     *
     *  @return Cluster state.
     */
    private IgniteFuture<ClusterState> sendComputeCheckGlobalState() {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        if (log.isInfoEnabled()) {
            log.info("Sending check cluster state request from node [id=" + ctx.localNodeId() +
                ", topVer=" + topVer +
                ", client=" + ctx.clientNode() +
                ", daemon " + ctx.isDaemon() + "]");
        }

        ClusterGroupAdapter clusterGroupAdapter = (ClusterGroupAdapter)ctx.cluster().get().forServers();

        if (F.isEmpty(clusterGroupAdapter.nodes()))
            return new IgniteFinishedFutureImpl<>(INACTIVE);

        return clusterGroupAdapter.compute().callAsync(new ClientGetClusterStateComputeRequest());
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
            IgniteCheckedException e = new IgniteCheckedException("Failed to " + prettyStr(req.state()), null, false);

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
                if (req.state().active())
                    onFinalActivate(req);

                globalState.setTransitionResult(req.requestId(), req.state());
            }

            sendChangeGlobalStateResponse(req.requestId(), req.initiatorNodeId(), null);
        }
        catch (Exception ex) {
            Exception e = new IgniteCheckedException("Failed to perform final activation steps", ex);

            U.error(log, "Failed to perform final activation steps [nodeId=" + ctx.localNodeId() +
                ", client=" + ctx.clientNode() + ", topVer=" + req.topologyVersion() + "]. New state: " + req.state(), ex);

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

        if (!state.state().active() && !state.transition() && state.baselineTopology() == null)
            globalState = DiscoveryDataClusterState.createState(INACTIVE, blt);
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
            && oldState.state().active()
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
                    oldState.state(),
                    true,
                    newBlt,
                    true,
                    System.currentTimeMillis()
                );

                AffinityTopologyVersion ver = new AffinityTopologyVersion(topVer, minorTopVer);

                onStateChangeMessage(ver, changeGlobalStateMsg, discoCache);

                ChangeGlobalStateFinishMessage finishMsg = new ChangeGlobalStateFinishMessage(nodeId, oldState.state(), true);

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
                clusterState.state().active() ? clusterState.state() : ACTIVE,
                true,
                blt,
                true,
                System.currentTimeMillis()
            );

            StateChangeRequest stateChangeReq = new StateChangeRequest(
                msg,
                BaselineTopologyHistoryItem.fromBaseline(blt),
                msg.state(),
                null
            );

            if (exchActs == null)
                exchActs = new ExchangeActions();

            exchActs.stateChangeRequest(stateChangeReq);
        }

        return exchActs;
    }

    /** {@inheritDoc} */
    @Override public void onExchangeFinishedOnCoordinator(IgniteInternalFuture exchangeFuture,
        boolean hasMovingPartitions) {
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
    public BaselineAutoAdjustStatus baselineAutoAdjustStatus() {
        return changeTopologyWatcher.getStatus();
    }

    /**
     * @param state Cluster state.
     * @return Cluster state string representation.
     */
    private static String prettyStr(ClusterState state) {
        switch (state) {
            case ACTIVE:
                return "activate cluster";

            case INACTIVE:
                return "deactivate cluster";

            case ACTIVE_READ_ONLY:
                return "activate cluster in read-only mode";

            default:
                throw new IllegalStateException("Unknown cluster state: " + state);
        }
    }

    /**
     * Checks that activation process is happening now.
     *
     * @param state Current cluster state.
     * @param newState Cluster state after finish transition.
     * @return {@code True} if activation process is happening now, and {@code False} otherwise.
     */
    private boolean activate(ClusterState state, ClusterState newState) {
        assert state != null;
        assert newState != null;

        return state == INACTIVE && newState.active();
    }

    /**
     * Gets from given config {@code cfg} field with name {@code fieldName} and type boolean.
     *
     * @param cfg Config.
     * @param fieldName Name of field.
     * @param defaultValue Default value of field, if field is not presented or empty.
     * @return Value of field, or {@code defaultValue} in case of any errors.
     */
    private boolean getBooleanFieldFromConfig(IgniteConfiguration cfg, String fieldName, boolean defaultValue) {
        A.notNull(cfg, "cfg");
        A.notNull(fieldName, "fieldName");

        Field field = U.findField(IgniteConfiguration.class, fieldName);

        try {
            if (field != null) {
                field.setAccessible(true);

                boolean val = defaultValue;

                try {
                    val = field.getBoolean(cfg);
                }
                catch (IllegalAccessException | IllegalArgumentException | NullPointerException e) {
                    log.error("Can't get value of field with name " + fieldName + " from config: " + cfg, e);
                }

                return val;
            }
        }
        catch (SecurityException e) {
            log.error("Can't get field with name " + fieldName + " from config: " + cfg + " due to security reasons", e);
        }

        return defaultValue;
    }

    /**
     * @return {@code True} if cluster has in-memory caches (without persistence) including the system caches.
     * {@code False} otherwise.
     */
    private boolean hasInMemoryCache() {
        return ctx.cache().cacheDescriptors().values().stream()
            .anyMatch(desc -> !isPersistentCache(desc.cacheConfiguration(), ctx.config().getDataStorageConfiguration())
                && (!desc.cacheConfiguration().isWriteBehindEnabled() || !desc.cacheConfiguration().isReadThrough()));
    }

    /**
     * Gets state of given two with minimal number of features.
     * <p/>
     * The order: {@link ClusterState#ACTIVE} > {@link ClusterState#ACTIVE_READ_ONLY} > {@link ClusterState#INACTIVE}.
     * <p/>
     * Explain:
     * <br/>
     * {@link ClusterState#INACTIVE} has the smallast number of available features. You can't use caches in this state.
     * <br/>
     * In {@link ClusterState#ACTIVE_READ_ONLY} you have more available features than {@link ClusterState#INACTIVE} state,
     * such as reading from caches, but can't write into them.
     * <br/> In {@link ClusterState#ACTIVE} you can update caches. It's a state with the biggest number of features.
     *
     * @param state1 First given state.
     * @param state2 Second given state.
     * @return State with minimal number of available features.
     */
    public static ClusterState stateWithMinimalFeatures(ClusterState state1, ClusterState state2) {
        if (state1 == state2)
            return state1;

        if (state1 == INACTIVE || state2 == INACTIVE)
            return INACTIVE;

        if (state1 == ACTIVE_READ_ONLY || state2 == ACTIVE_READ_ONLY)
            return ACTIVE_READ_ONLY;

        throw new IllegalArgumentException("Unknown cluster states. state1: " + state1 + ", state2: " + state2);
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

        /** Cluster state. */
        @GridToStringInclude
        private final ClusterState state;

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
         * @param reqId State change request ID.
         * @param state New cluster state.
         * @param ctx Context.
         */
        GridChangeGlobalStateFuture(UUID reqId, ClusterState state, GridKernalContext ctx) {
            this.requestId = reqId;
            this.state = state;
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

        /** */
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
