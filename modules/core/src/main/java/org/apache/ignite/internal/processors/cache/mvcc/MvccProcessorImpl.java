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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQueryCntr;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQueryId;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQueryCntr;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQueryId;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccActiveQueriesMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccFutureResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccQuerySnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccRecoveryFinishedMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccSnapshotResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccTxSnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccWaitTxsRequest;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxKey;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DatabaseLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker.MVCC_TRACKER_ID_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_INITIAL_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_READ_OP_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_START_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_START_OP_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.hasNewVersion;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.noCoordinatorError;
import static org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog.TX_LOG_CACHE_ID;
import static org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog.TX_LOG_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.KEY_ONLY;

/**
 * MVCC processor.
 */
@SuppressWarnings("serial")
public class MvccProcessorImpl extends GridProcessorAdapter implements MvccProcessor, DatabaseLifecycleListener {
    /** */
    private static final boolean FORCE_MVCC =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, false);

    /** */
    private static final IgniteProductVersion MVCC_SUPPORTED_SINCE = IgniteProductVersion.fromString("2.7.0");

    /** */
    private static final Waiter LOCAL_TRANSACTION_MARKER = new LocalTransactionMarker();

    /** Dummy tx for vacuum. */
    private static final IgniteInternalTx DUMMY_TX = new GridNearTxLocal();

    /** For tests only. */
    private static IgniteClosure<Collection<ClusterNode>, ClusterNode> crdC;

    /**
     * For testing only.
     *
     * @param crdC Closure assigning coordinator.
     */
    static void coordinatorAssignClosure(IgniteClosure<Collection<ClusterNode>, ClusterNode> crdC) {
        MvccProcessorImpl.crdC = crdC;
    }

    /** Topology version when local node was assigned as coordinator. */
    private volatile long crdVer;

    /** */
    private volatile MvccCoordinator curCrd;

    /** */
    private TxLog txLog;

    /** */
    private List<GridWorker> vacuumWorkers;

    /** */
    private BlockingQueue<VacuumTask> cleanupQueue;

    /**
     * Vacuum mutex. Prevents concurrent vacuum while start/stop operations
     */
    private final Object mux = new Object();

    /** */
    private final GridAtomicLong futIdCntr = new GridAtomicLong(0);

    /** */
    private final GridAtomicLong mvccCntr = new GridAtomicLong(MVCC_START_CNTR);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(MVCC_INITIAL_CNTR);

    /**
     * Contains active transactions on mvcc coordinator. Key is mvcc counter.
     * Access is protected by "this" monitor.
     */
    private final Map<Long, ActiveTx> activeTxs = new HashMap<>();

    /** Active query trackers. */
    private final Map<Long, MvccQueryTracker> activeTrackers = new ConcurrentHashMap<>();

    /** */
    private final Map<UUID, Map<Long, MvccSnapshotResponseListener>> snapLsnrs = new ConcurrentHashMap<>();

    /** */
    private final Map<Long, WaitAckFuture> ackFuts = new ConcurrentHashMap<>();

    /** */
    private final Map<Long, GridFutureAdapter> waitTxFuts = new ConcurrentHashMap<>();

    /** */
    private final Map<TxKey, Waiter> waitMap = new ConcurrentHashMap<>();

    /** */
    private final ActiveQueries activeQueries = new ActiveQueries();

    /** */
    private final MvccPreviousCoordinatorQueries prevCrdQueries = new MvccPreviousCoordinatorQueries();

    /** */
    private final GridFutureAdapter<Void> initFut = new GridFutureAdapter<>();

    /** Flag whether at least one cache with {@code CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT} mode is registered. */
    private volatile boolean mvccEnabled;

    /** Flag whether all nodes in cluster support MVCC. */
    private volatile boolean mvccSupported = true;

    /** Flag whether coordinator was changed by the last discovery event. */
    private volatile boolean crdChanged;

    /**
     * Maps failed node id to votes accumulator for that node.
     */
    private final ConcurrentHashMap<UUID, RecoveryBallotBox> recoveryBallotBoxes = new ConcurrentHashMap<>();

    /**
     * @param ctx Context.
     */
    public MvccProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerDatabaseListener(this);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    onDiscovery((DiscoveryEvent)evt);
                }
            },
            EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);

        ctx.io().addMessageListener(TOPIC_CACHE_COORDINATOR, new CoordinatorMessageListener());

        ctx.discovery().setCustomEventListener(DynamicCacheChangeBatch.class,
            new CustomEventListener<DynamicCacheChangeBatch>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, DynamicCacheChangeBatch msg) {
                    checkMvccCacheStarted(msg);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean mvccEnabled() {
        return mvccEnabled;
    }

    /** {@inheritDoc} */
    @Override public void preProcessCacheConfiguration(CacheConfiguration ccfg) {
        if (FORCE_MVCC && ccfg.getAtomicityMode() == TRANSACTIONAL && !CU.isSystemCache(ccfg.getName())) {
            ccfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
            ccfg.setNearConfiguration(null);
        }

        if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
            if (!mvccSupported)
                throw new IgniteException("Cannot start MVCC transactional cache. " +
                    "MVCC is unsupported by the cluster.");

            mvccEnabled = true;
        }
    }

    /** {@inheritDoc} */
    @Override public void validateCacheConfiguration(CacheConfiguration ccfg) {
        if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
            if (!mvccSupported)
                throw new IgniteException("Cannot start MVCC transactional cache. " +
                    "MVCC is unsupported by the cluster.");

            mvccEnabled = true;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        if (mvccEnabled && node.version().compareToIgnoreTimestamp(MVCC_SUPPORTED_SINCE) < 0) {
            String errMsg = "Failed to add node to topology. MVCC is enabled on the cluster, but " +
                "the node doesn't support MVCC [nodeId=" + node.id() + ']';

            return new IgniteNodeValidationResult(node.id(), errMsg, errMsg);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void ensureStarted() throws IgniteCheckedException {
        if (!ctx.clientNode()) {
            assert mvccEnabled && mvccSupported;

            if (txLog == null)
                txLog = new TxLog(ctx, ctx.cache().context().database());

            startVacuumWorkers();

            if (log.isInfoEnabled())
                log.info("Mvcc processor started.");
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeStop(IgniteCacheDatabaseSharedManager mgr) {
        stopVacuumWorkers();

        txLog = null;
    }

    /** {@inheritDoc} */
    @Override public void onInitDataRegions(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        // We have to always init txLog data region.
        DataStorageConfiguration dscfg = dataStorageConfiguration();

        mgr.addDataRegion(
            dscfg,
            createTxLogRegion(dscfg),
            CU.isPersistenceEnabled(ctx.config()));
    }

    /** {@inheritDoc} */
    @Override public void beforeResumeWalLogging(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        // In case of blt changed we should re-init TX_LOG cache.
        txLogPageStoreInit(mgr);
    }

    /** {@inheritDoc} */
    @Override public void beforeBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        txLogPageStoreInit(mgr);

        boolean hasMvccCaches = ctx.cache().persistentCaches().stream()
            .anyMatch(c -> c.cacheConfiguration().getAtomicityMode() == TRANSACTIONAL_SNAPSHOT);

        if (hasMvccCaches) {
            txLog = new TxLog(ctx, mgr);

            mvccEnabled = true;
        }
    }

    /**
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    private void txLogPageStoreInit(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());

        ctx.cache().context().pageStore().initialize(TX_LOG_CACHE_ID, 1,
            TX_LOG_CACHE_NAME, mgr.dataRegion(TX_LOG_CACHE_NAME).memoryMetrics());
    }

    /** {@inheritDoc} */
    @Override public void onExchangeDone(DiscoCache discoCache) {
        MvccCoordinator curCrd0 = curCrd;

        if (crdChanged) {
            // Rollback all transactions with old snapshots.
            ctx.cache().context().tm().rollbackMvccTxOnCoordinatorChange();

            // Complete init future if local node is a new coordinator. All previous txs are already completed here.
            if (crdVer != 0 && !initFut.isDone()) {
                assert curCrd0 != null && curCrd0.nodeId().equals(ctx.localNodeId());

                initFut.onDone();
            }

            crdChanged = false;
        }
        else {
            if (curCrd0 != null && ctx.localNodeId().equals(curCrd0.nodeId()) && discoCache != null)
                cleanupOrphanedServerTransactions(discoCache.serverNodes());
        }
    }

    /** {@inheritDoc} */
    @Override public void onLocalJoin(DiscoveryEvent evt) {
        assert evt.type() == EVT_NODE_JOINED && ctx.localNodeId().equals(evt.eventNode().id());

        onCoordinatorChanged(evt.topologyNodes(), evt.topologyVersion(), false);
    }

    /**
     * Discovery listener. Note: initial join event is handled by {@link MvccProcessorImpl#onLocalJoin(DiscoveryEvent)}
     * method.
     *
     * @param evt Discovery event.
     */
    private void onDiscovery(DiscoveryEvent evt) {
        assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_JOINED;

        UUID nodeId = evt.eventNode().id();

        MvccCoordinator curCrd0 = curCrd;

        if (evt.type() == EVT_NODE_JOINED) {
            if (curCrd0 == null) // Handle join event only if coordinator has not been elected yet.
                onCoordinatorChanged(evt.topologyNodes(), evt.topologyVersion(), false);

            return;
        }

        // Process mvcc coordinator left event on the rest nodes.
        if (nodeId.equals(curCrd0.nodeId())) {
            // 1. Notify all listeners waiting for a snapshot.
            Map<Long, MvccSnapshotResponseListener> map = snapLsnrs.remove(nodeId);

            if (map != null) {
                ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException("Failed to request mvcc " +
                    "version, coordinator failed: " + nodeId);

                MvccSnapshotResponseListener lsnr;

                for (Long id : map.keySet()) {
                    if ((lsnr = map.remove(id)) != null)
                        lsnr.onError(ex);
                }
            }

            // 2. Notify acknowledge futures.
            for (WaitAckFuture fut : ackFuts.values())
                fut.onNodeLeft(nodeId);

            // 3. Process coordinator change.
            onCoordinatorChanged(evt.topologyNodes(), evt.topologyVersion(), true);
        }
        // Process node left event on the current mvcc coordinator.
        else if (curCrd0.nodeId().equals(ctx.localNodeId())) {
            // 1. Notify active queries.
            activeQueries.onNodeFailed(nodeId);

            // 2. Notify previous queries.
            prevCrdQueries.onNodeFailed(nodeId);

            // 3. Recover transactions started by the failed node.
            recoveryBallotBoxes.forEach((nearNodeId, ballotBox) -> {
                // Put synthetic vote from another failed node
                ballotBox.vote(nodeId);

                tryFinishRecoveryVoting(nearNodeId, ballotBox);
            });

            if (evt.eventNode().isClient()) {
                RecoveryBallotBox ballotBox = recoveryBallotBoxes
                    .computeIfAbsent(nodeId, uuid -> new RecoveryBallotBox());

                ballotBox
                    .voters(evt.topologyNodes().stream().map(ClusterNode::id).collect(Collectors.toList()));

                tryFinishRecoveryVoting(nodeId, ballotBox);
            }
        }
    }

    /**
     * Coordinator change callback. Performs all needed actions for handling new coordinator assignment.
     *
     * @param nodes Cluster topology snapshot.
     * @param topVer Topology version.
     * @param sndQrys {@code True} if it is need to send an active queries list to the new coordinator.
     */
    private void onCoordinatorChanged(Collection<ClusterNode> nodes, long topVer, boolean sndQrys) {
        MvccCoordinator newCrd = pickMvccCoordinator(nodes, topVer);

        if (newCrd == null)
            return;

        // Update current coordinator, collect active queries and send it to the new coordinator if needed.
        GridLongList activeQryTrackers = null;

        synchronized (activeTrackers) {
            assert  curCrd == null || newCrd.topologyVersion().compareTo(curCrd.topologyVersion()) > 0;

            if (sndQrys) {
                activeQryTrackers = new GridLongList();

                for (MvccQueryTracker tracker : activeTrackers.values()) {
                    long trackerId = tracker.onMvccCoordinatorChange(newCrd);

                    if (trackerId != MVCC_TRACKER_ID_NA)
                        activeQryTrackers.add(trackerId);
                }
            }

            curCrd = newCrd;
        }

        // Send local active queries to remote coordinator, if needed.
        if (!newCrd.nodeId().equals(ctx.localNodeId())) {
            try {
                if (sndQrys)
                    sendMessage(newCrd.nodeId(), new MvccActiveQueriesMessage(activeQryTrackers));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send active queries to mvcc coordinator: " + e);
            }
        }
        // If a current node was elected as a new mvcc coordinator, we need to pre-initialize it.
        else {
            assert crdVer == 0 : crdVer;

            crdVer = newCrd.coordinatorVersion();

            if (log.isInfoEnabled())
                log.info("Initialize local node as mvcc coordinator [node=" + ctx.localNodeId() +
                    ", crdVer=" + crdVer + ']');

            prevCrdQueries.init(activeQryTrackers, F.view(nodes, this::supportsMvcc), ctx.discovery());

            // Do not complete init future here, because we should wait until all old transactions become terminated.
        }

        crdChanged = true;
    }

    /**
     * Cleans up active transactions lost near node which is server. Executed on coordinator.
     *
     * @param liveSrvs Live server nodes at the moment of cleanup.
     */
    private void cleanupOrphanedServerTransactions(Collection<ClusterNode> liveSrvs) {
        Set<UUID> ids = liveSrvs.stream()
            .map(ClusterNode::id)
            .collect(Collectors.toSet());

        List<Long> forRmv = new ArrayList<>();

        synchronized (this) {
            for (Map.Entry<Long, ActiveTx> entry : activeTxs.entrySet()) {
                // If node started tx is not known as live then remove such tx from active list
                ActiveTx activeTx = entry.getValue();

                if (activeTx.getClass() == ActiveServerTx.class && !ids.contains(activeTx.nearNodeId))
                    forRmv.add(entry.getKey());
            }
        }

        for (Long txCntr : forRmv)
            // Committed counter is increased because it is not known if transaction was committed or not and we must
            // bump committed counter for committed transaction as it is used in (read-only) query snapshot.
            onTxDone(txCntr, true);
    }

    /** {@inheritDoc} */
    @Override public void processClientActiveQueries(UUID nodeId, @Nullable GridLongList activeQueries) {
        prevCrdQueries.addNodeActiveQueries(nodeId, activeQueries);
    }

    /** {@inheritDoc} */
    @Override @Nullable public MvccCoordinator currentCoordinator() {
        return curCrd;
    }

    /** {@inheritDoc} */
    @Override public UUID currentCoordinatorId() {
        MvccCoordinator curCrd = this.curCrd;

        return curCrd != null ? curCrd.nodeId() : null;
    }

    /** {@inheritDoc} */
    @Override public byte state(MvccVersion ver) throws IgniteCheckedException {
        return state(ver.coordinatorVersion(), ver.counter());
    }

    /** {@inheritDoc} */
    @Override public byte state(long crdVer, long cntr) throws IgniteCheckedException {
        assert txLog != null && mvccEnabled;

        return txLog.get(crdVer, cntr);
    }

    /** {@inheritDoc} */
    @Override public void updateState(MvccVersion ver, byte state) throws IgniteCheckedException {
        updateState(ver, state, true);
    }

    /** {@inheritDoc} */
    @Override public void updateState(MvccVersion ver, byte state, boolean primary) throws IgniteCheckedException {
        assert txLog != null && mvccEnabled;

        TxKey key = new TxKey(ver.coordinatorVersion(), ver.counter());

        txLog.put(key, state, primary);

        Waiter waiter;

        if (primary && (state == TxState.ABORTED || state == TxState.COMMITTED)
            && (waiter = waitMap.remove(key)) != null)
            waiter.run(ctx);
    }

    /** {@inheritDoc} */
    @Override public void registerLocalTransaction(long crd, long cntr) {
        Waiter old = waitMap.putIfAbsent(new TxKey(crd, cntr), LOCAL_TRANSACTION_MARKER);

        assert old == null || old.hasLocalTransaction();
    }

    /** {@inheritDoc} */
    @Override public boolean hasLocalTransaction(long crd, long cntr) {
        Waiter waiter = waitMap.get(new TxKey(crd, cntr));

        return waiter != null && waiter.hasLocalTransaction();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> waitFor(GridCacheContext cctx, MvccVersion locked,
        MvccVersion blockedVersion) throws IgniteCheckedException {
        TxKey key = new TxKey(locked.coordinatorVersion(), locked.counter());

        LockFuture fut = new LockFuture(cctx.ioPolicy(), blockedVersion);

        Waiter waiter = waitMap.merge(key, fut, Waiter::concat);

        byte state = txLog.get(key);

        if ((state == TxState.ABORTED || state == TxState.COMMITTED)
            && !waiter.hasLocalTransaction() && (waiter = waitMap.remove(key)) != null)
            waiter.run(ctx);

        // t0d0 watch for exact falling asleep condition
        new DdCollaborator(ctx.cache().context())
            .startComputation(blockedVersion, locked);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void addQueryTracker(MvccQueryTracker tracker) {
        assert tracker.id() != MVCC_TRACKER_ID_NA;

        MvccQueryTracker tr = activeTrackers.put(tracker.id(), tracker);

        assert tr == null;
    }

    /** {@inheritDoc} */
    @Override public void removeQueryTracker(Long id) {
        activeTrackers.remove(id);
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot tryRequestSnapshotLocal() throws ClusterTopologyCheckedException {
        return tryRequestSnapshotLocal(null);
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot tryRequestSnapshotLocal(@Nullable IgniteInternalTx tx) throws ClusterTopologyCheckedException {
        MvccCoordinator crd = currentCoordinator();

        if (crd == null)
            throw noCoordinatorError();

        if (tx != null) {
            AffinityTopologyVersion topVer = ctx.cache().context().lockedTopologyVersion(null);

            if (topVer != null && topVer.compareTo(crd.topologyVersion()) < 0)
                throw new ClusterTopologyCheckedException("Mvcc coordinator is outdated " +
                    "for the locked topology version. [crd=" + crd + ", tx=" + tx + ']');
        }

        if (!ctx.localNodeId().equals(crd.nodeId()) || !initFut.isDone())
            return null;
        else if (tx != null)
            return assignTxSnapshot(0L, ctx.localNodeId(), false);
        else
            return activeQueries.assignQueryCounter(ctx.localNodeId(), 0L);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshotAsync(IgniteInternalTx tx) {
        MvccSnapshotFuture fut = new MvccSnapshotFuture();

        requestSnapshotAsync(tx, fut);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void requestSnapshotAsync(MvccSnapshotResponseListener lsnr) {
        requestSnapshotAsync(null, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void requestSnapshotAsync(IgniteInternalTx tx, MvccSnapshotResponseListener lsnr) {
        MvccCoordinator crd = currentCoordinator();

        if (crd == null) {
            lsnr.onError(noCoordinatorError());

            return;
        }

        if (tx != null) {
            AffinityTopologyVersion topVer = ctx.cache().context().lockedTopologyVersion(null);

            if (topVer != null && topVer.compareTo(crd.topologyVersion()) < 0) {
                lsnr.onError(new ClusterTopologyCheckedException("Mvcc coordinator is outdated " +
                    "for the locked topology version. [crd=" + crd + ", tx=" + tx + ']'));

                return;
            }
        }

        if (ctx.localNodeId().equals(crd.nodeId())) {
            if (!initFut.isDone()) {
                // Wait for the local coordinator init.
                initFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture fut) {
                        requestSnapshotAsync(tx, lsnr);
                    }
                });
            }
            else if (tx != null)
                lsnr.onResponse(assignTxSnapshot(0L, ctx.localNodeId(), false));
            else
                lsnr.onResponse(activeQueries.assignQueryCounter(ctx.localNodeId(), 0L));

            return;
        }

        // Send request to the remote coordinator.
        UUID nodeId = crd.nodeId();

        long id = futIdCntr.incrementAndGet();

        Map<Long, MvccSnapshotResponseListener> map = snapLsnrs.get(nodeId), map0;

        if (map == null && (map0 = snapLsnrs.putIfAbsent(nodeId, map = new ConcurrentHashMap<>())) != null)
            map = map0;

        map.put(id, lsnr);

        try {
            sendMessage(nodeId, tx != null ? new MvccTxSnapshotRequest(id) : new MvccQuerySnapshotRequest(id));
        }
        catch (IgniteCheckedException e) {
            if (map.remove(id) != null)
                lsnr.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> ackTxCommit(MvccSnapshot updateVer) {
        return ackTxCommit(updateVer, null, 0L);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> ackTxCommit(MvccVersion updateVer, MvccSnapshot readSnapshot,
        long qryId) {
        assert updateVer != null;

        MvccCoordinator crd = curCrd;

        if (updateVer.coordinatorVersion() == crd.coordinatorVersion())
            return sendTxCommit(crd, createTxAckMessage(futIdCntr.incrementAndGet(), updateVer, readSnapshot, qryId));
        else if (readSnapshot != null)
            ackQueryDone(readSnapshot, qryId);

        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Override public void ackTxRollback(MvccVersion updateVer) {
        assert updateVer != null;

        MvccCoordinator crd = curCrd;

        if (crd.coordinatorVersion() != updateVer.coordinatorVersion())
            return;

        MvccAckRequestTx msg = createTxAckMessage(-1, updateVer, null, 0L);

        msg.skipResponse(true);

        try {
            sendMessage(crd.nodeId(), msg);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx rollback ack, node left [msg=" + msg + ", node=" + crd.nodeId() + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx rollback ack [msg=" + msg + ", node=" + crd.nodeId() + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void ackTxRollback(MvccVersion updateVer, MvccSnapshot readSnapshot, long qryTrackerId) {
        assert updateVer != null;

        MvccCoordinator crd = curCrd;

        if (crd.coordinatorVersion() != updateVer.coordinatorVersion()) {
            if (readSnapshot != null)
                ackQueryDone(readSnapshot, qryTrackerId);

            return;
        }

        MvccAckRequestTx msg = createTxAckMessage(-1, updateVer, readSnapshot, qryTrackerId);

        msg.skipResponse(true);

        try {
            sendMessage(crd.nodeId(), msg);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx rollback ack, node left [msg=" + msg + ", node=" + crd.nodeId() + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx rollback ack [msg=" + msg + ", node=" + crd.nodeId() + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void ackQueryDone(MvccSnapshot snapshot, long qryId) {
        assert snapshot != null;

        MvccCoordinator crd = currentCoordinator();

        if (crd == null || crd.coordinatorVersion() == snapshot.coordinatorVersion()
            && sendQueryDone(crd, new MvccAckRequestQueryCntr(queryTrackCounter(snapshot))))
            return;

        Message msg = new MvccAckRequestQueryId(qryId);

        do {
            crd = currentCoordinator();
        }
        while (!sendQueryDone(crd, msg));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> waitTxsFuture(UUID crdId, GridLongList txs) {
        assert crdId != null;
        assert txs != null && !txs.isEmpty();

        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crdId, false);

        ackFuts.put(fut.id, fut);

        try {
            sendMessage(crdId, new MvccWaitTxsRequest(fut.id, txs));
        }
        catch (IgniteCheckedException e) {
            if (ackFuts.remove(fut.id) != null) {
                if (e instanceof ClusterTopologyCheckedException)
                    fut.onDone(); // No need to wait, new coordinator will be assigned, finish without error.
                else
                    fut.onDone(e);
            }
        }

        return fut;
    }

    /** {@inheritDoc} */
    // TODO: Proper use of diagnostic context.
    @Override public void dumpDebugInfo(IgniteLogger log, @Nullable IgniteDiagnosticPrepareContext diagCtx) {
        boolean first = true;

        for (Map<Long, MvccSnapshotResponseListener> map : snapLsnrs.values()) {
            if (first) {
                U.warn(log, "Pending mvcc listener: ");

                first = false;
            }

            for (MvccSnapshotResponseListener lsnr : map.values())
                U.warn(log, ">>> " + lsnr.toString());
        }

        first = true;

        for (WaitAckFuture waitAckFut : ackFuts.values()) {
            if (first) {
                U.warn(log, "Pending mvcc wait ack futures: ");

                first = false;
            }

            U.warn(log, ">>> " + waitAckFut.toString());
        }
    }

    /**
     * Removes all less or equals to the given one records from Tx log.
     *
     * @param ver Version.
     * @throws IgniteCheckedException If fails.
     */
    void removeUntil(MvccVersion ver) throws IgniteCheckedException {
        txLog.removeUntil(ver.coordinatorVersion(), ver.counter());
    }

    /**
     * TODO IGNITE-7966
     *
     * @return Data region configuration.
     */
    private DataRegionConfiguration createTxLogRegion(DataStorageConfiguration dscfg) {
        DataRegionConfiguration cfg = new DataRegionConfiguration();

        cfg.setName(TX_LOG_CACHE_NAME);
        cfg.setInitialSize(dscfg.getSystemRegionInitialSize());
        cfg.setMaxSize(dscfg.getSystemRegionMaxSize());
        cfg.setPersistenceEnabled(CU.isPersistenceEnabled(dscfg));
        return cfg;
    }

    /**
     * @return Data storage configuration.
     */
    private DataStorageConfiguration dataStorageConfiguration() {
        return ctx.config().getDataStorageConfiguration();
    }

    /**
     * Picks mvcc coordinator from the given list of nodes.
     *
     * @param nodes List of nodes.
     * @param topVer Topology version.
     * @return Chosen mvcc coordinator.
     */
    private MvccCoordinator pickMvccCoordinator(Collection<ClusterNode> nodes, long topVer) {
        checkMvccSupported(nodes);

        ClusterNode crdNode = null;

        if (crdC != null) {
            crdNode = crdC.apply(nodes);

            if (crdNode != null && log.isInfoEnabled())
                log.info("Assigned coordinator using test closure: " + crdNode.id());
        }
        else {
            // Expect nodes are sorted by order.
            for (ClusterNode node : nodes) {
                if (!node.isClient() && supportsMvcc(node)) {
                    crdNode = node;

                    break;
                }
            }
        }

        MvccCoordinator crd = crdNode != null ? new MvccCoordinator(crdNode.id(), coordinatorVersion(crdNode),
            new AffinityTopologyVersion(topVer, 0)) : null;

        if (log.isInfoEnabled() && crd != null)
            log.info("Assigned mvcc coordinator [crd=" + crd + ", crdNode=" + crdNode + ']');
        else if (crd == null)
            U.warn(log, "New mvcc coordinator was not assigned [topVer=" + topVer + ']');

        return crd;
    }


    /**
     * @param crdNode Assigned coordinator node.
     * @return Coordinator version.
     */
    private long coordinatorVersion(ClusterNode crdNode) {
        return crdNode.order() + ctx.discovery().gridStartTime();
    }

    /** */
    private void checkMvccSupported(Collection<ClusterNode> nodes) {
        if (mvccEnabled) {
            assert mvccSupported;

            return;
        }

        boolean res = true, was = mvccSupported;

        for (ClusterNode node : nodes) {
            if (!supportsMvcc(node)) {
                res = false;

                break;
            }
        }

        if (was != res)
            mvccSupported = res;
    }

    /** */
    private boolean supportsMvcc(ClusterNode node) {
        return node.version().compareToIgnoreTimestamp(MVCC_SUPPORTED_SINCE) >= 0;
    }

    /** */
    private void checkMvccCacheStarted(DynamicCacheChangeBatch cacheMsg) {
        if (!mvccEnabled) {
            for (DynamicCacheChangeRequest req : cacheMsg.requests()) {
                CacheConfiguration ccfg = req.startCacheConfiguration();

                if (ccfg == null)
                    continue;

                if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
                    assert mvccSupported;

                    mvccEnabled = true;
                }
            }
        }
    }

    /** */
    private MvccSnapshotResponse assignTxSnapshot(long futId, UUID nearId, boolean client) {
        assert initFut.isDone();
        assert crdVer != 0;
        assert ctx.localNodeId().equals(currentCoordinatorId());

        MvccSnapshotResponse res = new MvccSnapshotResponse();

        long ver, cleanup, tracking;

        synchronized (this) {
            ver = mvccCntr.incrementAndGet();
            tracking = ver;
            cleanup = committedCntr.get() + 1;

            for (Map.Entry<Long, ActiveTx> entry : activeTxs.entrySet()) {
                cleanup = Math.min(entry.getValue().tracking, cleanup);
                tracking = Math.min(entry.getKey(), tracking);

                res.addTx(entry.getKey());
            }

            ActiveTx activeTx = client ? new ActiveTx(tracking, nearId) : new ActiveServerTx(tracking, nearId);

            boolean add = activeTxs.put(ver, activeTx) == null;

            assert add : ver;
        }

        long minQry = activeQueries.minimalQueryCounter();

        if (minQry != -1)
            cleanup = Math.min(cleanup, minQry);

        cleanup = prevCrdQueries.previousQueriesDone() ? cleanup - 1 : MVCC_COUNTER_NA;

        res.init(futId, crdVer, ver, MVCC_START_OP_CNTR, cleanup, tracking);

        return res;
    }

    /** */
    private void onTxDone(Long txCntr, boolean increaseCommittedCntr) {
        assert initFut.isDone();

        GridFutureAdapter fut;

        synchronized (this) {
            activeTxs.remove(txCntr);

            if (increaseCommittedCntr)
                committedCntr.setIfGreater(txCntr);
        }

        fut = waitTxFuts.remove(txCntr);

        if (fut != null)
            fut.onDone();
    }

    /**
     * @param mvccCntr Query counter.
     */
    private void onQueryDone(UUID nodeId, Long mvccCntr) {
        activeQueries.onQueryDone(nodeId, mvccCntr);
    }

    /**
     * @param futId Future ID.
     * @param updateVer Update version.
     * @param readSnapshot Optional read version.
     * @param qryTrackerId Query tracker id.
     * @return Message.
     */
    private MvccAckRequestTx createTxAckMessage(long futId, MvccVersion updateVer, MvccSnapshot readSnapshot,
        long qryTrackerId) {
        if (readSnapshot == null)
            return new MvccAckRequestTx(futId, updateVer.counter());
        else if (readSnapshot.coordinatorVersion() == updateVer.coordinatorVersion())
            return new MvccAckRequestTxAndQueryCntr(futId, updateVer.counter(), queryTrackCounter(readSnapshot));
        else
            return new MvccAckRequestTxAndQueryId(futId, updateVer.counter(), qryTrackerId);
    }

    /**
     * @param mvccVer Read version.
     * @return Tracker counter.
     */
    private long queryTrackCounter(MvccSnapshot mvccVer) {
        long trackCntr = mvccVer.counter();

        MvccLongList txs = mvccVer.activeTransactions();

        int size = txs.size();

        for (int i = 0; i < size; i++) {
            long txVer = txs.get(i);

            if (txVer < trackCntr)
                trackCntr = txVer;
        }

        return trackCntr;
    }

    /**
     * Launches vacuum workers and scheduler.
     */
    void startVacuumWorkers() {
        if (!ctx.clientNode()) {
            synchronized (mux) {
                if (vacuumWorkers == null) {
                    assert cleanupQueue == null;

                    cleanupQueue = new LinkedBlockingQueue<>();

                    vacuumWorkers = new ArrayList<>(ctx.config().getMvccVacuumThreadCount() + 1);

                    vacuumWorkers.add(new VacuumScheduler(ctx, log, this));

                    for (int i = 0; i < ctx.config().getMvccVacuumThreadCount(); i++) {
                        vacuumWorkers.add(new VacuumWorker(ctx, log, cleanupQueue));
                    }

                    for (GridWorker worker : vacuumWorkers) {
                        new IgniteThread(worker).start();
                    }

                    return;
                }
            }

            U.warn(log, "Attempting to start active vacuum.");
        }
    }

    /**
     * Stops vacuum worker and scheduler.
     */
    void stopVacuumWorkers() {
        if (!ctx.clientNode()) {
            List<GridWorker> workers;
            BlockingQueue<VacuumTask> queue;

            synchronized (mux) {
                workers = vacuumWorkers;
                queue = cleanupQueue;

                vacuumWorkers = null;
                cleanupQueue = null;
            }

            if (workers == null) {
                if (log.isDebugEnabled() && mvccEnabled())
                    log.debug("Attempting to stop inactive vacuum.");

                return;
            }

            assert queue != null;

            // Stop vacuum workers outside mutex to prevent deadlocks.
            U.cancel(workers);
            U.join(workers, log);

            if (!queue.isEmpty()) {
                IgniteCheckedException ex = vacuumCancelledException();

                for (VacuumTask task : queue) {
                    task.onDone(ex);
                }
            }
        }
    }

    /**
     * Runs vacuum process.
     *
     * @return {@code Future} with {@link VacuumMetrics}.
     */
    IgniteInternalFuture<VacuumMetrics> runVacuum() {
        assert !ctx.clientNode();

        MvccCoordinator crd0 = currentCoordinator();

        if (Thread.currentThread().isInterrupted() ||
            crd0 == null ||
            crdVer == 0 && ctx.localNodeId().equals(crd0.nodeId()))
            return new GridFinishedFuture<>(new VacuumMetrics());

        final GridFutureAdapter<VacuumMetrics> res = new GridFutureAdapter<>();

        MvccSnapshot snapshot;

        try {
            // TODO IGNITE-8974 create special method for getting cleanup version only.
            snapshot = tryRequestSnapshotLocal(DUMMY_TX);
        }
        catch (ClusterTopologyCheckedException e) {
            throw new AssertionError(e);
        }

        if (snapshot != null)
            continueRunVacuum(res, snapshot);
        else
            requestSnapshotAsync(DUMMY_TX, new MvccSnapshotResponseListener() {
                @Override public void onResponse(MvccSnapshot s) {
                    continueRunVacuum(res, s);
                }

                @Override public void onError(IgniteCheckedException e) {
                    if (!(e instanceof ClusterTopologyCheckedException))
                        completeWithException(res, e);
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Vacuum failed to receive an Mvcc snapshot. " +
                                "Need to retry on the stable topology. " + e.getMessage());

                        res.onDone(new VacuumMetrics());
                    }
                }
            });

        return res;
    }

    /**
     * @param res Result.
     * @param snapshot Snapshot.
     */
    private void continueRunVacuum(GridFutureAdapter<VacuumMetrics> res, MvccSnapshot snapshot) {
        ackTxCommit(snapshot)
            .listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    Throwable err;

                    if ((err = fut.error()) != null) {
                        U.error(log, "Vacuum error.", err);

                        res.onDone(err);
                    }
                    else if (snapshot.cleanupVersion() <= MVCC_COUNTER_NA)
                        res.onDone(new VacuumMetrics());
                    else {
                        try {
                            if (log.isDebugEnabled())
                                log.debug("Started vacuum with cleanup version=" + snapshot.cleanupVersion() + '.');

                            synchronized (mux) {
                                if (cleanupQueue == null) {
                                    res.onDone(vacuumCancelledException());

                                    return;
                                }

                                GridCompoundIdentityFuture<VacuumMetrics> res0 =
                                    new GridCompoundIdentityFuture<VacuumMetrics>(new VacuumMetricsReducer()) {
                                        /** {@inheritDoc} */
                                        @Override protected void logError(IgniteLogger log, String msg, Throwable e) {
                                            // no-op
                                        }

                                        /** {@inheritDoc} */
                                        @Override protected void logDebug(IgniteLogger log, String msg) {
                                            // no-op
                                        }
                                    };

                                for (CacheGroupContext grp : ctx.cache().cacheGroups()) {
                                    if (grp.mvccEnabled()) {
                                        grp.topology().readLock();

                                        try {
                                            for (GridDhtLocalPartition part : grp.topology().localPartitions()) {
                                                VacuumTask task = new VacuumTask(snapshot, part);

                                                cleanupQueue.offer(task);

                                                res0.add(task);
                                            }
                                        }
                                        finally {
                                            grp.topology().readUnlock();
                                        }
                                    }
                                }

                                res0.markInitialized();

                                res0.listen(future -> {
                                    VacuumMetrics metrics = null; Throwable ex = null;

                                    try {
                                        metrics = future.get();

                                        if (U.assertionsEnabled()) {
                                            MvccCoordinator crd = currentCoordinator();

                                            assert crd != null
                                                && crd.coordinatorVersion() >= snapshot.coordinatorVersion();

                                            for (TxKey key : waitMap.keySet()) {
                                                if (!( key.major() == snapshot.coordinatorVersion()
                                                    && key.minor() > snapshot.cleanupVersion()
                                                    || key.major() > snapshot.coordinatorVersion())) {
                                                    byte state = state(key.major(), key.minor());

                                                    assert state == TxState.ABORTED : "tx state=" + state;
                                                }
                                            }
                                        }

                                        txLog.removeUntil(snapshot.coordinatorVersion(), snapshot.cleanupVersion());

                                        if (log.isDebugEnabled())
                                            log.debug("Vacuum completed. " + metrics);
                                    } catch (Throwable e) {
                                        if (X.hasCause(e, NodeStoppingException.class)) {
                                            if (log.isDebugEnabled())
                                                log.debug("Cannot complete vacuum (node is stopping).");

                                            metrics = new VacuumMetrics();
                                        } else
                                            ex = new GridClosureException(e);
                                    }

                                    res.onDone(metrics, ex);
                                });
                            }

                        }
                        catch (Throwable e) {
                            completeWithException(res, e);
                        }
                    }
                }
            });
    }

    /** */
    private void completeWithException(GridFutureAdapter fut, Throwable e) {
        fut.onDone(e);

        if (e instanceof Error)
            throw (Error)e;
    }

    /** */
    @NotNull private IgniteCheckedException vacuumCancelledException() {
        return new NodeStoppingException("Operation has been cancelled (node is stopping).");
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void sendFutureResponse(UUID nodeId, MvccWaitTxsRequest msg) {
        try {
            sendMessage(nodeId, new MvccFutureResponse(msg.futureId()));
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx ack response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx ack response [msg=" + msg + ", node=" + nodeId + ']', e);
        }
    }

    /** */
    @NotNull private IgniteInternalFuture<Void> sendTxCommit(MvccCoordinator crd, MvccAckRequestTx msg) {
        WaitAckFuture fut = new WaitAckFuture(msg.futureId(), crd.nodeId(), true);

        ackFuts.put(fut.id, fut);

        try {
            sendMessage(crd.nodeId(), msg);
        }
        catch (IgniteCheckedException e) {
            if (ackFuts.remove(fut.id) != null) {
                if (e instanceof ClusterTopologyCheckedException) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send tx ack, node left [crd=" + crd + ", msg=" + msg + ']');

                    fut.onDone(); // No need to ack, finish without error.
                }
                else
                    fut.onDone(e);
            }
        }

        return fut;
    }

    /**
     * @param crd Mvcc coordinator.
     * @param msg Message.
     * @return {@code True} if no need to resend the message to a new coordinator.
     */
    private boolean sendQueryDone(MvccCoordinator crd, Message msg) {
        if (crd == null)
            return true; // no need to send ack;

        try {
            sendMessage(crd.nodeId(), msg);

            return true;
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query ack, node left [crd=" + crd + ", msg=" + msg + ']');

            MvccCoordinator crd0 = currentCoordinator();

            // Coordinator is unassigned or still the same.
            return crd0 == null || crd.coordinatorVersion() == crd0.coordinatorVersion();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query ack [crd=" + crd + ", msg=" + msg + ']', e);

            return true;
        }
    }

    /**
     * Send IO message.
     *
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void sendMessage(UUID nodeId, Message msg) throws IgniteCheckedException {
        ctx.io().sendToGridTopic(nodeId, TOPIC_CACHE_COORDINATOR, msg, SYSTEM_POOL);
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxSnapshotRequest(UUID nodeId, MvccTxSnapshotRequest msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore tx snapshot request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccSnapshotResponse res = assignTxSnapshot(msg.futureId(), nodeId, node.isClient());

        boolean finishFailed = true;

        try {
            sendMessage(node.id(), res);

            finishFailed = false;
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx snapshot response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx snapshot response [msg=" + msg + ", node=" + nodeId + ']', e);
        }

        if (finishFailed)
            onTxDone(res.counter(), false);
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorQuerySnapshotRequest(UUID nodeId, MvccQuerySnapshotRequest msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore query counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccSnapshotResponse res = activeQueries.assignQueryCounter(nodeId, msg.futureId());

        try {
            sendMessage(node.id(), res);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query counter response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            onQueryDone(nodeId, res.tracking());

            U.error(log, "Failed to send query counter response [msg=" + msg + ", node=" + nodeId + ']', e);
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorSnapshotResponse(UUID nodeId, MvccSnapshotResponse msg) {
        Map<Long, MvccSnapshotResponseListener> map = snapLsnrs.get(nodeId);

        MvccSnapshotResponseListener lsnr;

        if (map != null && (lsnr = map.remove(msg.futureId())) != null)
            lsnr.onResponse(msg);
        else {
            if (ctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processCoordinatorQueryAckRequest(UUID nodeId, MvccAckRequestQueryCntr msg) {
        onQueryDone(nodeId, msg.counter());
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processNewCoordinatorQueryAckRequest(UUID nodeId, MvccAckRequestQueryId msg) {
        prevCrdQueries.onQueryDone(nodeId, msg.queryTrackerId());
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxAckRequest(UUID nodeId, MvccAckRequestTx msg) {
        onTxDone(msg.txCounter(), msg.futureId() >= 0);

        if (msg.queryCounter() != MVCC_COUNTER_NA)
            onQueryDone(nodeId, msg.queryCounter());
        else if (msg.queryTrackerId() != MVCC_TRACKER_ID_NA)
            prevCrdQueries.onQueryDone(nodeId, msg.queryTrackerId());

        if (!msg.skipResponse()) {
            try {
                sendMessage(nodeId, new MvccFutureResponse(msg.futureId()));
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send tx ack response, node left [msg=" + msg + ", node=" + nodeId + ']');
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send tx ack response [msg=" + msg + ", node=" + nodeId + ']', e);
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorAckResponse(UUID nodeId, MvccFutureResponse msg) {
        WaitAckFuture fut = ackFuts.remove(msg.futureId());

        if (fut != null)
            fut.onResponse();
        else {
            if (ctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    @SuppressWarnings("unchecked")
    private void processCoordinatorWaitTxsRequest(final UUID nodeId, final MvccWaitTxsRequest msg) {
        GridLongList txs = msg.transactions();

        GridCompoundFuture resFut = null;

        for (int i = 0; i < txs.size(); i++) {
            Long txId = txs.get(i);

            GridFutureAdapter fut = waitTxFuts.get(txId);

            if (fut == null) {
                GridFutureAdapter old = waitTxFuts.putIfAbsent(txId, fut = new GridFutureAdapter());

                if (old != null)
                    fut = old;
            }

            boolean isDone;

            synchronized (this) {
                isDone = !activeTxs.containsKey(txId);
            }

            if (isDone)
                fut.onDone();

            if (!fut.isDone()) {
                if (resFut == null)
                    resFut = new GridCompoundFuture();

                resFut.add(fut);
            }
        }

        if (resFut != null)
            resFut.markInitialized();

        if (resFut == null || resFut.isDone())
            sendFutureResponse(nodeId, msg);
        else {
            resFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    sendFutureResponse(nodeId, msg);
                }
            });
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processCoordinatorActiveQueriesMessage(UUID nodeId, MvccActiveQueriesMessage msg) {
        prevCrdQueries.addNodeActiveQueries(nodeId, msg.activeQueries());
    }

    /**
     *
     */
    private class ActiveQueries {
        /** */
        private final Map<UUID, TreeMap<Long, AtomicInteger>> activeQueries = new HashMap<>();

        /** */
        private Long minQry;

        /** */
        private synchronized long minimalQueryCounter() {
            return minQry == null ? -1 : minQry;
        }

        /** */
        private synchronized MvccSnapshotResponse assignQueryCounter(UUID nodeId, long futId) {
            MvccSnapshotResponse res = new MvccSnapshotResponse();

            long ver, tracking;

            synchronized (MvccProcessorImpl.this) {
                ver = committedCntr.get();
                tracking = ver;

                for (Long txVer : activeTxs.keySet()) {
                    if (txVer < ver) {
                        tracking = Math.min(txVer, tracking);
                        res.addTx(txVer);
                    }
                }
            }

            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null) {
                activeQueries.put(nodeId, nodeMap = new TreeMap<>());

                nodeMap.put(tracking, new AtomicInteger(1));
            }
            else {
                AtomicInteger cntr = nodeMap.get(tracking);

                if (cntr == null)
                    nodeMap.put(tracking, new AtomicInteger(1));
                else
                    cntr.incrementAndGet();
            }

            if (minQry == null)
                minQry = tracking;

            res.init(futId, crdVer, ver, MVCC_READ_OP_CNTR, MVCC_COUNTER_NA, tracking);

            return res;
        }

        /** */
        private synchronized void onQueryDone(UUID nodeId, Long ver) {
            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null)
                return;

            assert minQry != null;

            AtomicInteger cntr = nodeMap.get(ver);

            assert cntr != null && cntr.get() > 0 : "onQueryDone ver=" + ver;

            if (cntr.decrementAndGet() == 0) {
                nodeMap.remove(ver);

                if (nodeMap.isEmpty())
                    activeQueries.remove(nodeId);

                if (ver.equals(minQry))
                    minQry = activeMinimal();
            }
        }

        /** */
        private synchronized void onNodeFailed(UUID nodeId) {
            activeQueries.remove(nodeId);

            minQry = activeMinimal();
        }

        /** */
        private Long activeMinimal() {
            Long min = null;

            for (TreeMap<Long, AtomicInteger> s : activeQueries.values()) {
                Long first = s.firstKey();

                if (min == null || first < min)
                    min = first;
            }

            return min;
        }
    }

    /**
     *
     */
    private class WaitAckFuture extends MvccFuture<Void> {
        /** */
        private final long id;

        /** */
        final boolean ackTx;

        /**
         * @param id Future ID.
         * @param nodeId Coordinator node ID.
         * @param ackTx {@code True} if ack tx commit, {@code false} if waits for previous txs.
         */
        WaitAckFuture(long id, UUID nodeId, boolean ackTx) {
            super(nodeId);

            this.id = id;
            this.ackTx = ackTx;
        }

        /**
         *
         */
        void onResponse() {
            onDone();
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crdId.equals(nodeId) && ackFuts.remove(id) != null)
                onDone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WaitAckFuture.class, this, super.toString());
        }
    }

    @Override public IgniteInternalFuture<NearTxLocator> checkWaiting(UUID nodeId, MvccVersion mvccVer) {
        // t0d0 employ local check without futures and messagses
        LockWaitCheckFuture fut = new LockWaitCheckFuture(nodeId, mvccVer, ctx.cache().context());
        fut.init();
        return fut;
    }

    public class LockWaitCheckFuture extends GridCacheFutureAdapter<NearTxLocator> {
        private final UUID nodeId;
        private final IgniteUuid futId = IgniteUuid.randomUuid();
        private final MvccVersionImpl txVersion;
        private final GridCacheSharedContext<?, ?> cctx;

        private LockWaitCheckFuture(UUID nodeId, MvccVersion txVersion, GridCacheSharedContext<?, ?> cctx) {
            this.nodeId = nodeId;
            this.txVersion = new MvccVersionImpl(txVersion.coordinatorVersion(), txVersion.counter(), txVersion.operationCounter());
            this.cctx = cctx;
        }

        public void init() {
            try {
                cctx.mvcc().addFuture(this, futId);
                sendMessage(nodeId, new LockWaitCheckRequest(futId, txVersion));
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                e.printStackTrace();
            }
        }

        @Override public boolean onDone(@Nullable NearTxLocator res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                cctx.mvcc().removeFuture(futId);

                return true;
            }

            return false;
        }

        public void onResponse(LockWaitCheckResponse res) {
            onDone(res.isWaiting() ? new NearTxLocator(res.blockerNodeId(), res.blockerTxVersion()) : null);
        }

        @Override public IgniteUuid futureId() {
            return futId;
        }

        @Override public boolean onNodeLeft(UUID nodeId) {
            onDone(new ClusterTopologyCheckedException("Node left grid (will ignore)."));

            return true;
        }

        @Override public boolean trackable() {
            return true;
        }

        @Override public void markNotTrackable() {
        }
    }

    /**
     *
     */
    private class CoordinatorMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            // t0d0 setup message handler in proper place
            if (msg instanceof DeadlockProbe) {
                new DdCollaborator(ctx.cache().context()).handleDeadlockProbe((DeadlockProbe)msg);
                return;
            }

            if (msg instanceof LockWaitCheckRequest) {
                handleLockCheckRequest(nodeId, (LockWaitCheckRequest)msg);
                return;
            }

            if (msg instanceof LockWaitCheckResponse) {
                LockWaitCheckResponse checkRes = (LockWaitCheckResponse)msg;

                LockWaitCheckFuture fut = (LockWaitCheckFuture)ctx.cache().context().mvcc().future(checkRes.futId());
                if (fut == null) {
                    // t0d0 warning
                }
                else
                    fut.onResponse(checkRes);

                return;
            }

            MvccMessage msg0 = (MvccMessage)msg;

            if (msg0.waitForCoordinatorInit() && !initFut.isDone()) {
                initFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                    @Override public void apply(IgniteInternalFuture<Void> future) {
                        assert crdVer != 0L;

                        processMessage(nodeId, msg);
                    }
                });
            }
            else
                processMessage(nodeId, msg);
        }

        /**
         * Processes mvcc message.
         *
         * @param nodeId Node id.
         * @param msg Message.
         */
        private void processMessage(UUID nodeId, Object msg) {
            if (msg instanceof MvccTxSnapshotRequest)
                processCoordinatorTxSnapshotRequest(nodeId, (MvccTxSnapshotRequest)msg);
            else if (msg instanceof MvccAckRequestTx)
                processCoordinatorTxAckRequest(nodeId, (MvccAckRequestTx)msg);
            else if (msg instanceof MvccFutureResponse)
                processCoordinatorAckResponse(nodeId, (MvccFutureResponse)msg);
            else if (msg instanceof MvccAckRequestQueryCntr)
                processCoordinatorQueryAckRequest(nodeId, (MvccAckRequestQueryCntr)msg);
            else if (msg instanceof MvccQuerySnapshotRequest)
                processCoordinatorQuerySnapshotRequest(nodeId, (MvccQuerySnapshotRequest)msg);
            else if (msg instanceof MvccSnapshotResponse)
                processCoordinatorSnapshotResponse(nodeId, (MvccSnapshotResponse)msg);
            else if (msg instanceof MvccWaitTxsRequest)
                processCoordinatorWaitTxsRequest(nodeId, (MvccWaitTxsRequest)msg);
            else if (msg instanceof MvccAckRequestQueryId)
                processNewCoordinatorQueryAckRequest(nodeId, (MvccAckRequestQueryId)msg);
            else if (msg instanceof MvccActiveQueriesMessage)
                processCoordinatorActiveQueriesMessage(nodeId, (MvccActiveQueriesMessage)msg);
            else if (msg instanceof MvccRecoveryFinishedMessage)
                processRecoveryFinishedMessage(nodeId, ((MvccRecoveryFinishedMessage)msg));
            else
                U.warn(log, "Unexpected message received [node=" + nodeId + ", msg=" + msg + ']');
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CoordinatorMessageListener[]";
        }
    }

    private void handleLockCheckRequest(UUID nodeId, LockWaitCheckRequest req) {
        LockWaitCheckResponse res = findBlockerTx(req.txVersion())
            .map(tx -> LockWaitCheckResponse.waiting(req.futId(), tx.eventNodeId(), tx.nearXidVersion()))
            .orElseGet(() -> LockWaitCheckResponse.notWaiting(req.futId()));

        try {
            sendMessage(nodeId, res);
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }

    private Optional<IgniteInternalTx> findBlockerTx(MvccVersion checkedTxVer) {
        // t0d0 multiple blocker txs seems to be possible
        return waitMap.entrySet().stream()
            .filter(e -> e.getValue().waitQueue().stream()
                .anyMatch(waitingVer -> DdCollaborator.belongToSameTx(waitingVer, checkedTxVer)))
            .map(e -> e.getKey())
            .findAny()
            .flatMap(txKey -> ctx.cache().context().tm().activeTransactions().stream()
                .filter(tx -> tx.mvccSnapshot().coordinatorVersion() == txKey.major() && tx.mvccSnapshot().counter() == txKey.minor())
                .findAny());
    }

    /**
     * Accumulates transaction recovery votes for a node left the cluster.
     * Transactions started by the left node are considered not active
     * when each cluster server node aknowledges that is has finished transactions for the left node.
     */
    private static class RecoveryBallotBox {
        /** */
        private List<UUID> voters;
        /** */
        private final Set<UUID> ballots = new HashSet<>();

        /**
         * @param voters Nodes which can have transaction started by the left node.
         */
        private synchronized void voters(List<UUID> voters) {
            this.voters = voters;
        }

        /**
         * @param nodeId Voting node id.
         *
         */
        private synchronized void vote(UUID nodeId) {
            ballots.add(nodeId);
        }

        /**
         * @return {@code True} if all nodes expected to vote done it.
         */
        private synchronized boolean isVotingDone() {
            if (voters == null)
                return false;

            return ballots.containsAll(voters);
        }
    }

    /**
     * Process message that one node has finished with transactions for the left node.
     * @param nodeId Node sent the message.
     * @param msg Message.
     */
    private void processRecoveryFinishedMessage(UUID nodeId, MvccRecoveryFinishedMessage msg) {
        UUID nearNodeId = msg.nearNodeId();

        RecoveryBallotBox ballotBox = recoveryBallotBoxes.computeIfAbsent(nearNodeId, uuid -> new RecoveryBallotBox());

        ballotBox.vote(nodeId);

        tryFinishRecoveryVoting(nearNodeId, ballotBox);
    }

    /**
     * Finishes recovery on coordinator by removing transactions started by the left node
     * @param nearNodeId Left node.
     * @param ballotBox Votes accumulator for the left node.
     */
    private void tryFinishRecoveryVoting(UUID nearNodeId, RecoveryBallotBox ballotBox) {
        if (ballotBox.isVotingDone()) {
            List<Long> recoveredTxs;

            synchronized (this) {
                recoveredTxs = activeTxs.entrySet().stream()
                    .filter(e -> e.getValue().nearNodeId.equals(nearNodeId))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            }

            // Committed counter is increased because it is not known if transaction was committed or not and we must
            // bump committed counter for committed transaction as it is used in (read-only) query snapshot.
            recoveredTxs.forEach(txCntr -> onTxDone(txCntr, true));

            recoveryBallotBoxes.remove(nearNodeId);
        }
    }

    /** */
    private interface Waiter {
        /**
         * @param ctx Grid kernal context.
         */
        void run(GridKernalContext ctx);

        /**
         * @param other Another waiter.
         * @return New compound waiter.
         */
        Waiter concat(Waiter other);

        /**
         * @return {@code True} if there is an active local transaction
         */
        boolean hasLocalTransaction();

        /**
         * @return {@code True} if it is a compound waiter.
         */
        boolean compound();

        // t0d0 develop a good way for checking waiting transactions
        Set<MvccVersion> waitQueue();
    }

    /** */
    private static class LockFuture extends GridFutureAdapter<Void> implements Waiter, Runnable {
        /** */
        private final byte plc;
        private final MvccVersion blockedTxVer;

        /**
         * @param plc Pool policy.
         * @param blockedTxVer t0d0
         */
        LockFuture(byte plc, MvccVersion blockedTxVer) {
            this.plc = plc;
            this.blockedTxVer = blockedTxVer;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            onDone();
        }

        /** {@inheritDoc} */
        @Override public void run(GridKernalContext ctx) {
            try {
                ctx.pools().poolForPolicy(plc).execute(this);
            }
            catch (IgniteCheckedException e) {
                U.error(ctx.log(LockFuture.class), e);
            }
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiterNoLocal(this, other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean compound() {
            return false;
        }

        @Override public Set<MvccVersion> waitQueue() {
            return Collections.singleton(blockedTxVer);
        }
    }

    /** */
    private static class LocalTransactionMarker implements Waiter {
        /** {@inheritDoc} */
        @Override public void run(GridKernalContext ctx) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiter(other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean compound() {
            return false;
        }

        @Override public Set<MvccVersion> waitQueue() {
            return Collections.emptySet();
        }
    }

    /** */
    @SuppressWarnings("unchecked")
    private static class CompoundWaiter implements Waiter {
        /** */
        private final Object inner;

        /**
         * @param waiter Waiter to wrap.
         */
        private CompoundWaiter(Waiter waiter) {
            inner = waiter.compound() ? ((CompoundWaiter)waiter).inner : waiter;
        }

        /**
         * @param first First waiter.
         * @param second Second waiter.
         */
        private CompoundWaiter(Waiter first, Waiter second) {
            ArrayList<Waiter> list = new ArrayList<>();

            add(list, first);
            add(list, second);

            inner = list;
        }

        /** */
        private void add(List<Waiter> to, Waiter waiter) {
            if (!waiter.compound())
                to.add(waiter);
            else if (((CompoundWaiter)waiter).inner.getClass() == ArrayList.class)
                to.addAll((List<Waiter>)((CompoundWaiter)waiter).inner);
            else
                to.add((Waiter)((CompoundWaiter)waiter).inner);
        }

        /** {@inheritDoc} */
        @Override public void run(GridKernalContext ctx) {
            if (inner.getClass() == ArrayList.class) {
                for (Waiter waiter : (List<Waiter>)inner)
                    waiter.run(ctx);
            }
            else
                ((Waiter)inner).run(ctx);
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiter(this, other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean compound() {
            return true;
        }

        @Override public Set<MvccVersion> waitQueue() {
            if (inner.getClass() == ArrayList.class) {
                return ((List<Waiter>)inner).stream()
                    .map(Waiter::waitQueue)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            }
            else
                return ((Waiter)inner).waitQueue();
        }
    }

    /** */
    private static class CompoundWaiterNoLocal extends CompoundWaiter {
        /**
         * @param first First waiter.
         * @param second Second waiter.
         */
        private CompoundWaiterNoLocal(Waiter first, Waiter second) {
            super(first, second);
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiterNoLocal(this, other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return false;
        }
    }

    /**
     * Mvcc garbage collection scheduler.
     */
    private static class VacuumScheduler extends GridWorker {
        /** */
        private static final long VACUUM_TIMEOUT = 60_000;

        /** */
        private final long interval;

        /** */
        private final MvccProcessorImpl prc;

        /**
         * @param ctx Kernal context.
         * @param log Logger.
         * @param prc Mvcc processor.
         */
        VacuumScheduler(GridKernalContext ctx, IgniteLogger log, MvccProcessorImpl prc) {
            super(ctx.igniteInstanceName(), "vacuum-scheduler", log);

            this.interval = ctx.config().getMvccVacuumFrequency();
            this.prc = prc;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            U.sleep(interval); // initial delay

            while (!isCancelled()) {
                long nextScheduledTime = U.currentTimeMillis() + interval;

                try {
                    IgniteInternalFuture<VacuumMetrics> fut = prc.runVacuum();

                    if (log.isDebugEnabled())
                        log.debug("Vacuum started by scheduler.");

                    while (true) {
                        try {
                            fut.get(VACUUM_TIMEOUT);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException e) {
                            U.warn(log, "Failed to wait for vacuum complete. Consider increasing vacuum workers count.");
                        }
                    }
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw e; // Cancelled.
                }
                catch (Throwable e) {
                    if (e instanceof Error)
                        throw (Error) e;

                    U.error(log, "Vacuum error.", e);
                }

                long delay = nextScheduledTime - U.currentTimeMillis();

                if (delay > 0)
                    U.sleep(delay);
            }
        }
    }

    /**
     * Vacuum worker.
     */
    private static class VacuumWorker extends GridWorker {
        /** */
        private final BlockingQueue<VacuumTask> cleanupQueue;

        /**
         * @param ctx Kernal context.
         * @param log Logger.
         * @param cleanupQueue Cleanup tasks queue.
         */
        VacuumWorker(GridKernalContext ctx, IgniteLogger log, BlockingQueue<VacuumTask> cleanupQueue) {
            super(ctx.igniteInstanceName(), "vacuum-cleaner", log);

            this.cleanupQueue = cleanupQueue;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                VacuumTask task = cleanupQueue.take();

                try {
                    switch (task.part().state()) {
                        case EVICTED:
                        case RENTING:
                            task.onDone(new VacuumMetrics());

                            break;
                        case MOVING:
                            task.part().group().preloader().rebalanceFuture().listen(f -> cleanupQueue.add(task));

                            break;
                        case OWNING:
                            task.onDone(processPartition(task));

                            break;
                        case LOST:
                            task.onDone(new IgniteCheckedException("Partition is lost."));

                            break;
                    }
                }
                catch (IgniteInterruptedCheckedException e) {
                    task.onDone(e);

                    throw e; // Cancelled.
                }
                catch (Throwable e) {
                    task.onDone(e);

                    if (e instanceof Error)
                        throw (Error) e;
                }
            }
        }

        /**
         * Process partition.
         *
         * @param task VacuumTask.
         * @throws IgniteCheckedException If failed.
         */
        private VacuumMetrics processPartition(VacuumTask task) throws IgniteCheckedException {
            long startNanoTime = System.nanoTime();

            GridDhtLocalPartition part = task.part();

            VacuumMetrics metrics = new VacuumMetrics();

            if (!part.reserve())
                return metrics;

            int curCacheId = CU.UNDEFINED_CACHE_ID;

            try {
                GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(KEY_ONLY);

                KeyCacheObject prevKey = null;

                Object rest = null;

                List<MvccLinkAwareSearchRow> cleanupRows = null;

                MvccSnapshot snapshot = task.snapshot();

                GridCacheContext cctx = null;

                boolean shared = part.group().sharedGroup();

                if (!shared && (cctx = F.first(part.group().caches())) == null)
                    return metrics;

                while (cursor.next()) {
                    if (isCancelled())
                        throw new IgniteInterruptedCheckedException("Operation has been cancelled.");

                    MvccDataRow row = (MvccDataRow)cursor.get();

                    if (prevKey == null)
                        prevKey = row.key();

                    if (cctx == null) {
                        cctx = part.group().shared().cacheContext(curCacheId = row.cacheId());

                        if (cctx == null)
                            return metrics;
                    }

                    if (!prevKey.equals(row.key()) || (shared && curCacheId != row.cacheId())) {
                        if (rest != null || !F.isEmpty(cleanupRows))
                            cleanup(part, prevKey, cleanupRows, rest, cctx, metrics);

                        cleanupRows = null;

                        rest = null;

                        if (shared && curCacheId != row.cacheId()) {
                            cctx = part.group().shared().cacheContext(curCacheId = row.cacheId());

                            if (cctx == null)
                                return metrics;
                        }

                        prevKey = row.key();
                    }

                    if (canClean(row, snapshot, cctx))
                        cleanupRows = addRow(cleanupRows, row);
                    else if (actualize(cctx, row, snapshot))
                        rest = addRest(rest, row);

                    metrics.addScannedRowsCount(1);
                }

                if (rest != null || !F.isEmpty(cleanupRows))
                    cleanup(part, prevKey, cleanupRows, rest, cctx, metrics);

                metrics.addSearchNanoTime(System.nanoTime() - startNanoTime - metrics.cleanupNanoTime());

                return metrics;
            }
            finally {
                part.release();
            }
        }

        /** */
        @SuppressWarnings("unchecked")
        @NotNull private Object addRest(@Nullable Object rest, MvccDataRow row) {
            if (rest == null)
                rest = row;
            else if (rest.getClass() == ArrayList.class)
                ((List)rest).add(row);
            else {
                ArrayList list = new ArrayList();

                list.add(rest);
                list.add(row);

                rest = list;
            }

            return rest;
        }

        /**
         * @param rows Collection of rows.
         * @param row Row to add.
         * @return Collection of rows.
         */
        @NotNull private List<MvccLinkAwareSearchRow> addRow(@Nullable List<MvccLinkAwareSearchRow> rows, MvccDataRow row) {
            if (rows == null)
                rows = new ArrayList<>();

            rows.add(new MvccLinkAwareSearchRow(row.cacheId(), row.key(), row.mvccCoordinatorVersion(),
                row.mvccCounter(), row.mvccOperationCounter(), row.link()));

            return rows;
        }

        /**
         * @param row Mvcc row to check.
         * @param snapshot Cleanup version to compare with.
         * @param cctx Cache context.
         * @throws IgniteCheckedException If failed.
         */
        private boolean canClean(MvccDataRow row, MvccSnapshot snapshot,
            GridCacheContext cctx) throws IgniteCheckedException {
            // Row can be safely cleaned if it has ABORTED min version or COMMITTED and less than cleanup one max version.
            return compare(row, snapshot.coordinatorVersion(), snapshot.cleanupVersion()) <= 0
                && hasNewVersion(row) && MvccUtils.compareNewVersion(row, snapshot.coordinatorVersion(), snapshot.cleanupVersion()) <= 0
                && MvccUtils.state(cctx, row.newMvccCoordinatorVersion(), row.newMvccCounter(),
                row.newMvccOperationCounter() | (row.newMvccTxState() << MVCC_HINTS_BIT_OFF)) == TxState.COMMITTED
                || MvccUtils.state(cctx, row.mvccCoordinatorVersion(), row.mvccCounter(),
                row.mvccOperationCounter() | (row.mvccTxState() << MVCC_HINTS_BIT_OFF)) == TxState.ABORTED;
        }

        /** */
        private boolean actualize(GridCacheContext cctx, MvccDataRow row,
            MvccSnapshot snapshot) throws IgniteCheckedException {
            return isVisible(cctx, snapshot, row.mvccCoordinatorVersion(), row.mvccCounter(), row.mvccOperationCounter(), false)
                && (row.mvccTxState() == TxState.NA || (row.newMvccCoordinatorVersion() != MVCC_CRD_COUNTER_NA && row.newMvccTxState() == TxState.NA));
        }

        /**
         * @param part Local partition.
         * @param key Key.
         * @param cleanupRows Cleanup rows.
         * @param cctx Cache context.
         * @param metrics Vacuum metrics.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void cleanup(GridDhtLocalPartition part, KeyCacheObject key, List<MvccLinkAwareSearchRow> cleanupRows,
            Object rest, GridCacheContext cctx, VacuumMetrics metrics) throws IgniteCheckedException {
            assert key != null && cctx != null && (!F.isEmpty(cleanupRows) || rest != null);

            cctx.gate().enter();

            try {
                long cleanupStartNanoTime = System.nanoTime();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                while (true) {
                    entry.lockEntry();

                    if (!entry.obsolete())
                        break;

                    entry.unlockEntry();

                    entry = cctx.cache().entryEx(key);
                }

                int cleaned = 0;

                try {
                    cctx.shared().database().checkpointReadLock();

                    try {
                        if (cleanupRows != null)
                            cleaned = part.dataStore().cleanup(cctx, cleanupRows);

                        if (rest != null) {
                            if (rest.getClass() == ArrayList.class) {
                                for (MvccDataRow row : ((List<MvccDataRow>) rest)) {
                                    part.dataStore().updateTxState(cctx, row);
                                }
                            } else
                                part.dataStore().updateTxState(cctx, (MvccDataRow) rest);
                        }
                    } finally {
                        cctx.shared().database().checkpointReadUnlock();
                    }
                } finally {
                    entry.unlockEntry();
                    cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);

                    metrics.addCleanupNanoTime(System.nanoTime() - cleanupStartNanoTime);
                    metrics.addCleanupRowsCnt(cleaned);
                }
            }
            finally {
                cctx.gate().leave();
            }
        }
    }

    /** */
    private static class ActiveTx {
        /** */
        private final long tracking;
        /** */
        private final UUID nearNodeId;

        /** */
        private ActiveTx(long tracking, UUID nearNodeId) {
            this.tracking = tracking;
            this.nearNodeId = nearNodeId;
        }
    }

    /** */
    private static class ActiveServerTx extends ActiveTx {
        /** */
        private ActiveServerTx(long tracking, UUID nearNodeId) {
            super(tracking, nearNodeId);
        }
    }
}
