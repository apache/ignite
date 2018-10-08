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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
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
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
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
    private long crdVer;

    /** */
    private volatile MvccCoordinator curCrd;

    /** */
    private volatile MvccCoordinator assignedCrd;

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

    /** For tests only. */
    private volatile Throwable vacuumError;

    /** */
    private final GridAtomicLong futIdCntr = new GridAtomicLong(0);

    /** */
    private final GridAtomicLong mvccCntr = new GridAtomicLong(MVCC_START_CNTR);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(MVCC_INITIAL_CNTR);

    /** */
    private final Map<Long, Long> activeTxs = new HashMap<>();

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

    /**
     * @param ctx Context.
     */
    public MvccProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerDatabaseListener(this);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(new CacheCoordinatorNodeFailListener(),
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(TOPIC_CACHE_COORDINATOR, new CoordinatorMessageListener());
    }

    /** {@inheritDoc} */
    @Override public boolean mvccEnabled() {
        return mvccEnabled;
    }

    /** {@inheritDoc} */
    @Override public void preProcessCacheConfiguration(CacheConfiguration ccfg) {
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
        if (!ctx.clientNode() && txLog == null) {
            assert mvccEnabled && mvccSupported;

            txLog = new TxLog(ctx, ctx.cache().context().database());

            startVacuumWorkers();

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
    @Override public void afterInitialise(IgniteCacheDatabaseSharedManager mgr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void beforeMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());
        assert txLog == null;

        ctx.cache().context().pageStore().initialize(TX_LOG_CACHE_ID, 1,
            TX_LOG_CACHE_NAME, mgr.dataRegion(TX_LOG_CACHE_NAME).memoryMetrics());
    }

    /** {@inheritDoc} */
    @Override public void afterMemoryRestore(IgniteCacheDatabaseSharedManager mgr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryEvent(int evtType, Collection<ClusterNode> nodes, long topVer,
        @Nullable DiscoveryCustomMessage customMsg) {
        if (evtType == EVT_NODE_METRICS_UPDATED)
            return;

        if (evtType == EVT_DISCOVERY_CUSTOM_EVT)
            checkMvccCacheStarted(customMsg);
        else
            assignMvccCoordinator(evtType, nodes, topVer);
    }

    /** {@inheritDoc} */
    @Override public void onExchangeStart(MvccCoordinator mvccCrd, ExchangeContext exchCtx, ClusterNode exchCrd) {
        if (!exchCtx.newMvccCoordinator())
            return;

        GridLongList activeQryTrackers = collectActiveQueryTrackers();

        exchCtx.addActiveQueries(ctx.localNodeId(), activeQryTrackers);

        if (exchCrd == null || !mvccCrd.nodeId().equals(exchCrd.id())) {
            try {
                sendMessage(mvccCrd.nodeId(), new MvccActiveQueriesMessage(activeQryTrackers));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send active queries to mvcc coordinator: " + e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onExchangeDone(boolean newCrd, DiscoCache discoCache, Map<UUID, GridLongList> activeQueries) {
        if (!newCrd)
            return;

        ctx.cache().context().tm().rollbackMvccTxOnCoordinatorChange();

        if (ctx.localNodeId().equals(curCrd.nodeId())) {
            assert ctx.localNodeId().equals(curCrd.nodeId());

            MvccCoordinator crd = discoCache.mvccCoordinator();

            assert crd != null;

            // No need to re-initialize if coordinator version hasn't changed (e.g. it was cluster activation).
            if (crdVer == crd.coordinatorVersion())
                return;

            crdVer = crd.coordinatorVersion();

            log.info("Initialize local node as mvcc coordinator [node=" + ctx.localNodeId() +
                ", crdVer=" + crdVer + ']');

            prevCrdQueries.init(activeQueries, F.view(discoCache.allNodes(), this::supportsMvcc), ctx.discovery());

            initFut.onDone();
        }
    }

    /** {@inheritDoc} */
    @Override public void processClientActiveQueries(UUID nodeId, @Nullable GridLongList activeQueries) {
        prevCrdQueries.addNodeActiveQueries(nodeId, activeQueries);
    }

    /** {@inheritDoc} */
    @Override @Nullable public MvccCoordinator currentCoordinator() {
        return currentCoordinator(AffinityTopologyVersion.NONE);
    }

    /** {@inheritDoc} */
    @Override @Nullable public MvccCoordinator currentCoordinator(AffinityTopologyVersion topVer) {
        MvccCoordinator crd = curCrd;

        // Assert coordinator did not already change.
        assert crd == null
            || topVer == AffinityTopologyVersion.NONE
            || crd.topologyVersion().compareTo(topVer) <= 0 : "Invalid coordinator [crd=" + crd + ", topVer=" + topVer + ']';

        return crd;
    }

    /** {@inheritDoc} */
    @Override @Nullable public MvccCoordinator assignedCoordinator() {
        return assignedCrd;
    }

    /** {@inheritDoc} */
    @Override public UUID currentCoordinatorId() {
        MvccCoordinator curCrd = this.curCrd;

        return curCrd != null ? curCrd.nodeId() : null;
    }

    /** {@inheritDoc} */
    @Override public void updateCoordinator(MvccCoordinator curCrd) {
        this.curCrd = curCrd;
    }

    /** {@inheritDoc} */
    @Override public byte state(long crdVer, long cntr) throws IgniteCheckedException {
        return txLog.get(crdVer, cntr);
    }

    /** {@inheritDoc} */
    @Override public byte state(MvccVersion ver) throws IgniteCheckedException {
        assert txLog != null && mvccEnabled;

        return txLog.get(ver.coordinatorVersion(), ver.counter());
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
    @Override public IgniteInternalFuture<Void> waitFor(GridCacheContext cctx, MvccVersion locked) throws IgniteCheckedException {
        TxKey key = new TxKey(locked.coordinatorVersion(), locked.counter());

        LockFuture fut = new LockFuture(cctx.ioPolicy());

        Waiter waiter = waitMap.merge(key, fut, Waiter::concat);

        byte state = txLog.get(key);

        if ((state == TxState.ABORTED || state == TxState.COMMITTED)
            && !waiter.hasLocalTransaction() && (waiter = waitMap.remove(key)) != null)
            waiter.run(ctx);

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
            return assignTxSnapshot(0L);
        else
            return activeQueries.assignQueryCounter(ctx.localNodeId(), 0L);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshotAsync() {
        return requestSnapshotAsync((IgniteInternalTx)null);
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
                lsnr.onResponse(assignTxSnapshot(0L));
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

            for (MvccSnapshotResponseListener lsnr : map.values()) {
                U.warn(log, ">>> " + lsnr.toString());
            }
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

    /** */
    private void assignMvccCoordinator(int evtType, Collection<ClusterNode> nodes, long topVer) {
        checkMvccSupported(nodes);

        MvccCoordinator crd;

        if (evtType == EVT_NODE_SEGMENTED || evtType == EVT_CLIENT_NODE_DISCONNECTED)
            crd = null;
        else {
            crd = assignedCrd;

            if (crd == null ||
                ((evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT) && !F.nodeIds(nodes).contains(crd.nodeId()))) {
                ClusterNode crdNode = null;

                if (crdC != null) {
                    crdNode = crdC.apply(nodes);

                    if (log.isInfoEnabled())
                        log.info("Assigned coordinator using test closure: " + crd);
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

                crd = crdNode != null ? new MvccCoordinator(crdNode.id(), coordinatorVersion(crdNode),
                    new AffinityTopologyVersion(topVer, 0)) : null;

                if (log.isInfoEnabled() && crd != null)
                    log.info("Assigned mvcc coordinator [crd=" + crd + ", crdNode=" + crdNode + ']');
                else if (crd == null)
                    U.warn(log, "New mvcc coordinator was not assigned [topVer=" + topVer + ']');
            }
        }

        assignedCrd = crd;
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
    private void checkMvccCacheStarted(@Nullable DiscoveryCustomMessage customMsg) {
        assert customMsg != null;

        if (!mvccEnabled && customMsg instanceof DynamicCacheChangeBatch) {
            for (DynamicCacheChangeRequest req : ((DynamicCacheChangeBatch)customMsg).requests()) {
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

    /**
     * @return Active queries list.
     */
    private GridLongList collectActiveQueryTrackers() {
        assert curCrd != null;

        GridLongList activeQryTrackers = new GridLongList();

        for (MvccQueryTracker tracker : activeTrackers.values()) {
            long trackerId = tracker.onMvccCoordinatorChange(curCrd);

            if (trackerId != MVCC_TRACKER_ID_NA)
                activeQryTrackers.add(trackerId);
        }

        return activeQryTrackers;
    }

    /**
     * @return Counter.
     */
    private MvccSnapshotResponse assignTxSnapshot(long futId) {
        assert initFut.isDone();
        assert crdVer != 0;
        assert ctx.localNodeId().equals(currentCoordinatorId());

        MvccSnapshotResponse res = new MvccSnapshotResponse();

        long ver, cleanup, tracking;

        synchronized (this) {
            ver = mvccCntr.incrementAndGet();
            tracking = ver;
            cleanup = committedCntr.get() + 1;

            for (Map.Entry<Long, Long> txVer : activeTxs.entrySet()) {
                cleanup = Math.min(txVer.getValue(), cleanup);
                tracking = Math.min(txVer.getKey(), tracking);

                res.addTx(txVer.getKey());
            }

            boolean add = activeTxs.put(ver, tracking) == null;

            assert add : ver;
        }

        long minQry = activeQueries.minimalQueryCounter();

        if (minQry != -1)
            cleanup = Math.min(cleanup, minQry);

        cleanup = prevCrdQueries.previousQueriesDone() ? cleanup - 1 : MVCC_COUNTER_NA;

        res.init(futId, crdVer, ver, MVCC_START_OP_CNTR, cleanup, tracking);

        return res;
    }

    /**
     * @param txCntr Counter assigned to transaction.
     */
    private void onTxDone(Long txCntr, boolean committed) {
        assert initFut.isDone();

        GridFutureAdapter fut;

        synchronized (this) {
            activeTxs.remove(txCntr);

            if (committed)
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
                if (log.isInfoEnabled())
                    log.info("Attempting to stop inactive vacuum.");

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

        final GridCompoundIdentityFuture<VacuumMetrics> res =
            new GridCompoundIdentityFuture<>(new VacuumMetricsReducer());

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
     * For tests only.
     *
     * @return Vacuum error.
     */
    Throwable vacuumError() {
        return vacuumError;
    }

    /**
     * For tests only.
     *
     * @param e Vacuum error.
     */
    void vacuumError(Throwable e) {
        this.vacuumError = e;
    }

    /**
     * @param res Result.
     * @param snapshot Snapshot.
     */
    private void continueRunVacuum(GridCompoundIdentityFuture<VacuumMetrics> res, MvccSnapshot snapshot) {
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

                                for (CacheGroupContext grp : ctx.cache().cacheGroups()) {
                                    if (grp.mvccEnabled()) {
                                        for (GridDhtLocalPartition part : grp.topology().localPartitions()) {
                                            VacuumTask task = new VacuumTask(snapshot, part);

                                            cleanupQueue.offer(task);

                                            res.add(task);
                                        }
                                    }
                                }
                            }

                            res.listen(new CI1<IgniteInternalFuture<VacuumMetrics>>() {
                                @Override public void apply(IgniteInternalFuture<VacuumMetrics> fut) {
                                    try {
                                        VacuumMetrics metrics = fut.get();

                                        if (U.assertionsEnabled()) {
                                            MvccCoordinator crd = currentCoordinator();

                                            assert crd != null
                                                && crd.coordinatorVersion() >= snapshot.coordinatorVersion();

                                            for (TxKey key : waitMap.keySet()) {
                                                assert key.major() == snapshot.coordinatorVersion()
                                                    && key.minor() > snapshot.cleanupVersion()
                                                    || key.major() > snapshot.coordinatorVersion() :
                                                    "key=" + key + ", snapshot=" + snapshot;
                                            }
                                        }

                                        txLog.removeUntil(snapshot.coordinatorVersion(), snapshot.cleanupVersion());

                                        if (log.isDebugEnabled())
                                            log.debug("Vacuum completed. " + metrics);
                                    }
                                    catch (NodeStoppingException ignored) {
                                        if (log.isDebugEnabled())
                                            log.debug("Cannot complete vacuum (node is stopping).");
                                    }
                                    catch (Throwable e) {
                                        U.error(log, "Vacuum error.", e);
                                    }
                                }
                            });

                            res.markInitialized();
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

        MvccSnapshotResponse res = assignTxSnapshot(msg.futureId());

        try {
            sendMessage(node.id(), res);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx snapshot response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx snapshot response [msg=" + msg + ", node=" + nodeId + ']', e);
        }
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
            U.error(log, "Failed to send query counter response [msg=" + msg + ", node=" + nodeId + ']', e);

            onQueryDone(nodeId, res.tracking());
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

    /**
     *
     */
    private class CacheCoordinatorNodeFailListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            assert evt instanceof DiscoveryEvent : evt;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            UUID nodeId = discoEvt.eventNode().id();

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

            for (WaitAckFuture fut : ackFuts.values())
                fut.onNodeLeft(nodeId);

            activeQueries.onNodeFailed(nodeId);

            prevCrdQueries.onNodeFailed(nodeId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CacheCoordinatorDiscoveryListener[]";
        }
    }

    /**
     *
     */
    private class CoordinatorMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
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
            else
                U.warn(log, "Unexpected message received [node=" + nodeId + ", msg=" + msg + ']');
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CoordinatorMessageListener[]";
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
    }

    /** */
    private static class LockFuture extends GridFutureAdapter<Void> implements Waiter, Runnable {
        /** */
        private final byte plc;

        /**
         * @param plc Pool policy.
         */
        LockFuture(byte plc) {
            this.plc = plc;
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
                for (Waiter waiter : (List<Waiter>)inner) {
                    waiter.run(ctx);
                }
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
        private final static long VACUUM_TIMEOUT = 60_000;

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
                    throw e;
                }
                catch (Throwable e) {
                    prc.vacuumError(e);

                    if (e instanceof Error)
                        throw (Error) e;
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
                    if (task.part().state() != OWNING) {
                        task.part().group().preloader().rebalanceFuture()
                            .listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                                @Override public void apply(IgniteInternalFuture<Boolean> future) {
                                    cleanupQueue.add(task);
                                }
                            });

                        continue;
                    }

                    task.onDone(processPartition(task));
                }
                catch (IgniteInterruptedCheckedException e) {
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

            if (part == null || part.state() != OWNING || !part.reserve())
                return metrics;

            try {
                GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(KEY_ONLY);

                KeyCacheObject prevKey = null;

                Object rest = null;

                List<MvccLinkAwareSearchRow> cleanupRows = null;

                MvccSnapshot snapshot = task.snapshot();

                GridCacheContext cctx = null;

                int curCacheId = CU.UNDEFINED_CACHE_ID;

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

            long cleanupStartNanoTime = System.nanoTime();

            GridCacheEntryEx entry = cctx.cache().entryEx(key);

            while (true) {
                entry.lockEntry();

                if (!entry.obsolete())
                    break;

                entry.unlockEntry();

                entry = cctx.cache().entryEx(key);
            }

            cctx.shared().database().checkpointReadLock();

            int cleaned = 0;

            try {
                if (cleanupRows != null)
                    cleaned = part.dataStore().cleanup(cctx, cleanupRows);

                if (rest != null) {
                    if (rest.getClass() == ArrayList.class) {
                        for (MvccDataRow row : ((List<MvccDataRow>)rest)) {
                            part.dataStore().updateTxState(cctx, row);
                        }
                    }
                    else
                        part.dataStore().updateTxState(cctx, (MvccDataRow)rest);
                }
            }
            finally {
                cctx.shared().database().checkpointReadUnlock();

                entry.unlockEntry();
                cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);

                metrics.addCleanupNanoTime(System.nanoTime() - cleanupStartNanoTime);
                metrics.addCleanupRowsCnt(cleaned);
            }
        }
    }
}
