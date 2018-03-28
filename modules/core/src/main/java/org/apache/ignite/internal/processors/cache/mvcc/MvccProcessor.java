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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQuery;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQuery;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQueryEx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccActiveQueriesMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccFutureResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccNewQueryAckRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccQuerySnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccSnapshotResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccTxSnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccWaitTxsRequest;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxKey;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.DatabaseLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog.TX_LOG_CACHE_ID;
import static org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog.TX_LOG_CACHE_NAME;

/**
 * MVCC processor.
 */
public class MvccProcessor extends GridProcessorAdapter implements DatabaseLifecycleListener {
    /** */
    private static final BiFunction<List<LockFuture>, List<LockFuture>, List<LockFuture>> CONC =
        new BiFunction<List<LockFuture>, List<LockFuture>, List<LockFuture>>() {
        /** {@inheritDoc} */
        @Override public List<LockFuture> apply(List<LockFuture> l1, List<LockFuture> l2) {
            ArrayList<LockFuture> res = new ArrayList<>(l1.size() + l2.size());

            res.addAll(l1);
            res.addAll(l2);

            return res;
        }
    };

    /** */
    public static final long MVCC_COUNTER_NA = 0L;

    /** */
    public static final long MVCC_START_CNTR = 1L;

    /** */
    private volatile MvccCoordinator curCrd;

    /** */
    private final AtomicLong mvccCntr = new AtomicLong(MVCC_START_CNTR);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(MVCC_START_CNTR);

    /** */
    private final Set<Long> activeTxs = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** */
    private final ActiveQueries activeQueries = new ActiveQueries();

    /** */
    private final MvccPreviousCoordinatorQueries prevCrdQueries = new MvccPreviousCoordinatorQueries();

    /** */
    private final ConcurrentMap<Long, MvccSnapshotFuture> snapshotFuts = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Long, WaitAckFuture> ackFuts = new ConcurrentHashMap<>();

    /** */
    private ConcurrentMap<Long, WaitTxFuture> waitTxFuts = new ConcurrentHashMap<>();

    /** */
    private final Map<TxKey, List<LockFuture>> waitList = new ConcurrentHashMap<>();

    /** */
    private final AtomicLong futIdCntr = new AtomicLong(0);

    /** */
    private final GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

    /** */
    private final CountDownLatch crdLatch = new CountDownLatch(1);

    /** Topology version when local node was assigned as coordinator. */
    private long crdVer;

    /** */
    private MvccDiscoveryData discoData = new MvccDiscoveryData(null);

    /** */
    private TxLog txLog;

    /** */
    private final ScheduledExecutorService vacuumScheduledExecutor;

    /** */
    private VacuumScheduler vacuumScheduler;

    /** */
    private final BlockingQueue<VacuumTask> cleanupQueue = new LinkedBlockingQueue<>();

    /** */
    private final List<VacuumWorker> vacuumWorkers;

    /** */
    private final Semaphore vacuumSemaphore = new Semaphore(1);

    /** For tests only. */
    private static IgniteClosure<Collection<ClusterNode>, ClusterNode> crdC;

    /** For tests only. */
    private volatile Throwable vacuumError;

    /**
     * @param ctx Context.
     */
    public MvccProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerDatabaseListener(this);

        vacuumScheduledExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactory() {
                @Override public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r, "vacuum-scheduler-node-" + ctx.grid().localNode().order());

                    thread.setDaemon(true);

                    return thread;
                }
            });

        vacuumWorkers = new ArrayList<>(ctx.config().getMvccVacuumThreadCnt());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        if (!ctx.clientNode()) {
            startVacuumWorkers();

            startVacuumScheduler();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (!ctx.clientNode()) {
            stopVacuumScheduler();

            stopVacuumWorkers();
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(new CacheCoordinatorNodeFailListener(),
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(TOPIC_CACHE_COORDINATOR, new CoordinatorMessageListener());
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.CACHE_CRD_PROC;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        Integer cmpId = discoveryDataType().ordinal();

        if (!dataBag.commonDataCollectedFor(cmpId))
            dataBag.addGridCommonData(cmpId, discoData);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        MvccDiscoveryData discoData0 = (MvccDiscoveryData)data.commonData();

        // Disco data might be null in case the first joined node is daemon.
        if (discoData0 != null) {
            discoData = discoData0;

            log.info("Received mvcc coordinator on node join: " + discoData.coordinator());

            assert discoData != null;
        }
    }

    /**
     * @return State for given mvcc version.
     * @throws IgniteCheckedException If fails.
     */
    public byte state(long crdVer, long cntr) throws IgniteCheckedException {
        return txLog.get(crdVer, cntr);
    }

    /**
     * @param ver Version to check.
     * @return State for given mvcc version.
     * @throws IgniteCheckedException If fails.
     */
    public byte state(MvccVersion ver) throws IgniteCheckedException {
        return txLog.get(ver.coordinatorVersion(), ver.counter());
    }

    /**
     * @param ver Version.
     * @param state State.
     * @throws IgniteCheckedException If fails;
     */
    public void updateState(MvccVersion ver, byte state) throws IgniteCheckedException {
        List<LockFuture> waiting;

        txLog.put(ver.coordinatorVersion(), ver.counter(), state);

        if ((state == TxState.ABORTED || state == TxState.COMMITTED)
                && (waiting = waitList.remove(new TxKey(ver.coordinatorVersion(), ver.counter()))) != null) {
            for (LockFuture fut0 : waiting)
                complete(fut0);
        }
    }

    /**
     * @param cctx Cache context.
     * @param locked Version the entry is locked by.
     */
    public IgniteInternalFuture waitFor(GridCacheContext cctx, MvccVersion locked) throws IgniteCheckedException {
        TxKey key = new TxKey(locked.coordinatorVersion(), locked.counter());
        LockFuture fut = new LockFuture(cctx.ioPolicy());

        List<LockFuture> waiting = new ArrayList<>(1); waiting.add(fut);

        waitList.merge(key, waiting, CONC);

        byte state = txLog.get(key);

        if ((state == TxState.ABORTED || state == TxState.COMMITTED)
                && (waiting = waitList.remove(key)) != null) {
            for (LockFuture fut0 : waiting)
                complete(fut0);
        }

        return fut;
    }

    /**
     * Checks whether the transaction with given version is active.
     * @return {@code True} If active.
     * @throws IgniteCheckedException If fails.
     */
    public boolean isActive(long crdVer, long cntr) throws IgniteCheckedException {
        byte state = state(crdVer, cntr);

        return !(state == TxState.COMMITTED || state == TxState.ABORTED);
    }

    /**
     * Removes all less or equals to the given one records from Tx log.
     *
     * @param ver Version.
     * @throws IgniteCheckedException If fails.
     */
    public void removeUntil(MvccVersion ver) throws IgniteCheckedException {
        txLog.removeUntil(ver.coordinatorVersion(), ver.counter());
    }

    /**
     * @param fut Lock future.
     */
    private void complete(LockFuture fut) {
        try {
            ctx.pools().poolForPolicy(fut.plc).execute(fut);
        }
        catch (IgniteCheckedException e) {
            U.error(log, e);
        }
    }

    /**
     * @return Discovery data.
     */
    public MvccDiscoveryData discoveryData() {
        return discoData;
    }

    /**
     * For testing only.
     *
     * @param crdC Closure assigning coordinator.
     */
    static void coordinatorAssignClosure(IgniteClosure<Collection<ClusterNode>, ClusterNode> crdC) {
        MvccProcessor.crdC = crdC;
    }

    /**
     * @param evtType Event type.
     * @param nodes Current nodes.
     * @param topVer Topology version.
     */
    public void onDiscoveryEvent(int evtType, Collection<ClusterNode> nodes, long topVer) {
        if (evtType == EVT_NODE_METRICS_UPDATED || evtType == EVT_DISCOVERY_CUSTOM_EVT)
            return;

        MvccCoordinator crd;

        if (evtType == EVT_NODE_SEGMENTED || evtType == EVT_CLIENT_NODE_DISCONNECTED)
            crd = null;
        else {
            crd = discoData.coordinator();

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
                        if (!CU.clientNode(node)) {
                            crdNode = node;

                            break;
                        }
                    }
                }

                crd = crdNode != null ? new MvccCoordinator(crdNode.id(), coordinatorVersion(topVer), new AffinityTopologyVersion(topVer, 0)) : null;

                if (crd != null) {
                    if (log.isInfoEnabled())
                        log.info("Assigned mvcc coordinator [crd=" + crd + ", crdNode=" + crdNode + ']');
                }
                else
                    U.warn(log, "New mvcc coordinator was not assigned [topVer=" + topVer + ']');
            }
        }

        discoData = new MvccDiscoveryData(crd);
    }

    /**
     * @param topVer Topology version.
     * @return Coordinator version.
     */
    private long coordinatorVersion(long topVer) {
        return topVer + ctx.discovery().gridStartTime();
    }

    /**
     * @param tx Transaction.
     * @return Counter.
     */
    public MvccSnapshot requestTxSnapshotOnCoordinator(IgniteInternalTx tx) {
        assert ctx.localNodeId().equals(currentCoordinatorId());

        return assignTxSnapshot(0L);
    }

    /**
     * @param ver Version.
     * @return Counter.
     */
    public MvccSnapshot requestTxSnapshotOnCoordinator(GridCacheVersion ver) {
        assert ctx.localNodeId().equals(currentCoordinatorId());

        return assignTxSnapshot(0L);
    }

    /**
     * @param crd Coordinator.
     * @param lsnr Response listener.
     * @param txVer Transaction version.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccSnapshot> requestTxSnapshot(MvccCoordinator crd,
        MvccSnapshotResponseListener lsnr,
        GridCacheVersion txVer) {
        assert !ctx.localNodeId().equals(crd.nodeId());

        MvccSnapshotFuture fut = new MvccSnapshotFuture(futIdCntr.incrementAndGet(), crd, lsnr);

        snapshotFuts.put(fut.id, fut);

        try {
            sendMessage(crd.nodeId(), new MvccTxSnapshotRequest(fut.id, txVer));
        }
        catch (IgniteCheckedException e) {
            if (snapshotFuts.remove(fut.id) != null)
                fut.onError(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param mvccVer Query version.
     */
    public void ackQueryDone(MvccCoordinator crd, MvccSnapshot mvccVer) {
        assert crd != null;

        long trackCntr = queryTrackCounter(mvccVer);

        Message msg = crd.coordinatorVersion() == mvccVer.coordinatorVersion() ? new MvccAckRequestQuery(trackCntr) :
            new MvccNewQueryAckRequest(mvccVer.coordinatorVersion(), trackCntr);

        try {
            sendMessage(crd.nodeId(), msg);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query ack, node left [crd=" + crd + ", msg=" + msg + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query ack [crd=" + crd + ", msg=" + msg + ']', e);
        }
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
     * @param crd Coordinator.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccSnapshot> requestQuerySnapshot(MvccCoordinator crd) {
        assert crd != null;

        MvccSnapshotFuture fut = new MvccSnapshotFuture(futIdCntr.incrementAndGet(), crd, null);

        snapshotFuts.put(fut.id, fut);

        try {
            sendMessage(crd.nodeId(), new MvccQuerySnapshotRequest(fut.id));
        }
        catch (IgniteCheckedException e) {
            if (snapshotFuts.remove(fut.id) != null)
                fut.onError(e);
        }

        return fut;
    }

    /**
     * @param crdId Coordinator ID.
     * @param txs Transaction IDs.
     * @return Future.
     */
    public IgniteInternalFuture<Void> waitTxsFuture(UUID crdId, GridLongList txs) {
        assert crdId != null;
        assert txs != null && txs.size() > 0;

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

    /**
     * @param crd Coordinator.
     * @param updateVer Transaction update version.
     * @param readVer Transaction read version.
     * @return Acknowledge future.
     */
    public IgniteInternalFuture<Void> ackTxCommit(UUID crd,
        MvccSnapshot updateVer,
        @Nullable MvccSnapshot readVer) {
        assert crd != null;
        assert updateVer != null;

        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crd, true);

        ackFuts.put(fut.id, fut);

        MvccAckRequestTx msg = createTxAckMessage(fut.id, updateVer, readVer);

        try {
            sendMessage(crd, msg);
        }
        catch (IgniteCheckedException e) {
            if (ackFuts.remove(fut.id) != null) {
                if (e instanceof ClusterTopologyCheckedException)
                    fut.onDone(); // No need to ack, finish without error.
                else
                    fut.onDone(e);
            }
        }

        return fut;
    }

    /**
     * @param futId Future ID.
     * @param updateVer Update version.
     * @param readVer Optional read version.
     * @return Message.
     */
    private MvccAckRequestTx createTxAckMessage(long futId,
        MvccSnapshot updateVer,
        @Nullable MvccSnapshot readVer) {
        MvccAckRequestTx msg;

        if (readVer != null) {
            long trackCntr = queryTrackCounter(readVer);

            if (readVer.coordinatorVersion() == updateVer.coordinatorVersion()) {
                msg = new MvccAckRequestTxAndQuery(futId,
                    updateVer.counter(),
                    trackCntr);
            }
            else {
                msg = new MvccAckRequestTxAndQueryEx(futId,
                    updateVer.counter(),
                    readVer.coordinatorVersion(),
                    trackCntr);
            }
        }
        else
            msg = new MvccAckRequestTx(futId, updateVer.counter());

        return msg;
    }

    /**
     * @param crdId Coordinator node ID.
     * @param updateVer Transaction update version.
     * @param readVer Transaction read version.
     */
    public void ackTxRollback(UUID crdId, MvccSnapshot updateVer, @Nullable MvccSnapshot readVer) {
        MvccAckRequestTx msg = createTxAckMessage(-1, updateVer, readVer);

        msg.skipResponse(true);

        try {
            sendMessage(crdId, msg);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx rollback ack, node left [msg=" + msg + ", node=" + crdId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx rollback ack [msg=" + msg + ", node=" + crdId + ']', e);
        }
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

        MvccSnapshotResponse res = assignQueryCounter(nodeId, msg.futureId());

        try {
            sendMessage(node.id(), res);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query counter response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query counter response [msg=" + msg + ", node=" + nodeId + ']', e);

            onQueryDone(nodeId, res.counter());
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorSnapshotResponse(UUID nodeId, MvccSnapshotResponse msg) {
        MvccSnapshotFuture fut = snapshotFuts.remove(msg.futureId());

        if (fut != null) {
            fut.onResponse(msg);
        }
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
    private void processCoordinatorQueryAckRequest(UUID nodeId, MvccAckRequestQuery msg) {
        onQueryDone(nodeId, msg.counter());
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processNewCoordinatorQueryAckRequest(UUID nodeId, MvccNewQueryAckRequest msg) {
        prevCrdQueries.onQueryDone(nodeId, msg.coordinatorVersion(), msg.counter());
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxAckRequest(UUID nodeId, MvccAckRequestTx msg) {
        onTxDone(msg.txCounter(), msg.futureId() >= 0);

        if (msg.queryCounter() != MVCC_COUNTER_NA) {
            if (msg.queryCoordinatorVersion() == 0)
                onQueryDone(nodeId, msg.queryCounter());
            else
                prevCrdQueries.onQueryDone(nodeId, msg.queryCoordinatorVersion(), msg.queryCounter());
        }

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

        if (fut != null) {
            fut.onResponse();
        }
        else {
            if (ctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @return Counter.
     */
    private MvccSnapshotResponse assignTxSnapshot(long futId) {
        assert crdVer != 0;

        MvccSnapshotResponse res = new MvccSnapshotResponse();

        lock.writeLock();

        long ver = mvccCntr.incrementAndGet(), cleanupVer = committedCntr.get() + 1;

        for (Long txVer : activeTxs) {
            if (txVer < cleanupVer)
                cleanupVer = txVer;

            res.addTx(txVer);
        }

        boolean add = activeTxs.add(ver);

        lock.writeUnlock();

        assert add : ver;

        long minQry = activeQueries.minimalQueryCounter();

        if (minQry != -1)
            cleanupVer = Math.min(cleanupVer, minQry);

        cleanupVer--;

        res.init(futId, crdVer, ver, cleanupVer);

        return res;
    }

    /**
     * @param txCntr Counter assigned to transaction.
     */
    private void onTxDone(Long txCntr, boolean committed) {
        GridFutureAdapter fut;

        lock.readLock();

        activeTxs.remove(txCntr);

        if (committed)
            committedCntr.setIfGreater(txCntr);

        lock.readUnlock();

        fut = waitTxFuts.remove(txCntr);

        if (fut != null)
            fut.onDone();
    }

    /** {@inheritDoc} */
    @Override public void onInitDataRegions(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        DataStorageConfiguration dscfg = dataStorageConfiguration();

        mgr.addDataRegion(
            dscfg,
            createTxLogRegion(dscfg),
            CU.isPersistenceEnabled(ctx.config()));
    }

    /** {@inheritDoc} */
    @Override public void afterInitialise(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        if (!CU.isPersistenceEnabled(ctx.config())) {
            assert txLog == null;

            txLog = new TxLog(ctx, mgr);
        }
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
    @Override public void afterMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());
        assert txLog == null;

        txLog = new TxLog(ctx, mgr);
    }

    /** {@inheritDoc} */
    @Override public void beforeStop(IgniteCacheDatabaseSharedManager mgr) {
        txLog = null;
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
     * For tests only.
     *
     * @return Vacuum error.
     */
    public Throwable getVacuumError() {
        return vacuumError;
    }

    /**
     * For tests only.
     *
     * @param e Vacuum error.
     */
    void setVacuumError(Throwable e) {
        this.vacuumError = e;
    }

    /**
     *
     */
    class ActiveQueries {
        /** */
        private final Map<UUID, TreeMap<Long, AtomicInteger>> activeQueries = new HashMap<>();

        /** */
        private Long minQry;

        synchronized long minimalQueryCounter() {
            return minQry == null ? -1 : minQry;
        }

        synchronized MvccSnapshotResponse assignQueryCounter(UUID nodeId, long futId) {
            MvccSnapshotResponse res = new MvccSnapshotResponse();

            lock.writeLock();

            long ver = committedCntr.get(), trackVer = ver;

            for (Long txVer : activeTxs) {
                assert txVer != ver;

                if (txVer < ver) {
                    res.addTx(txVer);

                    if (txVer < trackVer)
                        trackVer = txVer;
                }
            }

            lock.writeUnlock();

            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null) {
                activeQueries.put(nodeId, nodeMap = new TreeMap<>());

                nodeMap.put(trackVer, new AtomicInteger(1));
            }
            else {
                AtomicInteger cntr = nodeMap.get(trackVer);

                if (cntr == null)
                    nodeMap.put(trackVer, new AtomicInteger(1));
                else
                    cntr.incrementAndGet();
            }

            if (minQry == null)
                minQry = trackVer;

            res.init(futId, crdVer, ver, MVCC_COUNTER_NA);

            return res;
        }

        synchronized void onQueryDone(UUID nodeId, Long ver) {
            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null)
                return;

            assert minQry != null;

            AtomicInteger cntr = nodeMap.get(ver);

            assert cntr != null && cntr.get() > 0;

            if (cntr.decrementAndGet() == 0) {
                nodeMap.remove(ver);

                if (nodeMap.isEmpty())
                    activeQueries.remove(nodeId);

                if (ver.equals(minQry))
                    minQry = activeMinimal();
            }


        }

        synchronized void onNodeFailed(UUID nodeId) {
            activeQueries.remove(nodeId);

            minQry = activeMinimal();
        }

        private Long activeMinimal() {
            Long min = null;

            for (TreeMap<Long,AtomicInteger> s : activeQueries.values()) {
                Long first = s.firstKey();

                if (min == null || first < min)
                    min = first;
            }

            return min;
        }
    }

    /**
     * @param qryNodeId Node initiated query.
     * @return Counter for query.
     */
    private synchronized MvccSnapshotResponse assignQueryCounter(UUID qryNodeId, long futId) {
        assert crdVer != 0;

        return activeQueries.assignQueryCounter(qryNodeId, futId);
    }

    /**
     * @param mvccCntr Query counter.
     */
    private void onQueryDone(UUID nodeId, Long mvccCntr) {
        activeQueries.onQueryDone(nodeId, mvccCntr);
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

            WaitTxFuture fut = waitTxFuts.get(txId);

            if (fut == null) {
                WaitTxFuture old = waitTxFuts.putIfAbsent(txId, fut = new WaitTxFuture(txId));

                if (old != null)
                    fut = old;
            }

            if (!activeTxs.contains(txId))
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

    /**
     * @return Coordinator.
     */
    public MvccCoordinator currentCoordinator() {
        return curCrd;
    }

    /**
     * @param curCrd Coordinator.
     */
    public void currentCoordinator(MvccCoordinator curCrd) {
        this.curCrd = curCrd;
    }

    /**
     * @return Current coordinator node ID.
     */
    public UUID currentCoordinatorId() {
        MvccCoordinator curCrd = this.curCrd;

        return curCrd != null ? curCrd.nodeId() : null;
    }

    /**
     * @param topVer Cache affinity version (used for assert).
     * @return Coordinator.
     */
    public MvccCoordinator currentCoordinatorForCacheAffinity(AffinityTopologyVersion topVer) {
        MvccCoordinator crd = curCrd;

        // Assert coordinator did not already change.
        assert crd == null || crd.topologyVersion().compareTo(topVer) <= 0 :
            "Invalid coordinator [crd=" + crd + ", topVer=" + topVer + ']';

        return crd;
    }

    /**
     * @param nodeId Node ID
     * @param activeQueries Active queries.
     */
    public void processClientActiveQueries(UUID nodeId,
        @Nullable Map<MvccVersion, Integer> activeQueries) {
        prevCrdQueries.addNodeActiveQueries(nodeId, activeQueries);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processCoordinatorActiveQueriesMessage(UUID nodeId, MvccActiveQueriesMessage msg) {
        prevCrdQueries.addNodeActiveQueries(nodeId, msg.activeQueries());
    }

    /**
     * @param nodeId Coordinator node ID.
     * @param activeQueries Active queries.
     */
    public void sendActiveQueries(UUID nodeId, @Nullable Map<MvccVersion, Integer> activeQueries) {
        MvccActiveQueriesMessage msg = new MvccActiveQueriesMessage(activeQueries);

        try {
            sendMessage(nodeId, msg);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send active queries to mvcc coordinator: " + e);
        }
    }

    /**
     * @param topVer Topology version.
     * @param discoCache Discovery data.
     * @param activeQueries Current queries.
     */
    public void initCoordinator(AffinityTopologyVersion topVer,
        DiscoCache discoCache,
        Map<UUID, Map<MvccVersion, Integer>> activeQueries) {
        assert ctx.localNodeId().equals(curCrd.nodeId());

        MvccCoordinator crd = discoCache.mvccCoordinator();

        assert crd != null;

        // No need to re-initialize if coordinator version hasn't changed (e.g. it was cluster activation).
        if (crdVer == crd.coordinatorVersion())
            return;

        crdVer = crd.coordinatorVersion();

        log.info("Initialize local node as mvcc coordinator [node=" + ctx.localNodeId() +
            ", topVer=" + topVer +
            ", crdVer=" + crdVer + ']');

        prevCrdQueries.init(activeQueries, discoCache, ctx.discovery());

        crdLatch.countDown();
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
     * @param log Logger.
     * @param diagCtx Diagnostic request.
     */
    // TODO: Proper use of diagnostic context.
    public void dumpDebugInfo(IgniteLogger log, @Nullable IgniteDiagnosticPrepareContext diagCtx) {
        boolean first = true;

        for (MvccSnapshotFuture verFur : snapshotFuts.values()) {
            if (first) {
                U.warn(log, "Pending mvcc version futures: ");

                first = false;
            }

            U.warn(log, ">>> " + verFur.toString());
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
     * Runs vacuum process.
     *
     * @return {@code Future} with {@link VacuumMetrics}.
     * @throws IgniteCheckedException If failed.
     * @throws InterruptedException If failed.
     */
    public IgniteInternalFuture<VacuumMetrics> runVacuum() throws IgniteCheckedException, InterruptedException {
        assert vacuumWorkersRunning();
        assert !ctx.clientNode();

        if (Thread.currentThread().isInterrupted())
            return new GridFinishedFuture<>(new VacuumMetrics());

        GridCompoundIdentityFuture<VacuumMetrics> compoundFut =
            new GridCompoundIdentityFuture<>(new VacuumMetricsReducer());

        vacuumSemaphore.acquire();

        compoundFut.listen(new IgniteInClosure<IgniteInternalFuture<VacuumMetrics>>() {
            @Override public void apply(IgniteInternalFuture<VacuumMetrics> fut) {
                vacuumSemaphore.release();

                try {
                    VacuumMetrics metrics = fut.get();

                    if (log.isDebugEnabled())
                        log.debug("Vacuum completed. " + metrics);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Vacuum error. ", e);
                }
            }
        });

        try {
            MvccSnapshot snapshot;

            AtomicReference<MvccTxInfo> txInfo = new AtomicReference<>();

            if (ctx.localNodeId().equals(currentCoordinator().nodeId())) {
                snapshot = requestTxSnapshotOnCoordinator(new GridCacheVersion());

                txInfo.set(new MvccTxInfo(ctx.localNodeId(), snapshot));
            }
            else {
                final IgniteInternalFuture<MvccSnapshot> cleanupVerFut =
                    requestTxSnapshot(curCrd, new MvccSnapshotResponseListener() {
                        @Override public void onResponse(UUID crdId, MvccSnapshot res) {
                            txInfo.set(new MvccTxInfo(crdId, res));
                        }

                        @Override public void onError(IgniteCheckedException e) {
                            compoundFut.onDone(e);
                        }
                    }, new GridCacheVersion());

                snapshot = cleanupVerFut.get();
            }

            ackTxCommit(txInfo.get().coordinatorNodeId(), txInfo.get().snapshot(), null).get();

            if (snapshot.cleanupVersion() <= MVCC_COUNTER_NA) {
                compoundFut.onDone(new VacuumMetrics(), null);

                compoundFut.markInitialized();

                return compoundFut;
            }

            MvccVersion cleanupVer = new MvccVersionImpl(snapshot.coordinatorVersion(), snapshot.cleanupVersion());

            if (log.isDebugEnabled())
                log.debug("Started vacuum with cleanup version=" + cleanupVer + '.');

            for (CacheGroupContext grp : ctx.cache().cacheGroups()) {
                if (Thread.currentThread().isInterrupted())
                    break;

                if (!grp.userCache() || !grp.mvccEnabled())
                    continue;

                List<GridDhtLocalPartition> parts = grp.topology().localPartitions();

                if (parts.isEmpty())
                    continue;

                for (int i = 0; i < parts.size(); i++) {
                    if (Thread.currentThread().isInterrupted())
                        break;

                    GridDhtLocalPartition part = parts.get(i);

                    GridFutureAdapter<VacuumMetrics> fut = new GridFutureAdapter<>();

                    compoundFut.add(fut);

                    VacuumTask task = new VacuumTask(cleanupVer, part, fut);

                    cleanupQueue.put(task);
                }
            }

        }
        catch (Throwable e) {
            compoundFut.onDone(e);
        }

        compoundFut.markInitialized();

        return compoundFut;
    }

    /**
     * Start vacuum workers.
     */
    private void startVacuumWorkers() {
        synchronized (vacuumWorkers) {
            assert !ctx.clientNode();
            assert vacuumWorkers.isEmpty();

            for (int i = 0; i < ctx.config().getMvccVacuumThreadCnt(); i++) {
                VacuumWorker vacuumWorker = new VacuumWorker(ctx, log, cleanupQueue);

                vacuumWorkers.add(vacuumWorker);

                new IgniteThread(vacuumWorker).start();
            }
        }
    }

    /**
     * Stop vacuum workers.
     */
    private void stopVacuumWorkers() {
        synchronized (vacuumWorkers) {
            assert !ctx.clientNode();
            assert !vacuumWorkers.isEmpty();

            U.cancel(vacuumWorkers);

            vacuumWorkers.clear();
        }
    }

    /**
     * Start vacuum scheduler.
     */
    private void startVacuumScheduler() {
        assert !ctx.clientNode();

        synchronized (vacuumScheduledExecutor) {
            assert vacuumScheduler == null || vacuumScheduler.isCancelled();
            assert vacuumWorkersRunning();

            vacuumScheduler = new VacuumScheduler(ctx, log);

            int interval = ctx.config().getMvccVacuumTimeInterval();

            vacuumScheduledExecutor.scheduleWithFixedDelay(vacuumScheduler, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stop vacuum scheduler.
     */
    private void stopVacuumScheduler() {
        assert !ctx.clientNode();

        synchronized (vacuumScheduledExecutor) {
            if(vacuumScheduler != null && !vacuumScheduler.isCancelled())
                U.cancel(vacuumScheduler);

            U.shutdownNow(MvccProcessor.class, vacuumScheduledExecutor, log);
        }
    }

    /**
     * Check vacuum workers state.
     *
     * @return {@code True} if
     */
    private boolean vacuumWorkersRunning() {
        assert !ctx.clientNode();

        synchronized (vacuumWorkers) {
            if (vacuumWorkers.isEmpty())
                return false;

            for (VacuumWorker w : vacuumWorkers) {
                if (w.isCancelled() || w.isDone())
                    return false;
            }

            return true;
        }
    }

    /**
     *
     */
    private class MvccSnapshotFuture extends GridFutureAdapter<MvccSnapshot> implements MvccFuture {
        /** */
        private final Long id;

        /** */
        private MvccSnapshotResponseListener lsnr;

        /** */
        public final MvccCoordinator crd;

        /**
         * @param id Future ID.
         * @param crd Mvcc coordinator.
         * @param lsnr Listener.
         */
        MvccSnapshotFuture(Long id, MvccCoordinator crd, @Nullable MvccSnapshotResponseListener lsnr) {
            this.id = id;
            this.crd = crd;
            this.lsnr = lsnr;
        }

        /** {@inheritDoc} */
        @Override public UUID coordinatorNodeId() {
            return crd.nodeId();
        }

        /**
         * @param res Response.
         */
        void onResponse(MvccSnapshotResponse res) {
            assert res.counter() != MVCC_COUNTER_NA;

            if (lsnr != null)
                lsnr.onResponse(crd.nodeId(), res);

            onDone(res);
        }

        /**
         * @param err Error.
         */
        void onError(IgniteCheckedException err) {
            if (lsnr != null)
                lsnr.onError(err);

            onDone(err);
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crd.nodeId().equals(nodeId) && snapshotFuts.remove(id) != null) {
                ClusterTopologyCheckedException err = new ClusterTopologyCheckedException("Failed to request mvcc " +
                    "version, coordinator failed: " + nodeId);

                onError(err);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MvccSnapshotFuture [crd=" + crd.nodeId() + ", id=" + id + ']';
        }
    }

    /**
     *
     */
    private class WaitAckFuture extends GridFutureAdapter<Void> implements MvccFuture {
        /** */
        private final long id;

        /** */
        private final UUID crdId;

        /** */
        final boolean ackTx;

        /**
         * @param id Future ID.
         * @param crdId Coordinator node ID.
         * @param ackTx {@code True} if ack tx commit, {@code false} if waits for previous txs.
         */
        WaitAckFuture(long id, UUID crdId, boolean ackTx) {
            assert crdId != null;

            this.id = id;
            this.crdId = crdId;
            this.ackTx = ackTx;
        }

        /** {@inheritDoc} */
        @Override public UUID coordinatorNodeId() {
            return crdId;
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
            return "WaitAckFuture [crdId=" + crdId +
                ", id=" + id +
                ", ackTx=" + ackTx + ']';
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

            for (MvccSnapshotFuture fut : snapshotFuts.values())
                fut.onNodeLeft(nodeId);

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

            if (msg0.waitForCoordinatorInit()) {
                if (crdVer == 0) {
                    try {
                        U.await(crdLatch);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        U.warn(log, "Failed to wait for coordinator initialization, thread interrupted [" +
                            "msgNode=" + nodeId + ", msg=" + msg + ']');

                        return;
                    }

                    assert crdVer != 0L;
                }
            }

            if (msg instanceof MvccTxSnapshotRequest)
                processCoordinatorTxSnapshotRequest(nodeId, (MvccTxSnapshotRequest)msg);
            else if (msg instanceof MvccAckRequestTx)
                processCoordinatorTxAckRequest(nodeId, (MvccAckRequestTx)msg);
            else if (msg instanceof MvccFutureResponse)
                processCoordinatorAckResponse(nodeId, (MvccFutureResponse)msg);
            else if (msg instanceof MvccAckRequestQuery)
                processCoordinatorQueryAckRequest(nodeId, (MvccAckRequestQuery)msg);
            else if (msg instanceof MvccQuerySnapshotRequest)
                processCoordinatorQuerySnapshotRequest(nodeId, (MvccQuerySnapshotRequest)msg);
            else if (msg instanceof MvccSnapshotResponse)
                processCoordinatorSnapshotResponse(nodeId, (MvccSnapshotResponse)msg);
            else if (msg instanceof MvccWaitTxsRequest)
                processCoordinatorWaitTxsRequest(nodeId, (MvccWaitTxsRequest)msg);
            else if (msg instanceof MvccNewQueryAckRequest)
                processNewCoordinatorQueryAckRequest(nodeId, (MvccNewQueryAckRequest)msg);
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
    private static class LockFuture extends GridFutureAdapter implements Runnable {
        /** */
        private final byte plc;

        /**
         * @param plc Pool policy.
         */
        LockFuture(byte plc) {
            this.plc = plc;
        }

        @Override public void run() {
            onDone();
        }
    }

    /**
     *
     */
    // TODO: Revisit WaitTxFuture purpose and usages. Looks like we do not need it at all.
    private static class WaitTxFuture extends GridFutureAdapter {
        /** */
        // TODO: Unused?
        private final long txId;

        /**
         * @param txId Transaction ID.
         */
        WaitTxFuture(long txId) {
            this.txId = txId;
        }
    }
}
