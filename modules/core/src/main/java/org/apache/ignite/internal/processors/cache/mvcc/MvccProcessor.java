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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccActiveQueriesMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQuery;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQuery;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQueryEx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccFutureResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccTxCounterRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccQueryVersionRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccVersionResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccWaitTxsRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccNewQueryAckRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * MVCC processor.
 */
public class MvccProcessor extends GridProcessorAdapter {
    /** */
    public static final long MVCC_COUNTER_NA = 0L;

    /** */
    public static final long MVCC_START_CNTR = 1L;

    /** */
    private static final boolean STAT_CNTRS = false;

    /** */
    private static final GridTopic MSG_TOPIC = TOPIC_CACHE_COORDINATOR;

    /** */
    private static final byte MSG_POLICY = SYSTEM_POOL;

    /** */
    private static final long CRD_VER_MASK = 0x3F_FF_FF_FF_FF_FF_FF_FFL;

    /** */
    private static final long RMVD_VAL_VER_MASK = 0x80_00_00_00_00_00_00_00L;

    /** */
    private volatile MvccCoordinator curCrd;

    /** */
    private final AtomicLong mvccCntr = new AtomicLong(MVCC_START_CNTR);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(1L);

    /** */
    private final ConcurrentSkipListMap<Long, GridCacheVersion> activeTxs = new ConcurrentSkipListMap<>();

    /** */
    private final ActiveQueries activeQueries = new ActiveQueries();

    /** */
    private final MvccPreviousCoordinatorQueries prevCrdQueries = new MvccPreviousCoordinatorQueries();

    /** */
    private final ConcurrentMap<Long, MvccVersionFuture> verFuts = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Long, WaitAckFuture> ackFuts = new ConcurrentHashMap<>();

    /** */
    private ConcurrentMap<Long, WaitTxFuture> waitTxFuts = new ConcurrentHashMap<>();

    /** */
    private final AtomicLong futIdCntr = new AtomicLong();

    /** */
    private final CountDownLatch crdLatch = new CountDownLatch(1);

    /** Topology version when local node was assigned as coordinator. */
    private long crdVer;

    /** */
    private StatCounter[] statCntrs;

    /** */
    private MvccDiscoveryData discoData = new MvccDiscoveryData(null);

    /** For tests only. */
    private static IgniteClosure<Collection<ClusterNode>, ClusterNode> crdC;

    /**
     * @param ctx Context.
     */
    public MvccProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Counter.
     * @return Always {@code true}.
     */
    public static boolean assertMvccVersionValid(long crdVer, long cntr) {
        assert unmaskCoordinatorVersion(crdVer) > 0;
        assert cntr != MVCC_COUNTER_NA;

        return true;
    }

    /**
     * @param crdVer Coordinator version.
     * @return Coordinator version with removed value flag.
     */
    public static long createVersionForRemovedValue(long crdVer) {
        return crdVer | RMVD_VAL_VER_MASK;
    }

    /**
     * @param crdVer Coordinator version with flags.
     * @return {@code True} if removed value flag is set.
     */
    public static boolean versionForRemovedValue(long crdVer) {
        return (crdVer & RMVD_VAL_VER_MASK) != 0;
    }

    /**
     * @param crdVer Coordinator version with flags.
     * @return Coordinator version.
     */
    public static long unmaskCoordinatorVersion(long crdVer) {
        return crdVer & CRD_VER_MASK;
    }

    /**
     * @param topVer Topology version for cache operation.
     * @return Error.
     */
    public static IgniteCheckedException noCoordinatorError(AffinityTopologyVersion topVer) {
        return new ClusterTopologyServerNotFoundException("Mvcc coordinator is not assigned for " +
            "topology version: " + topVer);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        statCntrs = new StatCounter[7];

        statCntrs[0] = new CounterWithAvg("MvccTxCounterRequest", "avgTxs");
        statCntrs[1] = new CounterWithAvg("MvccVersionResponse", "avgFutTime");
        statCntrs[2] = new StatCounter("MvccAckRequestTx");
        statCntrs[3] = new CounterWithAvg("CoordinatorTxAckResponse", "avgFutTime");
        statCntrs[4] = new StatCounter("TotalRequests");
        statCntrs[5] = new StatCounter("MvccWaitTxsRequest");
        statCntrs[6] = new CounterWithAvg("CoordinatorWaitTxsResponse", "avgFutTime");

        ctx.event().addLocalEventListener(new CacheCoordinatorNodeFailListener(),
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(MSG_TOPIC, new CoordinatorMessageListener());
    }

    /** {@inheritDoc} */
    @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.CACHE_CRD_PROC;
    }

    /** {@inheritDoc} */
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

        // TODO: IGNITE-3478 handle inactive state.

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
     * @param log Logger.
     */
    public void dumpStatistics(IgniteLogger log) {
        if (STAT_CNTRS) {
            log.info("Mvcc coordinator statistics: ");

            for (StatCounter cntr : statCntrs)
                cntr.dumpInfo(log);
        }
    }

    /**
     * @param tx Transaction.
     * @return Counter.
     */
    public MvccVersion requestTxCounterOnCoordinator(IgniteInternalTx tx) {
        assert ctx.localNodeId().equals(currentCoordinatorId());

        return assignTxCounter(tx.nearXidVersion(), 0L);
    }

    /**
     * @param ver Version.
     * @return Counter.
     */
    public MvccVersion requestTxCounterOnCoordinator(GridCacheVersion ver) {
        assert ctx.localNodeId().equals(currentCoordinatorId());

        return assignTxCounter(ver, 0L);
    }

    /**
     * @param crd Coordinator.
     * @param lsnr Response listener.
     * @param txVer Transaction version.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccVersion> requestTxCounter(MvccCoordinator crd,
        MvccResponseListener lsnr,
        GridCacheVersion txVer) {
        assert !ctx.localNodeId().equals(crd.nodeId());

        MvccVersionFuture fut = new MvccVersionFuture(futIdCntr.incrementAndGet(), crd, lsnr);

        verFuts.put(fut.id, fut);

        try {
            ctx.io().sendToGridTopic(crd.nodeId(),
                MSG_TOPIC,
                new MvccTxCounterRequest(fut.id, txVer),
                MSG_POLICY);
        }
        catch (IgniteCheckedException e) {
            if (verFuts.remove(fut.id) != null)
                fut.onError(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param mvccVer Query version.
     */
    public void ackQueryDone(MvccCoordinator crd, MvccVersion mvccVer) {
        assert crd != null;

        long trackCntr = queryTrackCounter(mvccVer);

        Message msg = crd.coordinatorVersion() == mvccVer.coordinatorVersion() ? new MvccAckRequestQuery(trackCntr) :
            new MvccNewQueryAckRequest(mvccVer.coordinatorVersion(), trackCntr);

        try {
            ctx.io().sendToGridTopic(crd.nodeId(),
                MSG_TOPIC,
                msg,
                MSG_POLICY);
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
    private long queryTrackCounter(MvccVersion mvccVer) {
        long trackCntr = mvccVer.counter();

        MvccLongList txs = mvccVer.activeTransactions();

        int size = txs.size();

        for (int i = 0; i < size; i++) {
            long txId = txs.get(i);

            if (txId < trackCntr)
                trackCntr = txId;
        }

        return trackCntr;
    }

    /**
     * @param crd Coordinator.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccVersion> requestQueryCounter(MvccCoordinator crd) {
        assert crd != null;

        // TODO IGNITE-3478: special case for local?
        MvccVersionFuture fut = new MvccVersionFuture(futIdCntr.incrementAndGet(), crd, null);

        verFuts.put(fut.id, fut);

        try {
            ctx.io().sendToGridTopic(crd.nodeId(),
                MSG_TOPIC,
                new MvccQueryVersionRequest(fut.id),
                MSG_POLICY);
        }
        catch (IgniteCheckedException e) {
            if (verFuts.remove(fut.id) != null)
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

        // TODO IGNITE-3478: special case for local?
        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crdId, false);

        ackFuts.put(fut.id, fut);

        try {
            ctx.io().sendToGridTopic(crdId,
                MSG_TOPIC,
                new MvccWaitTxsRequest(fut.id, txs),
                MSG_POLICY);
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
        MvccVersion updateVer,
        @Nullable MvccVersion readVer) {
        assert crd != null;
        assert updateVer != null;

        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crd, true);

        ackFuts.put(fut.id, fut);

        MvccAckRequestTx msg = createTxAckMessage(fut.id, updateVer, readVer);

        try {
            ctx.io().sendToGridTopic(crd, MSG_TOPIC, msg, MSG_POLICY);
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
        MvccVersion updateVer,
        @Nullable MvccVersion readVer)
    {
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
    public void ackTxRollback(UUID crdId, MvccVersion updateVer, @Nullable MvccVersion readVer) {
        MvccAckRequestTx msg = createTxAckMessage(0, updateVer, readVer);

        msg.skipResponse(true);

        try {
            ctx.io().sendToGridTopic(crdId,
                MSG_TOPIC,
                msg,
                MSG_POLICY);
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
    private void processCoordinatorTxCounterRequest(UUID nodeId, MvccTxCounterRequest msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore tx counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccVersionResponse res = assignTxCounter(msg.txId(), msg.futureId());

        if (STAT_CNTRS)
            statCntrs[0].update(res.size());

        try {
            ctx.io().sendToGridTopic(node,
                MSG_TOPIC,
                res,
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx counter response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx counter response [msg=" + msg + ", node=" + nodeId + ']', e);
        }
    }

    /**
     *
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorQueryVersionRequest(UUID nodeId, MvccQueryVersionRequest msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore query counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccVersionResponse res = assignQueryCounter(nodeId, msg.futureId());

        try {
            ctx.io().sendToGridTopic(node,
                MSG_TOPIC,
                res,
                MSG_POLICY);
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
    private void processCoordinatorVersionResponse(UUID nodeId, MvccVersionResponse msg) {
        MvccVersionFuture fut = verFuts.remove(msg.futureId());

        if (fut != null) {
            if (STAT_CNTRS)
                statCntrs[1].update((System.nanoTime() - fut.startTime) * 1000);

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
        onTxDone(msg.txCounter());

        if (msg.queryCounter() != MVCC_COUNTER_NA) {
            if (msg.queryCoordinatorVersion() == 0)
                onQueryDone(nodeId, msg.queryCounter());
            else
                prevCrdQueries.onQueryDone(nodeId, msg.queryCoordinatorVersion(), msg.queryCounter());
        }

        if (STAT_CNTRS)
            statCntrs[2].update();

        if (!msg.skipResponse()) {
            try {
                ctx.io().sendToGridTopic(nodeId,
                    MSG_TOPIC,
                    new MvccFutureResponse(msg.futureId()),
                    MSG_POLICY);
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
            if (STAT_CNTRS) {
                StatCounter cntr = fut.ackTx ? statCntrs[3] : statCntrs[6];

                cntr.update((System.nanoTime() - fut.startTime) * 1000);
            }

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
     * @param txId Transaction ID.
     * @return Counter.
     */
    private MvccVersionResponse assignTxCounter(GridCacheVersion txId, long futId) {
        assert crdVer != 0;

        long nextCtr = mvccCntr.incrementAndGet();

        // TODO IGNITE-3478 sorted? + change GridLongList.writeTo?
        MvccVersionResponse res = new MvccVersionResponse();

        long minActive = Long.MAX_VALUE;

        for (Long txVer : activeTxs.keySet()) {
            if (txVer < minActive)
                minActive = txVer;

            res.addTx(txVer);
        }

        Object old = activeTxs.put(nextCtr, txId);

        assert old == null : txId;

        long cleanupVer;

        if (prevCrdQueries.previousQueriesDone()) {
            cleanupVer = Math.min(minActive, committedCntr.get());

            cleanupVer--;

            Long qryVer = activeQueries.minimalQueryCounter();

            if (qryVer != null && qryVer <= cleanupVer)
                cleanupVer = qryVer - 1;
        }
        else
            cleanupVer = -1;

        res.init(futId, crdVer, nextCtr, cleanupVer);

        return res;
    }

    /**
     * @param txCntr Counter assigned to transaction.
     */
    private void onTxDone(Long txCntr) {
        GridFutureAdapter fut; // TODO IGNITE-3478.

        GridCacheVersion ver = activeTxs.remove(txCntr);

        assert ver != null;

        committedCntr.setIfGreater(txCntr);

        fut = waitTxFuts.remove(txCntr);

        if (fut != null)
            fut.onDone();
    }

    /**
     *
     */
    class ActiveQueries {
        /** */
        private final Map<UUID, TreeMap<Long, AtomicInteger>> activeQueries = new HashMap<>();

        /** */
        private Long minQry;

        Long minimalQueryCounter() {
            synchronized (this) {
                return minQry;
            }
        }

        synchronized MvccVersionResponse assignQueryCounter(UUID nodeId, long futId) {
            MvccVersionResponse res = new MvccVersionResponse();

            Long mvccCntr;
            Long trackCntr;

            for(;;) {
                mvccCntr = committedCntr.get();

                trackCntr = mvccCntr;

                for (Long txVer : activeTxs.keySet()) {
                    if (txVer < trackCntr)
                        trackCntr = txVer;

                    res.addTx(txVer);
                }

                Long minQry0 = minQry;

                if (minQry == null || trackCntr < minQry)
                    minQry = trackCntr;

                if (committedCntr.get() == mvccCntr)
                    break;

                minQry = minQry0;

                res.resetTransactionsCount();
            }

            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null)
                activeQueries.put(nodeId, nodeMap = new TreeMap<>());

            AtomicInteger qryCnt = nodeMap.get(trackCntr);

            if (qryCnt == null)
                nodeMap.put(trackCntr, new AtomicInteger(1));
            else
                qryCnt.incrementAndGet();

            res.init(futId, crdVer, mvccCntr, MVCC_COUNTER_NA);

            return res;
        }

        synchronized void onQueryDone(UUID nodeId, Long mvccCntr) {
            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null)
                return;

            assert minQry != null;

            AtomicInteger qryCnt = nodeMap.get(mvccCntr);

            assert qryCnt != null : "[node=" + nodeId + ", nodeMap=" + nodeMap + ", cntr=" + mvccCntr + "]";

            int left = qryCnt.decrementAndGet();

            if (left == 0) {
                nodeMap.remove(mvccCntr);

                if (mvccCntr == minQry.longValue())
                    minQry = activeMinimal();
            }
        }

        synchronized void onNodeFailed(UUID nodeId) {
            activeQueries.remove(nodeId);

            minQry = activeMinimal();
        }

        private Long activeMinimal() {
            Long min = null;

            for (TreeMap<Long, AtomicInteger> m : activeQueries.values()) {
                Map.Entry<Long, AtomicInteger> e = m.firstEntry();

                if (e != null && (min == null || e.getKey() < min))
                    min = e.getKey();
            }

            return min;
        }
    }

    /**
     * @param qryNodeId Node initiated query.
     * @return Counter for query.
     */
    private MvccVersionResponse assignQueryCounter(UUID qryNodeId, long futId) {
        assert crdVer != 0;

        MvccVersionResponse res = activeQueries.assignQueryCounter(qryNodeId, futId);

        return res;

//        MvccVersionResponse res = new MvccVersionResponse();
//
//        Long mvccCntr;
//
//        for(;;) {
//            mvccCntr = committedCntr.get();
//
//            Long trackCntr = mvccCntr;
//
//            for (Long txVer : activeTxs.keySet()) {
//                if (txVer < trackCntr)
//                    trackCntr = txVer;
//
//                res.addTx(txVer);
//            }
//
//            registerActiveQuery(trackCntr);
//
//            if (committedCntr.get() == mvccCntr)
//                break;
//            else {
//                res.resetTransactionsCount();
//
//                onQueryDone(trackCntr);
//            }
//        }
//
//        res.init(futId, crdVer, mvccCntr, MVCC_COUNTER_NA);
//
//        return res;
    }
//
//    private void registerActiveQuery(Long mvccCntr) {
//        for (;;) {
//            AtomicInteger qryCnt = activeQueries.get(mvccCntr);
//
//            if (qryCnt != null) {
//                boolean inc = increment(qryCnt);
//
//                if (!inc) {
//                    activeQueries.remove(mvccCntr, qryCnt);
//
//                    continue;
//                }
//            }
//            else {
//                qryCnt = new AtomicInteger(1);
//
//                if (activeQueries.putIfAbsent(mvccCntr, qryCnt) != null)
//                    continue;
//            }
//
//            break;
//        }
//    }
//
//    static boolean increment(AtomicInteger cntr) {
//        for (;;) {
//            int current = cntr.get();
//
//            if (current == 0)
//                return false;
//
//            if (cntr.compareAndSet(current, current + 1))
//                return true;
//        }
//    }

    /**
     * @param mvccCntr Query counter.
     */
    private void onQueryDone(UUID nodeId, Long mvccCntr) {
        activeQueries.onQueryDone(nodeId, mvccCntr);
//        AtomicInteger qryCnt = activeQueries.get(mvccCntr);
//
//        assert qryCnt != null : mvccCntr;
//
//        int left = qryCnt.decrementAndGet();
//
//        assert left >= 0 : left;
//
//        if (left == 0)
//            activeQueries.remove(mvccCntr, qryCnt);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processCoordinatorWaitTxsRequest(final UUID nodeId, final MvccWaitTxsRequest msg) {
        statCntrs[5].update();

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

            if (!activeTxs.containsKey(txId))
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
            ctx.io().sendToGridTopic(nodeId,
                MSG_TOPIC,
                new MvccFutureResponse(msg.futureId()),
                MSG_POLICY);
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
        @Nullable Map<MvccCounter, Integer> activeQueries) {
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
    public void sendActiveQueries(UUID nodeId, @Nullable Map<MvccCounter, Integer> activeQueries) {
        MvccActiveQueriesMessage msg = new MvccActiveQueriesMessage(activeQueries);

        try {
            ctx.io().sendToGridTopic(nodeId,
                MSG_TOPIC,
                msg,
                MSG_POLICY);
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
        Map<UUID, Map<MvccCounter, Integer>> activeQueries)
    {
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
     * @param log Logger.
     * @param diagCtx Diagnostic request.
     */
    public void dumpDebugInfo(IgniteLogger log, @Nullable IgniteDiagnosticPrepareContext diagCtx) {
        boolean first = true;

        for (MvccVersionFuture verFur : verFuts.values()) {
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
     *
     */
    private class MvccVersionFuture extends GridFutureAdapter<MvccVersion> implements MvccFuture {
        /** */
        private final Long id;

        /** */
        private MvccResponseListener lsnr;

        /** */
        public final MvccCoordinator crd;

        /** */
        long startTime;

        /**
         * @param id Future ID.
         * @param crd Mvcc coordinator.
         * @param lsnr Listener.
         */
        MvccVersionFuture(Long id, MvccCoordinator crd, @Nullable MvccResponseListener lsnr) {
            this.id = id;
            this.crd = crd;
            this.lsnr = lsnr;

            if (STAT_CNTRS)
                startTime = System.nanoTime();
        }

        /** {@inheritDoc} */
        @Override public UUID coordinatorNodeId() {
            return crd.nodeId();
        }

        /**
         * @param res Response.
         */
        void onResponse(MvccVersionResponse res) {
            assert res.counter() != MVCC_COUNTER_NA;

            if (lsnr != null)
                lsnr.onMvccResponse(crd.nodeId(), res);

            onDone(res);
        }

        /**
         * @param err Error.
         */
        void onError(IgniteCheckedException err) {
            if (lsnr != null)
                lsnr.onMvccError(err);

            onDone(err);
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId ) {
            if (crd.nodeId().equals(nodeId) && verFuts.remove(id) != null) {
                ClusterTopologyCheckedException err = new ClusterTopologyCheckedException("Failed to request mvcc " +
                    "version, coordinator failed: " + nodeId);

                onError(err);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MvccVersionFuture [crd=" + crd.nodeId() + ", id=" + id + ']';
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
        long startTime;

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

            if (STAT_CNTRS)
                startTime = System.nanoTime();
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

            for (MvccVersionFuture fut : verFuts.values())
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
            if (STAT_CNTRS)
                statCntrs[4].update();

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

            if (msg instanceof MvccTxCounterRequest)
                processCoordinatorTxCounterRequest(nodeId, (MvccTxCounterRequest)msg);
            else if (msg instanceof MvccAckRequestTx)
                processCoordinatorTxAckRequest(nodeId, (MvccAckRequestTx)msg);
            else if (msg instanceof MvccFutureResponse)
                processCoordinatorAckResponse(nodeId, (MvccFutureResponse)msg);
            else if (msg instanceof MvccAckRequestQuery)
                processCoordinatorQueryAckRequest(nodeId, (MvccAckRequestQuery)msg);
            else if (msg instanceof MvccQueryVersionRequest)
                processCoordinatorQueryVersionRequest(nodeId, (MvccQueryVersionRequest)msg);
            else if (msg instanceof MvccVersionResponse)
                processCoordinatorVersionResponse(nodeId, (MvccVersionResponse) msg);
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
    /**
     *
     */
    static class StatCounter {
        /** */
        final String name;

        /** */
        final LongAdder8 cntr = new LongAdder8();

        public StatCounter(String name) {
            this.name = name;
        }

        void update() {
            cntr.increment();
        }

        void update(GridLongList arg) {
            throw new UnsupportedOperationException();
        }

        void update(long arg) {
            throw new UnsupportedOperationException();
        }

        void dumpInfo(IgniteLogger log) {
            long totalCnt = cntr.sumThenReset();

            if (totalCnt > 0)
                log.info(name + " [cnt=" + totalCnt + ']');
        }
    }

    /**
     *
     */
    static class CounterWithAvg extends StatCounter {
        /** */
        final LongAdder8 total = new LongAdder8();

        /** */
        final String avgName;

        CounterWithAvg(String name, String avgName) {
            super(name);

            this.avgName = avgName;
        }

        @Override void update(GridLongList arg) {
            update(arg != null ? arg.size() : 0);
        }

        @Override void update(long add) {
            cntr.increment();

            total.add(add);
        }

        void dumpInfo(IgniteLogger log) {
            long totalCnt = cntr.sumThenReset();
            long totalSum = total.sumThenReset();

            if (totalCnt > 0)
                log.info(name + " [cnt=" + totalCnt + ", " + avgName + "=" + ((float)totalSum / totalCnt) + ']');
        }
    }

    /**
     *
     */
    private static class WaitTxFuture extends GridFutureAdapter {
        /** */
        private final long txId;

        /**
         * @param txId Transaction ID.
         */
        WaitTxFuture(long txId) {
            this.txId = txId;
        }
    }
}
