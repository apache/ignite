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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 *
 */
public class CacheCoordinatorsSharedManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    public static final long COUNTER_NA = 0L;

    /** */
    private static final GridTopic MSG_TOPIC = TOPIC_CACHE_COORDINATOR;

    /** */
    private static final byte MSG_POLICY = SYSTEM_POOL;
    
    /** */
    private final CoordinatorAssignmentHistory assignHist = new CoordinatorAssignmentHistory();

    /** */
    private final AtomicLong mvccCntr = new AtomicLong(1L);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(1L);

    /** */
    private final ConcurrentHashMap<GridCacheVersion, Long> activeTxs = new ConcurrentHashMap<>();

    /** */
    private final Map<Long, Integer> activeQueries = new HashMap<>();

    /** */
    private final ConcurrentMap<Long, MvccVersionFuture> verFuts = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Long, WaitAckFuture> ackFuts = new ConcurrentHashMap<>();

    /** */
    private final AtomicLong futIdCntr = new AtomicLong();

    /** */
    private final CountDownLatch crdLatch = new CountDownLatch(1);

    /** Topology version when local node was assigned as coordinator. */
    private long crdVer;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        cctx.gridEvents().addLocalEventListener(new CacheCoordinatorDiscoveryListener(),
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        cctx.gridIO().addMessageListener(MSG_TOPIC, new CoordinatorMessageListener());
    }

    /**
     * @param tx Transaction.
     * @return Counter.
     */
    public MvccCoordinatorVersion requestTxCounterOnCoordinator(IgniteInternalTx tx) {
        assert cctx.localNode().equals(assignHist.currentCoordinator());

        return assignTxCounter(tx.nearXidVersion(), 0L);
    }

    /**
     * @param crd Coordinator.
     * @param tx Transaction.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccCoordinatorVersion> requestTxCounter(ClusterNode crd, GridDhtTxLocalAdapter tx) {
        assert !crd.isLocal() : crd;

        MvccVersionFuture fut = new MvccVersionFuture(futIdCntr.incrementAndGet(),
            crd,
            tx);

        verFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorTxCounterRequest(fut.id, tx.nearXidVersion()),
                MSG_POLICY);
        }
        catch (IgniteCheckedException e) {
            if (verFuts.remove(fut.id) != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param cntr Counter assigned to query.
     */
    public void ackQueryDone(ClusterNode crd, long cntr) {
        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorQueryAckRequest(cntr),
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query ack, node left [crd=" + crd + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query ack [crd=" + crd + ", cntr=" + cntr + ']', e);
        }
    }

    /**
     * @param crd Coordinator.
     * @return Counter request future.
     */
    public IgniteInternalFuture<MvccCoordinatorVersion> requestQueryCounter(ClusterNode crd) {
        assert crd != null;

        // TODO IGNITE-3478: special case for local?
        MvccVersionFuture fut = new MvccVersionFuture(futIdCntr.incrementAndGet(), crd, null);

        verFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorQueryVersionRequest(fut.id),
                MSG_POLICY);
        }
        catch (IgniteCheckedException e) {
            if (verFuts.remove(fut.id) != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param txs Transaction IDs.
     * @return Future.
     */
    public IgniteInternalFuture<Void> waitTxsFuture(ClusterNode crd, GridLongList txs) {
        assert crd != null;
        assert txs != null && txs.size() > 0;

        // TODO IGNITE-3478: special case for local?

        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crd);

        ackFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorWaitTxsRequest(fut.id, txs),
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(); // No need to ack, finish without error.
        }
        catch (IgniteCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param txId Transaction ID.
     * @return Acknowledge future.
     */
    public IgniteInternalFuture<Void> ackTxCommit(ClusterNode crd, GridCacheVersion txId) {
        assert crd != null;
        assert txId != null;

        WaitAckFuture fut = new WaitAckFuture(futIdCntr.incrementAndGet(), crd);

        ackFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                new CoordinatorTxAckRequest(fut.id, txId),
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(); // No need to ack, finish without error.
        }
        catch (IgniteCheckedException e) {
            if (ackFuts.remove(fut.id) != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * @param crd Coordinator.
     * @param txId Transaction ID.
     */
    public void ackTxRollback(ClusterNode crd, GridCacheVersion txId) {
        CoordinatorTxAckRequest msg = new CoordinatorTxAckRequest(0, txId);

        msg.skipResponse(true);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                MSG_TOPIC,
                msg,
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx rollback ack, node left [msg=" + msg + ", node=" + crd.id() + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx rollback ack [msg=" + msg + ", node=" + crd.id() + ']', e);
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxCounterRequest(UUID nodeId, CoordinatorTxCounterRequest msg) {
        ClusterNode node = cctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore tx counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccCoordinatorVersionResponse res = assignTxCounter(msg.txId(), msg.futureId());

        try {
            cctx.gridIO().sendToGridTopic(node,
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
    private void processCoordinatorQueryVersionRequest(UUID nodeId, CoordinatorQueryVersionRequest msg) {
        ClusterNode node = cctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore query counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccCoordinatorVersionResponse res = assignQueryCounter(nodeId, msg.futureId());

        try {
            cctx.gridIO().sendToGridTopic(node,
                MSG_TOPIC,
                res,
                MSG_POLICY);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query counter response, node left [msg=" + msg + ", node=" + nodeId + ']');

            onQueryDone(res.counter());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query counter response [msg=" + msg + ", node=" + nodeId + ']', e);

            onQueryDone(res.counter());
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorQueryVersionResponse(UUID nodeId, MvccCoordinatorVersionResponse msg) {
        MvccVersionFuture fut = verFuts.remove(msg.futureId());

        if (fut != null)
            fut.onResponse(msg);
        else {
            if (cctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param msg Message.
     */
    private void processCoordinatorQueryAckRequest(CoordinatorQueryAckRequest msg) {
        onQueryDone(msg.counter());
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxAckRequest(UUID nodeId, CoordinatorTxAckRequest msg) {
        onTxDone(msg.txId());

        if (!msg.skipResponse()) {
            try {
                cctx.gridIO().sendToGridTopic(nodeId,
                    MSG_TOPIC,
                    new CoordinatorFutureResponse(msg.futureId()),
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
    private void processCoordinatorAckResponse(UUID nodeId, CoordinatorFutureResponse msg) {
        WaitAckFuture fut = ackFuts.remove(msg.futureId());

        if (fut != null)
            fut.onResponse();
        else {
            if (cctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param txId Transaction ID.
     * @return Counter.
     */
    private synchronized MvccCoordinatorVersionResponse assignTxCounter(GridCacheVersion txId, long futId) {
        assert crdVer != 0;

        long nextCtr = mvccCntr.incrementAndGet();

        // TODO IGNITE-3478 sorted? + change GridLongList.writeTo?
        GridLongList txs = null;

        for (Long txVer : activeTxs.values()) {
            if (txs == null)
                txs = new GridLongList();

            txs.add(txVer);
        }

        Object old = activeTxs.put(txId, nextCtr);

        assert old == null : txId;

        long cleanupVer = Long.MAX_VALUE;

        for (Long qryVer : activeQueries.keySet()) {
            if (qryVer < cleanupVer)
                cleanupVer = qryVer - 1;
        }

        return new MvccCoordinatorVersionResponse(futId, crdVer, nextCtr, txs, cleanupVer);
    }

    /**
     * @param txId Transaction ID.
     */
    private void onTxDone(GridCacheVersion txId) {
        GridFutureAdapter fut; // TODO IGNITE-3478.

        synchronized (this) {
            Long cntr = activeTxs.remove(txId);

            assert cntr != null;

            committedCntr.setIfGreater(cntr);

            fut = waitTxFuts.remove(cntr);
        }

        if (fut != null)
            fut.onDone();
    }

    /**
     * @param qryNodeId Node initiated query.
     * @return Counter for query.
     */
    private synchronized MvccCoordinatorVersionResponse assignQueryCounter(UUID qryNodeId, long futId) {
        assert crdVer != 0;

        Long mvccCntr = committedCntr.get();

        GridLongList txs = null;

        for (Long txVer : activeTxs.values()) {
            if (txs == null)
                txs = new GridLongList();

            txs.add(txVer);
        }

        Integer queries = activeQueries.get(mvccCntr);

        if (queries != null)
            activeQueries.put(mvccCntr, queries + 1);
        else
            activeQueries.put(mvccCntr, 1);

        return new MvccCoordinatorVersionResponse(futId, crdVer, mvccCntr, txs, COUNTER_NA);
    }

    /**
     * @param mvccCntr Query counter.
     */
    private synchronized void onQueryDone(long mvccCntr) {
        Integer queries = activeQueries.get(mvccCntr);

        assert queries != null : mvccCntr;

        int left = queries - 1;

        assert left >= 0 : left;

        if (left == 0) {
            Integer rmvd = activeQueries.remove(mvccCntr);

            assert rmvd != null;
        }
    }

    /** */
    private Map<Long, GridFutureAdapter> waitTxFuts = new HashMap<>(); // TODO IGNITE-3478.

    /**
     * @param msg Message.
     */
    private void processCoordinatorWaitTxsRequest(final UUID nodeId, final CoordinatorWaitTxsRequest msg) {
        GridLongList txs = msg.transactions();

        // TODO IGNITE-3478.
        GridCompoundFuture fut = null;

        synchronized (this) {
            for (int i = 0; i < txs.size(); i++) {
                long txId = txs.get(i);

                if (hasActiveTx(txId)) {
                    GridFutureAdapter fut0 = waitTxFuts.get(txId);

                    if (fut0 == null) {
                        fut0 = new GridFutureAdapter();

                        waitTxFuts.put(txId, fut0);
                    }

                    if (fut == null)
                        fut = new GridCompoundFuture();

                    fut.add(fut0);
                }
            }
        }

        if (fut != null)
            fut.markInitialized();

        if (fut == null || fut.isDone())
            sendFutureResponse(nodeId, msg);
        else {
            fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    sendFutureResponse(nodeId, msg);
                }
            });
        }
    }

    /**
     * @param nodeId
     * @param msg
     */
    private void sendFutureResponse(UUID nodeId, CoordinatorWaitTxsRequest msg) {
        try {
            cctx.gridIO().sendToGridTopic(nodeId,
                MSG_TOPIC,
                new CoordinatorFutureResponse(msg.futureId()),
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

    private boolean hasActiveTx(long txId) {
        for (Long id : activeTxs.values()) {
            if (id == txId)
                return true;
        }

        return false;
    }

    /**
     * @param topVer Topology version.
     * @return MVCC coordinator for given topology version.
     */
    @Nullable public ClusterNode coordinator(AffinityTopologyVersion topVer) {
        return assignHist.coordinator(topVer);
    }

    /**
     * @param discoCache Discovery snapshot.
     */
    public void assignCoordinator(DiscoCache discoCache) {
        ClusterNode curCrd = assignHist.currentCoordinator();

        if (curCrd == null || !discoCache.allNodes().contains(curCrd)) {
            ClusterNode newCrd = null;

            if (!discoCache.serverNodes().isEmpty())
                newCrd = discoCache.serverNodes().get(0);

            if (!F.eq(curCrd, newCrd)) {
                assignHist.addAssignment(discoCache.version(), newCrd);

                if (cctx.localNode().equals(newCrd)) {
                    crdVer = discoCache.version().topologyVersion();

                    crdLatch.countDown();
                }

                log.info("Assigned mvcc coordinator [topVer=" + discoCache.version() +
                    ", crd=" + newCrd + ']');

                return;
            }
        }

        assignHist.addAssignment(discoCache.version(), curCrd);
    }

    /**
     *
     */
    public class MvccVersionFuture extends GridFutureAdapter<MvccCoordinatorVersion> {
        /** */
        private final Long id;

        /** */
        private GridDhtTxLocalAdapter tx;

        /** */
        public final ClusterNode crd;

        /**
         * @param id Future ID.
         * @param crd Coordinator.
         */
        MvccVersionFuture(Long id, ClusterNode crd, @Nullable GridDhtTxLocalAdapter tx) {
            this.id = id;
            this.crd = crd;
            this.tx = tx;
        }

        /**
         * @param res Response.
         */
        void onResponse(MvccCoordinatorVersionResponse res) {
            assert res.counter() != COUNTER_NA;

            if (tx != null)
                tx.mvccCoordinatorVersion(res);

            onDone(res);
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crd.id().equals(nodeId) && verFuts.remove(id) != null) {
                onDone(new ClusterTopologyCheckedException("Failed to request coordinator version, " +
                    "coordinator failed: " + nodeId));
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MvccVersionFuture [crd=" + crd + ", id=" + id + ']';
        }
    }

    /**
     *
     */
    private class WaitAckFuture extends GridFutureAdapter<Void> {
        /** */
        private final long id;

        /** */
        private final ClusterNode crd;

        /**
         * @param id Future ID.
         * @param crd Coordinator.
         */
        WaitAckFuture(long id, ClusterNode crd) {
            this.id = id;
            this.crd = crd;
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
            if (crd.id().equals(nodeId) && verFuts.remove(id) != null)
                onDone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "WaitAckFuture [crd=" + crd + ", id=" + id + ']';
        }
    }

    /**
     *
     */
    private class CacheCoordinatorDiscoveryListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            assert evt instanceof DiscoveryEvent : evt;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            UUID nodeId = discoEvt.eventNode().id();

            for (MvccVersionFuture fut : verFuts.values())
                fut.onNodeLeft(nodeId);

            for (WaitAckFuture fut : ackFuts.values())
                fut.onNodeLeft(nodeId);
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
            MvccCoordinatorMessage msg0 = (MvccCoordinatorMessage)msg;

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

            if (msg instanceof CoordinatorTxCounterRequest)
                processCoordinatorTxCounterRequest(nodeId, (CoordinatorTxCounterRequest)msg);
            else if (msg instanceof CoordinatorTxAckRequest)
                processCoordinatorTxAckRequest(nodeId, (CoordinatorTxAckRequest)msg);
            else if (msg instanceof CoordinatorFutureResponse)
                processCoordinatorAckResponse(nodeId, (CoordinatorFutureResponse)msg);
            else if (msg instanceof CoordinatorQueryAckRequest)
                processCoordinatorQueryAckRequest((CoordinatorQueryAckRequest)msg);
            else if (msg instanceof CoordinatorQueryVersionRequest)
                processCoordinatorQueryVersionRequest(nodeId, (CoordinatorQueryVersionRequest)msg);
            else if (msg instanceof MvccCoordinatorVersionResponse)
                processCoordinatorQueryVersionResponse(nodeId, (MvccCoordinatorVersionResponse) msg);
            else if (msg instanceof CoordinatorWaitTxsRequest)
                processCoordinatorWaitTxsRequest(nodeId, (CoordinatorWaitTxsRequest)msg);
            else
                U.warn(log, "Unexpected message received [node=" + nodeId + ", msg=" + msg + ']');
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CoordinatorMessageListener[]";
        }
    }
}
