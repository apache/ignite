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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
    private final CoordinatorAssignmentHistory assignHist = new CoordinatorAssignmentHistory();

    /** */
    private final AtomicLong mvccCntr = new AtomicLong(1L);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(1L);

    /** */
    private final ConcurrentHashMap<GridCacheVersion, MvccUpdateVersion> activeTxs = new ConcurrentHashMap<>();

    /** */
    private final Map<Long, Integer> activeQueries = new HashMap<>();

    /** */
    private final ConcurrentMap<Long, TxCounterFuture> cntrFuts = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Long, QueryVersionFuture> qryVerFuts = new ConcurrentHashMap<>();


    /** */
    private final ConcurrentMap<Long, TxAckFuture> ackFuts = new ConcurrentHashMap<>();

    /** */
    private final AtomicLong futIdCntr = new AtomicLong();

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        cctx.gridEvents().addLocalEventListener(new CacheCoordinatorDiscoveryListener(),
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        cctx.gridIO().addMessageListener(TOPIC_CACHE_COORDINATOR, new CoordinatorMessageListener());
    }

    /**
     * @param tx Transaction.
     * @return Counter.
     */
    public long requestTxCounterOnCoordinator(IgniteInternalTx tx) {
        assert cctx.localNode().equals(assignHist.currentCoordinator());

        AffinityTopologyVersion txTopVer = tx.topologyVersionSnapshot();

        assert txTopVer != null && txTopVer.initialized() : txTopVer;

        return assignTxCounter(tx.nearXidVersion(), txTopVer.topologyVersion());
    }

    /**
     * @param crd Coordinator.
     * @param tx Transaction.
     * @return Counter request future.
     */
    public IgniteInternalFuture<Long> requestTxCounter(ClusterNode crd, IgniteInternalTx tx) {
        assert !crd.isLocal() : crd;

        AffinityTopologyVersion txTopVer = tx.topologyVersionSnapshot();

        assert txTopVer != null && txTopVer.initialized() : txTopVer;

        TxCounterFuture fut = new TxCounterFuture(futIdCntr.incrementAndGet(), crd, tx);

        cntrFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                TOPIC_CACHE_COORDINATOR,
                new CoordinatorTxCounterRequest(fut.id, tx.nearXidVersion(), txTopVer.topologyVersion()),
                SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            if (cntrFuts.remove(fut.id) != null)
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
                TOPIC_CACHE_COORDINATOR,
                new CoordinatorQueryAckRequest(cntr),
                SYSTEM_POOL);
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
    public IgniteInternalFuture<MvccQueryVersion> requestQueryCounter(ClusterNode crd, long topVer) {
        QueryVersionFuture fut = new QueryVersionFuture(futIdCntr.incrementAndGet(), topVer, crd);

        qryVerFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                TOPIC_CACHE_COORDINATOR,
                new CoordinatorQueryVersionRequest(fut.id),
                SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            if (cntrFuts.remove(fut.id) != null)
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
        TxAckFuture fut = new TxAckFuture(futIdCntr.incrementAndGet(), crd);

        ackFuts.put(fut.id, fut);

        try {
            cctx.gridIO().sendToGridTopic(crd,
                TOPIC_CACHE_COORDINATOR,
                new CoordinatorTxAckRequest(fut.id, txId),
                SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException e) {
            if (cntrFuts.remove(fut.id) != null)
                fut.onDone();
        }
        catch (IgniteCheckedException e) {
            if (cntrFuts.remove(fut.id) != null)
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
                TOPIC_CACHE_COORDINATOR,
                msg,
                SYSTEM_POOL);
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

        long nextCtr = assignTxCounter(msg.txId(), msg.topologyVersion());

        try {
            cctx.gridIO().sendToGridTopic(node,
                TOPIC_CACHE_COORDINATOR,
                new CoordinatorTxCounterResponse(nextCtr, msg.futureId()),
                SYSTEM_POOL);
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
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorCounterResponse(UUID nodeId, CoordinatorTxCounterResponse msg) {
        TxCounterFuture fut = cntrFuts.remove(msg.futureId());

        if (fut != null)
            fut.onResponse(msg.counter());
        else {
            if (cctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find coordinator counter future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find coordinator counter future [node=" + nodeId + ", msg=" + msg + ']');
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

        CoordinatorQueryVersionResponse res = assignQueryCounter(nodeId, msg.futureId());

        try {
            cctx.gridIO().sendToGridTopic(node,
                TOPIC_CACHE_COORDINATOR,
                res,
                SYSTEM_POOL);
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
    private void processCoordinatorQueryVersionResponse(UUID nodeId, CoordinatorQueryVersionResponse msg) {
        QueryVersionFuture fut = qryVerFuts.remove(msg.futureId());

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
                    TOPIC_CACHE_COORDINATOR,
                    new CoordinatorTxAckResponse(msg.futureId()),
                    SYSTEM_POOL);
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
    private void processCoordinatorTxAckResponse(UUID nodeId, CoordinatorTxAckResponse msg) {
        TxAckFuture fut = ackFuts.remove(msg.futureId());

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
     * @param topVer Topology version.
     * @return Counter.
     */
    private synchronized long assignTxCounter(GridCacheVersion txId, long topVer) {
        long nextCtr = mvccCntr.incrementAndGet();

        MvccUpdateVersion ver = new MvccUpdateVersion(topVer, nextCtr);

        Object old = activeTxs.put(txId, ver);

        assert old == null : txId;

        return nextCtr;
    }

    /**
     * @param txId Transaction ID.
     */
    private synchronized void onTxDone(GridCacheVersion txId) {
        MvccUpdateVersion ver = activeTxs.remove(txId);

        assert ver != null;

        committedCntr.setIfGreater(ver.counter());
    }

    /**
     * @param qryNodeId Node initiated query.
     * @return Counter for query.
     */
    private synchronized CoordinatorQueryVersionResponse assignQueryCounter(UUID qryNodeId, long futId) {
        Long mvccCntr = committedCntr.get();

        List<MvccUpdateVersion> txs = null;

        for (MvccUpdateVersion txVer : activeTxs.values()) {
            if (txs == null)
                txs = new ArrayList<>();

            txs.add(txVer);
        }

        Integer queries = activeQueries.get(mvccCntr);

        if (queries != null)
            activeQueries.put(mvccCntr, queries + 1);
        else
            activeQueries.put(mvccCntr, 1);

        return new CoordinatorQueryVersionResponse(futId, mvccCntr, txs);
    }

    /**
     * @param mvccCntr Query counter.
     */
    private synchronized void onQueryDone(long mvccCntr) {
        Integer queries = activeQueries.get(mvccCntr);

        assert queries != null : mvccCntr;

        int left = queries - 1;

        assert left >= 0 : left;

        if (left == 0)
            activeQueries.remove(mvccCntr);
    }

    private synchronized long cleanupVersion() {
        long cntr = committedCntr.get();

        Long minActive = minActiveTx();

        if (minActive != null && minActive < cntr)
            cntr = minActive - 1;

        for (Long qryCntr : activeQueries.keySet()) {
            if (qryCntr <= cntr)
                cntr = qryCntr - 1;
        }

        return cntr;
    }

    @Nullable private Long minActiveTx() {
        Long min = null;

        for (Map.Entry<GridCacheVersion, MvccUpdateVersion> e : activeTxs.entrySet()) {
            if (min == null || e.getValue().counter() < min)
                min = e.getValue().counter();
        }

        return min;
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
    public class QueryVersionFuture extends GridFutureAdapter<MvccQueryVersion> {
        /** */
        private final Long id;

        /** */
        private long topVer;

        /** */
        public final ClusterNode crd;

        /**
         * @param id Future ID.
         * @param topVer Topology version.
         * @param crd Coordinator.
         */
        QueryVersionFuture(Long id, long topVer, ClusterNode crd) {
            this.id = id;
            this.topVer = topVer;
            this.crd = crd;
        }

        /**
         * @param res Response.
         */
        void onResponse(CoordinatorQueryVersionResponse res) {
            assert res.counter() != MvccUpdateVersion.COUNTER_NA;

            res.topologyVersion(topVer);

            onDone(res);
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crd.id().equals(nodeId) && cntrFuts.remove(id) != null)
                onDone(new ClusterTopologyCheckedException("Failed to request query version, coordinator failed: " + nodeId));
        }
    }

    /**
     *
     */
    public class TxCounterFuture extends GridFutureAdapter<Long> {
        /** */
        private final Long id;

        /** */
        private IgniteInternalTx tx;

        /** */
        public final ClusterNode crd;

        /**
         * @param id Future ID.
         * @param crd Coordinator.
         */
        TxCounterFuture(Long id, ClusterNode crd, IgniteInternalTx tx) {
            this.id = id;
            this.crd = crd;
            this.tx = tx;
        }

        /**
         * @param cntr Counter.
         */
        void onResponse(long cntr) {
            assert cntr != MvccUpdateVersion.COUNTER_NA;

            if (tx != null)
                tx.mvccCoordinatorCounter(cntr);

            onDone(cntr);
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crd.id().equals(nodeId) && cntrFuts.remove(id) != null)
                onDone(new ClusterTopologyCheckedException("Failed to request counter, coordinator failed: " + nodeId));
        }
    }

    /**
     *
     */
    private class TxAckFuture extends GridFutureAdapter<Void> {
        /** */
        private final long id;

        /** */
        private final ClusterNode crd;

        /**
         * @param id Future ID.
         * @param crd Coordinator.
         */
        TxAckFuture(long id, ClusterNode crd) {
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
            if (crd.id().equals(nodeId) && cntrFuts.remove(id) != null)
                onDone();
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

            for (TxCounterFuture fut : cntrFuts.values())
                fut.onNodeLeft(nodeId);

            for (QueryVersionFuture fut : qryVerFuts.values())
                fut.onNodeLeft(nodeId);

            for (TxAckFuture fut : ackFuts.values())
                fut.onNodeLeft(nodeId);
        }
    }
    /**
     *
     */
    private class CoordinatorMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (msg instanceof CoordinatorTxCounterRequest)
                processCoordinatorTxCounterRequest(nodeId, (CoordinatorTxCounterRequest)msg);
            else if (msg instanceof CoordinatorTxCounterResponse)
                processCoordinatorCounterResponse(nodeId, (CoordinatorTxCounterResponse)msg);
            else if (msg instanceof CoordinatorTxAckRequest)
                processCoordinatorTxAckRequest(nodeId, (CoordinatorTxAckRequest)msg);
            else if (msg instanceof CoordinatorTxAckResponse)
                processCoordinatorTxAckResponse(nodeId, (CoordinatorTxAckResponse)msg);
            else if (msg instanceof CoordinatorQueryAckRequest)
                processCoordinatorQueryAckRequest((CoordinatorQueryAckRequest)msg);
            else if (msg instanceof CoordinatorQueryVersionRequest)
                processCoordinatorQueryVersionRequest(nodeId, (CoordinatorQueryVersionRequest)msg);
            else if (msg instanceof CoordinatorQueryVersionResponse)
                processCoordinatorQueryVersionResponse(nodeId, (CoordinatorQueryVersionResponse) msg);
            else
                U.warn(log, "Unexpected message received [node=" + nodeId + ", msg=" + msg + ']');
        }
    }
}
