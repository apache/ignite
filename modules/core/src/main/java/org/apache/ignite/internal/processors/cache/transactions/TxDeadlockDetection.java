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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_DEADLOCK_DETECTION_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.DEADLOCK_MAX_ITERS;

/**
 * Transactions deadlock detection.
 */
public class TxDeadlockDetection {
    /** Deadlock detection maximum iterations. */
    private static final int DEADLOCK_TIMEOUT = getInteger(IGNITE_TX_DEADLOCK_DETECTION_TIMEOUT, 60000);

    /** Sequence. */
    private static final AtomicLong SEQ = new AtomicLong();

    /** Cctx. */
    private final GridCacheSharedContext cctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param cctx Context.
     */
    public TxDeadlockDetection(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
        this.log = cctx.logger(TxDeadlockDetection.class);
    }

    /**
     * Detects deadlock starting from given keys.
     *
     * @param tx Target tx.
     * @param keys Keys.
     * @return {@link TxDeadlock} if found, otherwise - {@code null}.
     */
    TxDeadlockFuture detectDeadlock(IgniteInternalTx tx, Set<IgniteTxKey> keys) {
        GridCacheVersion txId = tx.nearXidVersion();

        if (log.isDebugEnabled()) {
            log.debug("Deadlock detection started " +
                "[nodeId=" + cctx.localNodeId() + ", xidVersion=" + txId + ", keys=" + keys + ']');
        }

        TxDeadlockFuture fut = new TxDeadlockFuture(cctx, txId, tx.topologyVersion(), keys);

        fut.init();

        return fut;
    }

    /**
     * @param wfg Wait-for-graph.
     * @param txId Tx ID - start vertex for cycle search in graph.
     */
    static List<GridCacheVersion> findCycle(Map<GridCacheVersion, Set<GridCacheVersion>> wfg, GridCacheVersion txId) {
        if (wfg == null || wfg.isEmpty())
            return null;

        ArrayDeque<GridCacheVersion> stack = new ArrayDeque<>();
        Set<GridCacheVersion> inPath = new HashSet<>();
        Set<GridCacheVersion> visited = new HashSet<>();
        Map<GridCacheVersion, GridCacheVersion> edgeTo = new HashMap<>();

        stack.push(txId);

        while (!stack.isEmpty()) {
            GridCacheVersion v = stack.peek();

            if (visited.contains(v)) {
                stack.pop();
                inPath.remove(v);

                continue;
            }

            visited.add(v);

            Set<GridCacheVersion> children = wfg.get(v);

            if (children == null || children.isEmpty()) {
                stack.pop();
                inPath.remove(v);

                continue;
            }

            inPath.add(v);

            for (GridCacheVersion w : children) {
                if (inPath.contains(w) && visited.contains(w)) {
                    List<GridCacheVersion> cycle = new ArrayList<>();

                    for (GridCacheVersion x = v; !x.equals(w); x = edgeTo.get(x))
                        cycle.add(x);

                    cycle.add(w);
                    cycle.add(v);

                    return cycle;
                }

                edgeTo.put(w, v);
                stack.push(w);
            }
        }

        return null;
    }

    /**
     *
     */
    static class TxDeadlockFuture extends GridFutureAdapter<TxDeadlock> {
        /** Context. */
        private final GridCacheSharedContext cctx;

        /** Future ID. */
        private final long futId = SEQ.incrementAndGet();

        /** Tx ID. */
        private final GridCacheVersion txId;

        /** Keys. */
        private final Set<IgniteTxKey> keys;

        /** Processed keys. */
        @GridToStringInclude
        private final Set<IgniteTxKey> processedKeys = new HashSet<>();

        /** Processed nodes. */
        private final Set<UUID> processedNodes = new HashSet<>();

        /** Pending keys. */
        @GridToStringInclude
        private Map<UUID, Set<IgniteTxKey>> pendingKeys = new HashMap<>();

        /** Nodes queue. */
        @GridToStringInclude
        private final UniqueDeque<UUID> nodesQueue = new UniqueDeque<>();

        /** Preferred nodes. */
        private final Set<UUID> preferredNodes = new HashSet<>();

        /** Tx locked keys. */
        private final Map<GridCacheVersion, Set<IgniteTxKey>> txLockedKeys = new HashMap<>();

        /** Tx requested keys. */
        private final Map<IgniteTxKey, Set<GridCacheVersion>> txRequestedKeys = new HashMap<>();

        /** Wait-for-graph. */
        private final Map<GridCacheVersion, Set<GridCacheVersion>> wfg = new HashMap<>();

        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Transactions. */
        private final Map<GridCacheVersion, T2<UUID, Long>> txs = new HashMap<>();

        /** Current processing node ID. */
        private UUID curNodeId;

        /** Iterations count. */
        private int itersCnt;

        /** Timeout object. */
        @GridToStringExclude
        private DeadlockTimeoutObject timeoutObj;

        /** Timed out flag. */
        private volatile boolean timedOut;

        /** Mutex. */
        private final Object mux = new Object();

        /**
         * @param cctx Context.
         * @param txId Tx ID.
         * @param topVer Transaction topology version.
         * @param keys Keys.
         */
        @SuppressWarnings("unchecked")
        private TxDeadlockFuture(GridCacheSharedContext cctx,
            GridCacheVersion txId,
            AffinityTopologyVersion topVer,
            Set<IgniteTxKey> keys) {
            this.cctx = cctx;
            this.txId = txId;
            this.topVer = topVer;
            this.keys = keys;

            if (DEADLOCK_TIMEOUT > 0) {
                timeoutObj = new DeadlockTimeoutObject();

                cctx.time().addTimeoutObject(timeoutObj);
            }
        }

        /**
         * @return Future ID.
         */
        long futureId() {
            return futId;
        }

        /**
         * @param nodeId Node ID.
         */
        public void onNodeLeft(UUID nodeId) {
            if (compareAndSet(nodeId, null)) {
                IgniteLogger log = cctx.logger(TxDeadlockDetection.class);

                if (log.isDebugEnabled())
                    log.debug("Failed to finish deadlock detection, node left: " + nodeId);

                onDone();
            }
        }

        /** */
        private void init() {
            cctx.tm().addFuture(this);

            if (topVer == null) // Tx manager already stopped
                onDone();
            else
                map(keys, Collections.<IgniteTxKey, TxLockList>emptyMap());
        }

        /**
         * @param keys Keys.
         * @param txLocks Tx locks.
         */
        private void map(@Nullable Set<IgniteTxKey> keys, Map<IgniteTxKey, TxLockList> txLocks) {
            mapTxKeys(keys, txLocks);

            UUID nodeId = nodesQueue.pollFirst();

            boolean set = compareAndSet(null, nodeId);

            assert set;

            if (nodeId == null || itersCnt++ >= DEADLOCK_MAX_ITERS || timedOut)
                onDone();
            else {
                final Set<IgniteTxKey> txKeys = pendingKeys.get(nodeId);

                processedKeys.addAll(txKeys);
                processedNodes.add(nodeId);
                pendingKeys.remove(nodeId);

                cctx.tm().txLocksInfo(nodeId, this, txKeys);
            }
        }

        /**
         * @param res Response.
         */
        private void detect(TxLocksResponse res) {
            assert res != null;

            merge(res);

            updateWaitForGraph(res.txLocks());

            List<GridCacheVersion> cycle = findCycle(wfg, txId);

            if (cycle != null)
                onDone(new TxDeadlock(cycle, txs, txLockedKeys, txRequestedKeys));
            else
                map(res.keys(), res.txLocks());
        }

        /**
         * Maps tx keys on nodes. Key can be mapped on some node if this node is primary for given key or
         * node is near for transaction that holds or requests lock for key.
         *
         * Key will not be be mapped to node if both key and node are already handled.
         *
         * @param txKeys Tx keys.
         * @param txLocks Tx locks.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        private void mapTxKeys(@Nullable Set<IgniteTxKey> txKeys, Map<IgniteTxKey, TxLockList> txLocks) {
            for (Map.Entry<IgniteTxKey, TxLockList> e : txLocks.entrySet()) {
                List<TxLock> locks = e.getValue().txLocks();

                for (int i = 0; i < locks.size(); i++) {
                    TxLock txLock = locks.get(i);

                    UUID nearNodeId = txLock.nearNodeId();

                    IgniteTxKey txKey = e.getKey();

                    if (processedKeys.contains(txKey) && processedNodes.contains(nearNodeId))
                        continue;

                    if (txLock.requested()) {
                        UUID nodeId = primary(txKey);

                        // Process this node earlier than other in order to optimize amount of requests.
                        preferredNodes.add(nodeId);

                        Set<IgniteTxKey> mappedKeys = pendingKeys.get(nodeId);

                        if (mappedKeys == null)
                            pendingKeys.put(nodeId, mappedKeys = new HashSet<>());

                        mappedKeys.add(txKey);
                    }
                    else {
                        if (txLock.owner()) {
                            if (!preferredNodes.contains(nearNodeId))
                                nodesQueue.addFirst(nearNodeId);
                        }
                        else
                            nodesQueue.addLast(nearNodeId);

                        Set<IgniteTxKey> mappedKeys = pendingKeys.get(nearNodeId);

                        if (mappedKeys == null)
                            pendingKeys.put(nearNodeId, mappedKeys = new HashSet<>());

                        mappedKeys.add(txKey);
                    }
                }
            }

            for (UUID nodeId : preferredNodes)
                nodesQueue.addFirst(nodeId);

            preferredNodes.clear();

            if (txKeys != null) {
                for (IgniteTxKey txKey : txKeys) {
                    UUID nodeId = primary(txKey);

                    if (processedKeys.contains(txKey) && processedNodes.contains(nodeId))
                        continue;

                    nodesQueue.addLast(nodeId);

                    Set<IgniteTxKey> mappedKeys = pendingKeys.get(nodeId);

                    if (mappedKeys == null)
                        pendingKeys.put(nodeId, mappedKeys = new HashSet<>());

                    mappedKeys.add(txKey);
                }
            }
        }

        /**
         * @param txKey Tx key.
         * @return Primary node ID.
         */
        private UUID primary(IgniteTxKey txKey) {
            GridCacheContext ctx = cctx.cacheContext(txKey.cacheId());

            ClusterNode node = ctx.affinity().primaryByKey(txKey.key(), topVer);

            assert node != null : topVer;

            return node.id();
        }

        /**
         * @param res Tx locks.
         */
        private void merge(TxLocksResponse res) {
            Map<IgniteTxKey, TxLockList> txLocks = res.txLocks();

            if (txLocks == null || txLocks.isEmpty())
                return;

            for (Map.Entry<IgniteTxKey, TxLockList> e : txLocks.entrySet()) {
                IgniteTxKey txKey = e.getKey();

                TxLockList lockList = e.getValue();

                if (lockList != null && !lockList.isEmpty()) {
                    for (TxLock lock : lockList.txLocks()) {
                        if (lock.owner() || lock.candiate()) {
                            if (txs.get(lock.txId()) == null)
                                txs.put(lock.txId(), new T2<>(lock.nearNodeId(), lock.threadId()));
                        }

                        if (lock.owner()) {
                            GridCacheVersion txId = lock.txId();

                            Set<IgniteTxKey> keys = txLockedKeys.get(txId);

                            if (keys == null)
                                txLockedKeys.put(txId, keys = new HashSet<>());

                            keys.add(txKey);
                        }
                        else if (lock.candiate()) {
                            Set<GridCacheVersion> txs = txRequestedKeys.get(txKey);

                            if (txs == null)
                                txRequestedKeys.put(txKey, txs = new HashSet<>());

                            txs.add(lock.txId());
                        }
                    }
                }
            }
        }

        /**
         * @param txLocks Tx locks.
         */
        private void updateWaitForGraph(Map<IgniteTxKey, TxLockList> txLocks) {
            if (txLocks == null || txLocks.isEmpty())
                return;

            for (Map.Entry<IgniteTxKey, TxLockList> e : txLocks.entrySet()) {

                GridCacheVersion txOwner = null;

                for (TxLock lock : e.getValue().txLocks()) {
                    if (lock.owner() && txOwner == null) {
                        // Actually we can get lock list with more than one owner. In this case ignore all owners
                        // except first because likely the first owner was cause of deadlock.
                        txOwner = lock.txId();

                        if (keys.contains(e.getKey()) && !txId.equals(lock.txId())) {
                            Set<GridCacheVersion> waitingTxs = wfg.get(txId);

                            if (waitingTxs == null)
                                wfg.put(txId, waitingTxs = new HashSet<>());

                            waitingTxs.add(lock.txId());
                        }

                        continue;
                    }

                    if (lock.candiate() || lock.owner()) {
                        GridCacheVersion txId0 = lock.txId();

                        Set<GridCacheVersion> waitForTxs = wfg.get(txId0);

                        if (waitForTxs == null)
                            wfg.put(txId0, waitForTxs = new HashSet<>());

                        waitForTxs.add(txOwner);
                    }
                }
            }
        }

        /**
         * @param res Response.
         */
        public void onResult(UUID nodeId, TxLocksResponse res) {
            boolean set = compareAndSet(nodeId, null);

            if (res != null && set) {
                if (res.classError() != null) {
                    IgniteLogger log = cctx.kernalContext().log(this.getClass());

                    U.warn(log, "Failed to finish deadlock detection due to an error: " + nodeId);

                    onDone();
                }
                else
                    detect(res);
            }
            else
                onDone();
        }

        /**
         * @param exp Expected.
         * @param val Value.
         */
        private boolean compareAndSet(UUID exp, UUID val) {
            synchronized (mux) {
                if (Objects.equals(curNodeId, exp)) {
                    curNodeId = val;

                    return true;
                }
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable TxDeadlock res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                cctx.tm().removeFuture(futId);

                if (timeoutObj != null)
                    cctx.time().removeTimeoutObject(timeoutObj);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TxDeadlockFuture.class, this);
        }

        /**
         * Lock request timeout object.
         */
        private class DeadlockTimeoutObject extends GridTimeoutObjectAdapter {
            /**
             * Default constructor.
             */
            DeadlockTimeoutObject() {
                super(DEADLOCK_TIMEOUT);
            }

            /** {@inheritDoc} */
            @Override public void onTimeout() {
                timedOut = true;

                IgniteLogger log = cctx.kernalContext().log(this.getClass());

                U.warn(log, "Deadlock detection was timed out [timeout=" + DEADLOCK_TIMEOUT + ", fut=" + this + ']');

                onDone();
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return S.toString(DeadlockTimeoutObject.class, this);
            }
        }
    }

    /**
     * Deque with Set semantic.
     * Only overridden methods can be used.
     */
    private static class UniqueDeque<E> extends ArrayDeque<E> {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Items. */
        private final Set<E> items = new HashSet<>();

        /** {@inheritDoc} */
        @Override public void addFirst(E e) {
            boolean contains, first = false;

            if ((contains = items.contains(e)) && !(first = getFirst().equals(e)))
                remove(e);

            if (!contains)
                items.add(e);

            if (!first)
                super.addFirst(e);
        }

        /** {@inheritDoc} */
        @Override public void addLast(E e) {
            if (!items.contains(e)) {
                super.addLast(e);

                items.add(e);
            }
        }

        /** {@inheritDoc} */
        @Override public E pollFirst() {
            E e = super.pollFirst();

            items.remove(e);

            return e;
        }
    }
}
