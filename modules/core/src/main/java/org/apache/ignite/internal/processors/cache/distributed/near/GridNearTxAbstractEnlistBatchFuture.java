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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class GridNearTxAbstractEnlistBatchFuture<T> extends GridNearTxAbstractEnlistFuture<T> {
    /** Default batch size. */
    protected static final int DFLT_BATCH_SIZE = 1024;

    /** SkipCntr field updater. */
    protected static final AtomicIntegerFieldUpdater<GridNearTxAbstractEnlistBatchFuture> SKIP_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxAbstractEnlistBatchFuture.class, "skipCntr");

    /** Marker object. */
    protected static final Object FINISHED = new Object();

    /** */
    protected final UpdateSourceIterator<?> it;

    /** */
    protected final Map<UUID, Batch> batches = new ConcurrentHashMap<>();

    /** */
    protected final boolean sequential;

    /** */
    protected final AtomicInteger batchCntr = new AtomicInteger();

    /** Batch size. */
    protected final int batchSize;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    protected volatile int skipCntr;

    /** Topology locked flag. */
    protected boolean topLocked;

    /** Row extracted from iterator but not yet used. */
    protected Object peek;

    /**
     * @param cctx    Cache context.
     * @param tx      Transaction.
     * @param timeout Timeout.
     * @param rdc     Compound future reducer.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     * @param sequential Sequential locking flag.
     */
    protected GridNearTxAbstractEnlistBatchFuture(
        GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout,
        @Nullable IgniteReducer<T, T> rdc,
        UpdateSourceIterator<?> it,
        int batchSize,
        boolean sequential) {
        super(cctx, tx, timeout, rdc);

        this.it = it;
        this.batchSize = batchSize > 0 ? batchSize : DFLT_BATCH_SIZE;
        this.sequential = sequential;
    }

    /**
     * Continue iterating the data rows and form new batches.
     *
     * @param nodeId Node that is ready for a new batch.
     */
    protected void sendNextBatches(@Nullable UUID nodeId) {
        try {
            Collection<Batch> next = continueLoop(nodeId);

            if (next == null)
                return;

            boolean first = (nodeId != null);

            for (Batch batch : next) {
                ClusterNode node = batch.node();

                sendBatch(node, batch, first);

                if (!node.isLocal())
                    first = false;
            }
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     *
     * @param node Node.
     * @param batch Batch.
     * @param first First mapping flag.
     */
    private void sendBatch(ClusterNode node, Batch batch, boolean first) throws IgniteCheckedException {
        updateMappings(node);

        boolean clientFirst = first && cctx.localNode().isClient() && !topLocked && !tx.hasRemoteLocks();

        int batchId = batchCntr.incrementAndGet();

        if (node.isLocal())
            enlistLocal(batchId, node.id(), batch);
        else
            sendBatch(batchId, node.id(), batch, clientFirst);
    }

    /**
     * Iterate data rows and form batches.
     *
     * @param nodeId Id of node acknowledged the last batch.
     * @return Collection of newly completed batches.
     * @throws IgniteCheckedException If failed.
     */
    private Collection<Batch> continueLoop(@Nullable UUID nodeId) throws IgniteCheckedException {
        if (nodeId != null)
            batches.remove(nodeId);

        // Accumulate number of batches released since we got here.
        // Let only one thread do the looping.
        if (isDone() || SKIP_UPD.getAndIncrement(this) != 0)
            return null;

        ArrayList<Batch> res = null;
        Batch batch = null;

        boolean flush = false;

        EnlistOperation op = it.operation();

        while (true) {
            while (hasNext0()) {
                checkCompleted();

                Object cur = next0();

                KeyCacheObject key = cctx.toCacheKeyObject(op.isDeleteOrLock() ? cur : ((Map.Entry<?, ?>)cur).getKey());

                ClusterNode node = cctx.affinity().primaryByKey(key, topVer);

                if (node == null)
                    throw new ClusterTopologyServerNotFoundException("Failed to get primary node " +
                        "[topVer=" + topVer + ", key=" + key + ']');

                if (!sequential)
                    batch = batches.get(node.id());
                else if (batch != null && !batch.node().equals(node))
                    res = markReady(res, batch);

                if (batch == null)
                    batches.put(node.id(), batch = new Batch(node));

                if (batch.ready()) {
                    // Can't advance further at the moment.
                    batch = null;

                    peek = cur;

                    it.beforeDetach();

                    flush = true;

                    break;
                }

                batch.add(op.isDeleteOrLock() ? key : cur, !node.isLocal() && isLocalBackup(op, key));

                if (batch.size() == batchSize)
                    res = markReady(res, batch);
            }

            if (SKIP_UPD.decrementAndGet(this) == 0)
                break;

            skipCntr = 1;
        }

        if (flush)
            return res;

        // No data left - flush incomplete batches.
        for (Batch batch0 : batches.values()) {
            if (!batch0.ready()) {
                if (res == null)
                    res = new ArrayList<>();

                batch0.ready(true);

                res.add(batch0);
            }
        }

        if (batches.isEmpty())
            complete();

        return res;
    }

    /**
     * @param primaryId Primary node id.
     * @param rows Rows.
     * @param dhtVer Dht version assigned at primary node.
     * @param dhtFutId Dht future id assigned at primary node.
     */
    protected void processBatchLocalBackupKeys(UUID primaryId, List<Object> rows, GridCacheVersion dhtVer,
        IgniteUuid dhtFutId) {
        assert dhtVer != null;
        assert dhtFutId != null;

        EnlistOperation op = it.operation();

        assert op != EnlistOperation.LOCK;

        boolean keysOnly = op.isDeleteOrLock();

        final ArrayList<KeyCacheObject> keys = new ArrayList<>(rows.size());
        final ArrayList<Message> vals = keysOnly ? null : new ArrayList<>(rows.size());

        for (Object row : rows) {
            if (keysOnly)
                keys.add(cctx.toCacheKeyObject(row));
            else {
                keys.add(cctx.toCacheKeyObject(((Map.Entry<?, ?>)row).getKey()));

                if (op.isInvoke())
                    vals.add((Message)((Map.Entry<?, ?>)row).getValue());
                else
                    vals.add(cctx.toCacheObject(((Map.Entry<?, ?>)row).getValue()));
            }
        }

        try {
            GridDhtTxRemote dhtTx = cctx.tm().tx(dhtVer);

            if (dhtTx == null) {
                dhtTx = new GridDhtTxRemote(cctx.shared(),
                    cctx.localNodeId(),
                    primaryId,
                    lockVer,
                    topVer,
                    dhtVer,
                    null,
                    cctx.systemTx(),
                    cctx.ioPolicy(),
                    PESSIMISTIC,
                    REPEATABLE_READ,
                    false,
                    tx.remainingTime(),
                    -1,
                    SecurityUtils.securitySubjectId(cctx),
                    tx.taskNameHash(),
                    false,
                    tx.label());

                dhtTx.mvccSnapshot(new MvccSnapshotWithoutTxs(mvccSnapshot.coordinatorVersion(),
                    mvccSnapshot.counter(), MVCC_OP_COUNTER_NA, mvccSnapshot.cleanupVersion()));

                dhtTx = cctx.tm().onCreated(null, dhtTx);

                if (dhtTx == null || !cctx.tm().onStarted(dhtTx)) {
                    throw new IgniteTxRollbackCheckedException("Failed to update backup " +
                        "(transaction has been completed): " + dhtVer);
                }
            }

            cctx.tm().txHandler().mvccEnlistBatch(dhtTx, cctx, it.operation(), keys, vals,
                mvccSnapshot.withoutActiveTransactions(), null, -1);
        }
        catch (IgniteCheckedException e) {
            onDone(e);

            return;
        }

        sendNextBatches(primaryId);
    }

    /** */
    private Object next0() {
        if (!hasNext0())
            throw new NoSuchElementException();

        Object cur;

        if ((cur = peek) != null)
            peek = null;
        else
            cur = it.next();

        return cur;
    }

    /** */
    private boolean hasNext0() {
        if (peek == null && !it.hasNext())
            peek = FINISHED;

        return peek != FINISHED;
    }

    /**
     * Add batch to batch collection if it is ready.
     *
     * @param batches Collection of batches.
     * @param batch Batch to be added.
     */
    private ArrayList<Batch> markReady(ArrayList<Batch> batches, Batch batch) {
        if (!batch.ready()) {
            batch.ready(true);

            if (batches == null)
                batches = new ArrayList<>();

            batches.add(batch);
        }

        return batches;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (batches.containsKey(nodeId)) {
            if (log.isDebugEnabled())
                log.debug("Found unacknowledged batch for left node [nodeId=" + nodeId + ", fut=" +
                    this + ']');

            ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to enlist keys " +
                "(primary node left grid, retry transaction if possible) [node=" + nodeId + ']');

            topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

            onDone(topEx);
        }

        if (log.isDebugEnabled())
            log.debug("Future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');

        return false;
    }

    /** */
    protected boolean isLocalBackup(EnlistOperation op, KeyCacheObject key) {
        if (!cctx.affinityNode() || op == EnlistOperation.LOCK)
            return false;
        else if (cctx.isReplicated())
            return true;

        return cctx.topology().nodes(key.partition(), tx.topologyVersion()).indexOf(cctx.localNode()) > 0;
    }

    /**
     * Enlist batch of entries to the transaction on local node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batch Batch.
     */
    protected abstract void enlistLocal(int batchId, UUID nodeId, Batch batch) throws IgniteCheckedException;

    /**
     * Send batch request to remote data node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batchFut Mini-future for the batch.
     * @param clientFirst {@code true} if originating node is client and it is a first request to any data node.
     */
    protected abstract void sendBatch(int batchId, UUID nodeId, Batch batchFut, boolean clientFirst) throws IgniteCheckedException;

    /**
     *
     */
    protected abstract void complete();

    /**
     * A batch of rows
     */
    protected static class Batch {
        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** Rows. */
        private final List<Object> rows = new ArrayList<>();

        /** Local backup rows. */
        private List<Object> locBkpRows;

        /** Readiness flag. Set when batch is full or no new rows are expected. */
        private boolean ready;

        /**
         * @param node Cluster node.
         */
        protected Batch(ClusterNode node) {
            this.node = node;
        }

        /**
         * @return Node.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * Adds a row.
         *
         * @param row Row.
         * @param locBackup {@code true}, when the row key has local backup.
         */
        public void add(Object row, boolean locBackup) {
            rows.add(row);

            if (locBackup) {
                if (locBkpRows == null)
                    locBkpRows = new ArrayList<>();

                locBkpRows.add(row);
            }
        }

        /**
         * @return number of rows.
         */
        public int size() {
            return rows.size();
        }

        /**
         * @return Collection of rows.
         */
        public Collection<Object> rows() {
            return rows;
        }

        /**
         * @return Collection of local backup rows.
         */
        public List<Object> localBackupRows() {
            return locBkpRows;
        }

        /**
         * @return Readiness flag.
         */
        public boolean ready() {
            return ready;
        }

        /**
         * Sets readiness flag.
         *
         * @param ready Flag value.
         */
        public void ready(boolean ready) {
            this.ready = ready;
        }
    }
}
