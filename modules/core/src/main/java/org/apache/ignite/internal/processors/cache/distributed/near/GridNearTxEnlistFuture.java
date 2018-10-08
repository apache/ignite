/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.NearTxResultHandler.createResponse;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * A future tracking requests for remote nodes transaction enlisting and locking produces by cache API operations.
 */
public class GridNearTxEnlistFuture extends GridNearTxAbstractEnlistFuture<GridCacheReturn> {
    /** Default batch size. */
    public static final int DFLT_BATCH_SIZE = 1024;

    /** SkipCntr field updater. */
    private static final AtomicIntegerFieldUpdater<GridNearTxEnlistFuture> SKIP_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxEnlistFuture.class, "skipCntr");

    /** Marker object. */
    private static final Object FINISHED = new Object();

    /** Source iterator. */
    @GridToStringExclude
    private final UpdateSourceIterator<?> it;

    /** Batch size. */
    private int batchSize;

    /** */
    private AtomicInteger batchCntr = new AtomicInteger();

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int skipCntr;

    /** Future result. */
    @GridToStringExclude
    private volatile GridCacheReturn res;

    /** */
    private final Map<UUID, Batch> batches = new ConcurrentHashMap<>();

    /** Row extracted from iterator but not yet used. */
    private Object peek;

    /** Topology locked flag. */
    private boolean topLocked;

    /** Ordered batch sending flag. */
    private final boolean sequential;

    /** Filter. */
    private final CacheEntryPredicate filter;

    /** Need previous value flag. */
    private final boolean needRes;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param timeout Timeout.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     * @param sequential Sequential locking flag.
     * @param filter Filter.
     * @param needRes Need previous value flag.
     */
    public GridNearTxEnlistFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout,
        UpdateSourceIterator<?> it,
        int batchSize,
        boolean sequential,
        @Nullable CacheEntryPredicate filter,
        boolean needRes) {
        super(cctx, tx, timeout, null);

        this.it = it;
        this.batchSize = batchSize > 0 ? batchSize : DFLT_BATCH_SIZE;
        this.sequential = sequential;
        this.filter = filter;
        this.needRes = needRes;
    }

    /** {@inheritDoc} */
    @Override protected void map(boolean topLocked) {
        this.topLocked = topLocked;

        sendNextBatches(null);
    }

    /**
     * Continue iterating the data rows and form new batches.
     *
     * @param nodeId Node that is ready for a new batch.
     */
    private void sendNextBatches(@Nullable UUID nodeId) {
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

                KeyCacheObject key = cctx.toCacheKeyObject(op.isDeleteOrLock() ? cur : ((IgniteBiTuple)cur).getKey());

                List<ClusterNode> nodes = cctx.affinity().nodesByKey(key, topVer);

                ClusterNode node;

                if (F.isEmpty(nodes) || ((node = nodes.get(0)) == null))
                    throw new ClusterTopologyCheckedException("Failed to get primary node " +
                        "[topVer=" + topVer + ", key=" + key + ']');

                tx.markQueryEnlisted(null);

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

                batch.add(op.isDeleteOrLock() ? key : cur,
                    op != EnlistOperation.LOCK && cctx.affinityNode() && (cctx.isReplicated() || nodes.indexOf(cctx.localNode()) > 0));

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
            onDone(this.res);

        return res;
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

    /**
     * @param primaryId Primary node id.
     * @param rows Rows.
     * @param dhtVer Dht version assigned at primary node.
     * @param dhtFutId Dht future id assigned at primary node.
     */
    private void processBatchLocalBackupKeys(UUID primaryId, List<Object> rows, GridCacheVersion dhtVer,
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
                keys.add(cctx.toCacheKeyObject(((IgniteBiTuple)row).getKey()));

                if (op.isInvoke())
                    vals.add((Message)((IgniteBiTuple)row).getValue());
                else
                    vals.add(cctx.toCacheObject(((IgniteBiTuple)row).getValue()));
            }
        }

        try {
            GridDhtTxRemote dhtTx = cctx.tm().tx(dhtVer);

            if (dhtTx == null) {
                dhtTx = new GridDhtTxRemote(cctx.shared(),
                    cctx.localNodeId(),
                    dhtFutId,
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
                    this.tx.subjectId(),
                    this.tx.taskNameHash(),
                    false);

                dhtTx.mvccSnapshot(new MvccSnapshotWithoutTxs(mvccSnapshot.coordinatorVersion(),
                    mvccSnapshot.counter(), MVCC_OP_COUNTER_NA, mvccSnapshot.cleanupVersion()));

                dhtTx = cctx.tm().onCreated(null, dhtTx);

                if (dhtTx == null || !cctx.tm().onStarted(dhtTx)) {
                    throw new IgniteTxRollbackCheckedException("Failed to update backup " +
                        "(transaction has been completed): " + dhtVer);
                }
            }

            dhtTx.mvccEnlistBatch(cctx, it.operation(), keys, vals, mvccSnapshot.withoutActiveTransactions());
        }
        catch (IgniteCheckedException e) {
            onDone(e);

            return;
        }

        sendNextBatches(primaryId);
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
     * Send batch request to remote data node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batchFut Mini-future for the batch.
     * @param clientFirst {@code true} if originating node is client and it is a first request to any data node.
     */
    private void sendBatch(int batchId, UUID nodeId, Batch batchFut, boolean clientFirst) throws IgniteCheckedException {
        assert batchFut != null;

        GridNearTxEnlistRequest req = new GridNearTxEnlistRequest(cctx.cacheId(),
            threadId,
            futId,
            batchId,
            tx.subjectId(),
            topVer,
            lockVer,
            mvccSnapshot,
            clientFirst,
            remainingTime(),
            tx.remainingTime(),
            tx.taskNameHash(),
            batchFut.rows(),
            it.operation(),
            needRes,
            filter
        );

        sendRequest(req, nodeId);
    }

    /**
     * @param req Request.
     * @param nodeId Remote node ID
     * @throws IgniteCheckedException if failed to send.
     */
    private void sendRequest(GridCacheMessage req, UUID nodeId) throws IgniteCheckedException {
        IgniteInternalFuture<?> txSync = cctx.tm().awaitFinishAckAsync(nodeId, tx.threadId());

        if (txSync == null || txSync.isDone())
            cctx.io().send(nodeId, req, cctx.ioPolicy());
        else
            txSync.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> future) {
                    try {
                        cctx.io().send(nodeId, req, cctx.ioPolicy());
                    }
                    catch (IgniteCheckedException e) {
                        GridNearTxEnlistFuture.this.onDone(e);
                    }
                }
            });
    }

    /**
     * Enlist batch of entries to the transaction on local node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batch Batch.
     */
    private void enlistLocal(int batchId, UUID nodeId, Batch batch) throws IgniteCheckedException {
        Collection<Object> rows = batch.rows();

        GridDhtTxEnlistFuture fut = new GridDhtTxEnlistFuture(nodeId,
            lockVer,
            mvccSnapshot,
            threadId,
            futId,
            batchId,
            tx,
            remainingTime(),
            cctx,
            rows,
            it.operation(),
            filter,
            needRes);

        updateLocalFuture(fut);

        fut.listen(new CI1<IgniteInternalFuture<GridCacheReturn>>() {
            @Override public void apply(IgniteInternalFuture<GridCacheReturn> fut) {
                try {
                    clearLocalFuture((GridDhtTxAbstractEnlistFuture)fut);

                    GridNearTxEnlistResponse res = fut.error() == null ? createResponse(fut) : null;

                    if (checkResponse(nodeId, res, fut.error()))
                        sendNextBatches(nodeId);
                }
                catch (IgniteCheckedException e) {
                    checkResponse(nodeId, null, e);
                }
                finally {
                    CU.unwindEvicts(cctx);
                }
            }
        });

        fut.init();
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearTxEnlistResponse res) {
        if (checkResponse(nodeId, res, res.error())) {

            Batch batch = batches.get(nodeId);

            if (batch != null && !F.isEmpty(batch.localBackupRows()) && res.dhtFutureId() != null)
                processBatchLocalBackupKeys(nodeId, batch.localBackupRows(), res.dhtVersion(), res.dhtFutureId());
            else
                sendNextBatches(nodeId);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (batches.keySet().contains(nodeId)) {
            if (log.isDebugEnabled())
                log.debug("Found unacknowledged batch for left node [nodeId=" + nodeId + ", fut=" +
                    this + ']');

            ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to enlist keys " +
                "(primary node left grid, retry transaction if possible) [node=" + nodeId + ']');

            topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

            processFailure(topEx, null);

            batches.remove(nodeId);

            if (batches.isEmpty()) // Wait for all pending requests.
                onDone();

        }

        if (log.isDebugEnabled())
            log.debug("Future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');

        return false;
    }

    /**
     * @param nodeId Originating node ID.
     * @param res Response.
     * @param err Exception.
     * @return {@code True} if future was completed by this call.
     */
    @SuppressWarnings("unchecked")
    public boolean checkResponse(UUID nodeId, GridNearTxEnlistResponse res, Throwable err) {
        assert res != null || err != null : this;

        if (err == null && res.error() != null)
            err = res.error();

        if (X.hasCause(err, ClusterTopologyCheckedException.class))
            tx.removeMapping(nodeId);

        if (err != null)
            processFailure(err, null);

        if (ex != null) {
            batches.remove(nodeId);

            if (batches.isEmpty()) // Wait for all pending requests.
                onDone();

            return false;
        }

        assert res != null;

        if (res.result().invokeResult()) {
            if(this.res == null)
                this.res = new GridCacheReturn(true, true);

            this.res.success(this.res.success() && err == null && res.result().success());

            this.res.mergeEntryProcessResults(res.result());
        }
        else
            this.res = res.result();

        assert this.res != null && (this.res.emptyResult() || needRes || this.res.invokeResult() || !this.res.success());

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxEnlistFuture.class, this, super.toString());
    }

    /**
     * A batch of rows
     */
    private static class Batch {
        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** Rows. */
        private List<Object> rows = new ArrayList<>();

        /** Local backup rows. */
        private List<Object> locBkpRows;

        /** Readiness flag. Set when batch is full or no new rows are expected. */
        private boolean ready;

        /**
         * @param node Cluster node.
         */
        private Batch(ClusterNode node) {
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
         * @param localBackup {@code true}, when the row key has local backup.
         */
        public void add(Object row, boolean localBackup) {
            rows.add(row);

            if (localBackup) {
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
