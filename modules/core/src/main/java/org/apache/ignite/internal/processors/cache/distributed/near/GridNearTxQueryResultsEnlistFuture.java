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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryResultsEnlistFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotResponseListener;
import org.apache.ignite.internal.processors.cache.mvcc.MvccTxInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * A future tracking requests for remote nodes transaction enlisting and locking
 * of entries produced with complex DML queries requiring reduce step.
 */
public class GridNearTxQueryResultsEnlistFuture extends GridCacheCompoundIdentityFuture<Long>
    implements GridCacheVersionedFuture<Long>, MvccSnapshotResponseListener {
    /** */
    public static final int DFLT_BATCH_SIZE = 1024;

    /** Done field updater. */
    private static final AtomicIntegerFieldUpdater<GridNearTxQueryResultsEnlistFuture> DONE_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxQueryResultsEnlistFuture.class, "done");

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int done;

    /** Cache context. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Transaction. */
    private final GridNearTxLocal tx;

    /** Initiated thread id. */
    private final long threadId;

    /** Mvcc future id. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** */
    private long timeout;

    /** */
    private GridCacheOperation op;

    /** */
    private final UpdateSourceIterator<IgniteBiTuple> it;

    /** */
    private int batchSize;

    /** */
    private final Map<UUID, BatchFuture> batches = new HashMap<>();

    /** */
    private AffinityTopologyVersion topVer;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Row extracted from iterator but not yet used. */
    private IgniteBiTuple peek;

    /** Topology locked flag. */
    private boolean topLocked;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param mvccSnapshot MVCC Snapshot.
     * @param timeout Timeout.
     * @param op Cache operation.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     */
    GridNearTxQueryResultsEnlistFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        MvccSnapshot mvccSnapshot,
        long timeout,
        GridCacheOperation op,
        UpdateSourceIterator<IgniteBiTuple> it,
        int batchSize) {
        super(CU.longReducer());

        this.cctx = cctx;
        this.tx = tx;
        this.mvccSnapshot = mvccSnapshot;
        this.timeout = timeout;
        this.op = op;
        this.it = it;
        this.batchSize = batchSize > 0 ? batchSize : DFLT_BATCH_SIZE;

        threadId = tx.threadId();
        futId = IgniteUuid.randomUuid();
        lockVer = tx.xidVersion();

        log = cctx.logger(GridNearTxQueryResultsEnlistFuture.class);
    }

    /** */
    public void init() {
        if (tx.trackTimeout()) {
            if (!tx.removeTimeoutHandler()) {
                tx.finishFuture().listen(new IgniteInClosure<IgniteInternalFuture<IgniteInternalTx>>() {
                    @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                        IgniteTxTimeoutCheckedException err = new IgniteTxTimeoutCheckedException("Failed to " +
                            "acquire lock, transaction was rolled back on timeout [timeout=" + tx.timeout() +
                            ", tx=" + tx + ']');

                        onDone(err);
                    }
                });

                return;
            }
        }

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        boolean added = cctx.mvcc().addFuture(this);

        assert added : this;

        // Obtain the topology version to use.
        long threadId = Thread.currentThread().getId();

        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(threadId);

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx.system())
            topVer = cctx.tm().lockedTopologyVersion(threadId, tx);

        if (topVer != null)
            tx.topologyVersion(topVer);

        if (topVer == null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer != null) {
            for (GridDhtTopologyFuture fut : cctx.shared().exchange().exchangeFutures()) {
                if (fut.exchangeDone() && fut.topologyVersion().equals(topVer)) {
                    Throwable err = fut.validateCache(cctx, false, false, null, null);

                    if (err != null) {
                        onDone(err);

                        return;
                    }

                    break;
                }
            }

            if (this.topVer == null)
                this.topVer = topVer;

            topLocked = true;

            map();

            return;
        }

        mapOnTopology();
    }

    /**
     * Acquire topology future and wait for its completion.
     * Start forming batches on stable topology.
     */
    private void mapOnTopology() {
        cctx.topology().readLock();

        try {
            if (cctx.topology().stopping()) {
                onDone(new CacheStoppedException(cctx.name()));

                return;
            }

            GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

            if (fut.isDone()) {
                Throwable err = fut.validateCache(cctx, false, false, null, null);

                if (err != null) {
                    onDone(err);

                    return;
                }

                AffinityTopologyVersion topVer = fut.topologyVersion();

                if (tx != null)
                    tx.topologyVersion(topVer);

                if (this.topVer == null)
                    this.topVer = topVer;

                map();
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            mapOnTopology();
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                        finally {
                            cctx.shared().txContextReset();
                        }
                    }
                });
            }
        }
        finally {
            cctx.topology().readUnlock();
        }
    }

    /**
     * Start iterating the data rows and form batches.
     */
    private void map() {
        sendNextBatches(null);

        markInitialized();
    }

    /**
     * Continue iterating the data rows and form new batches.
     *
     * @param nodeId Node that is ready for a new batch.
     */
    private void sendNextBatches(@Nullable UUID nodeId) {
        Collection<BatchFuture> next;

        try {
            next = mapRows(nodeId);
        }
        catch (IgniteCheckedException e) {
            onDone(e);

            return;
        }

        if (next == null)
            return;

        boolean first = (nodeId != null);

        for (BatchFuture batch : next) {
            ClusterNode node = batch.node();

            sendBatch(node, batch, first);

            if (!node.isLocal())
                first = false;
        }
    }

    /**
     * Iterate data rows and form batches.
     *
     * @param nodeId Id of node acknowledged the last batch.
     * @return Collection of newly completed batches.
     * @throws IgniteCheckedException If failed.
     */
    private synchronized Collection<BatchFuture> mapRows(@Nullable UUID nodeId) throws IgniteCheckedException {
        if (nodeId != null)
            batches.remove(nodeId);

        ArrayList<BatchFuture> res = null;

        IgniteBiTuple cur = (peek != null) ? peek : (it.hasNextX() ? it.nextX() : null);

        while (cur != null) {
            ClusterNode node = cctx.affinity().primaryByKey(cur.getKey(), topVer);

            if (node == null)
                throw new ClusterTopologyCheckedException("Failed to get primary node " +
                    "[topVer=" + topVer + ", key=" + cur.getKey() + ']');

            BatchFuture batch = batches.get(node.id());

            if (batch == null) {
                batch = new BatchFuture(node);

                batches.put(node.id(), batch);
            }

            if (batch.ready()) {
                // Can't advance further at the moment.
                peek = cur;

                break;
            }

            peek = null;

            batch.add(op == GridCacheOperation.DELETE ? cur.getKey() : new Object[] { cur.getKey(), cur.getValue()});

            cur = it.hasNextX() ? it.nextX() : null;

            if (batch.size() == batchSize) {
                batch.ready(true);

                if (res == null)
                    res = new ArrayList<>(batchSize);

                res.add(batch);
            }
        }

        it.beforeDetach();

        if (it.hasNext() || peek != null)
            return res;

        // No data left - flush incomplete batches.
        for (BatchFuture batch : batches.values()) {
            if (!batch.ready()) {
                if (res == null)
                    res = new ArrayList<>(batchSize);

                batch.ready(true);

                res.add(batch);
            }
        }

        return res;
    }

    /**
     *
     * @param node Node.
     * @param batch Batch.
     * @param first First mapping flag.
     */
    private void sendBatch(ClusterNode node, BatchFuture batch, boolean first) {
        GridDistributedTxMapping mapping = tx.mappings().get(node.id());

        if (mapping == null)
            tx.mappings().put(mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();

        boolean clientFirst = first && cctx.localNode().isClient() && !topLocked && !tx.hasRemoteLocks();

        add(batch);

        int batchId;

        synchronized (this) {
            batchId = futuresCountNoLock();
        }

        if (node.isLocal())
            enlistLocal(batchId, node.id(), batch, timeout);
        else
            sendBatch(batchId, node.id(), batch, clientFirst, timeout);
    }

    /**
     * Enlist batch of entries to the transaction on local node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batchFut batch mini-future.
     * @param timeout Timeout.
     */
    private void enlistLocal(int batchId,
        UUID nodeId,
        final BatchFuture batchFut,
        long timeout) {
        tx.init();

        Collection<Object> rows = batchFut.rows();

        GridNearTxQueryResultsEnlistRequest req = new GridNearTxQueryResultsEnlistRequest(cctx.cacheId(),
            threadId,
            futId,
            batchId,
            tx.subjectId(),
            topVer,
            lockVer,
            mvccSnapshot,
            false,
            timeout,
            tx.taskNameHash(),
            rows,
            op);

        GridDhtTxQueryResultsEnlistFuture fut = new GridDhtTxQueryResultsEnlistFuture(nodeId,
            lockVer,
            topVer,
            mvccSnapshot,
            threadId,
            futId,
            batchId,
            tx,
            timeout,
            cctx,
            rows,
            op);

        fut.listen(new CI1<IgniteInternalFuture<GridNearTxQueryResultsEnlistResponse>>() {
            @Override public void apply(IgniteInternalFuture<GridNearTxQueryResultsEnlistResponse> fut) {
                assert fut.error() != null || fut.result() != null : fut;

                try {
                    sendNextBatches(nodeId);

                    batchFut.onResult(fut.result(), fut.error());
                }
                finally {
                    cctx.io().onMessageProcessed(req);
                }
            }
        });

        fut.init();
    }

    /**
     * Send batch request to remote data node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batchFut Mini-future for the batch.
     * @param clientFirst {@code true} if originating node is client and it is a first request to any data node.
     * @param timeout Timeout.
     */
    private void sendBatch(int batchId,
        UUID nodeId,
        BatchFuture batchFut,
        boolean clientFirst,
        long timeout) {
        assert batchFut != null;

        GridNearTxQueryResultsEnlistRequest req = new GridNearTxQueryResultsEnlistRequest(cctx.cacheId(),
            threadId,
            futId,
            batchId,
            tx.subjectId(),
            topVer,
            lockVer,
            mvccSnapshot,
            clientFirst,
            timeout,
            tx.taskNameHash(),
            batchFut.rows(),
            op);

        try {
            cctx.io().send(nodeId, req, cctx.ioPolicy());
        }
        catch (IgniteCheckedException ex) {
            batchFut.onResult(null, ex);
        }
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearTxQueryResultsEnlistResponse res) {
        BatchFuture batchFut = batchFuture(res.miniId());

        sendNextBatches(nodeId);

        if (batchFut != null)
            batchFut.onResult(res, null);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Long res, @Nullable Throwable err) {
        if (!DONE_UPD.compareAndSet(this, 0, 1))
            return false;

        cctx.tm().txContext(tx);

        if (err != null)
            tx.setRollbackOnly();

        if (!X.hasCause(err, IgniteTxTimeoutCheckedException.class) && tx.trackTimeout()) {
            // Need restore timeout before onDone is called and next tx operation can proceed.
            boolean add = tx.addTimeoutHandler();

            assert add;
        }

        if (super.onDone(res, err)) {
            U.close(it, log);

            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     * Finds pending batch future by the given ID.
     *
     * @param batchId Batch ID to find.
     * @return Batch future.
     */
    private BatchFuture batchFuture(int batchId) {
        synchronized (this) {
            int idx = Math.abs(batchId) - 1;

            assert idx >= 0 && idx < futuresCountNoLock();

            IgniteInternalFuture<Long> fut = future(idx);

            if (!fut.isDone())
                return (BatchFuture)fut;
        }

        return null;
    }


    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures()) {
            BatchFuture f = (BatchFuture)fut;

            if (f.node.id().equals(nodeId)) {
                if (log.isDebugEnabled())
                    log.debug("Found mini-future for left node [nodeId=" + nodeId + ", mini=" + f + ", fut=" +
                        this + ']');

                ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to enlist keys " +
                    "(primary node left grid, retry transaction if possible) [node=" + nodeId + ']');

                topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

                return f.onResult(null, topEx);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onResponse(UUID crdId, MvccSnapshot res) {
        mvccSnapshot = res;

        if (tx != null)
            tx.mvccInfo(new MvccTxInfo(crdId, res));
    }

    /** {@inheritDoc} */
    @Override public void onError(IgniteCheckedException e) {
        onDone(e);
    }

    /** {@inheritDoc} */
    @Override protected void logError(IgniteLogger log, String msg, Throwable e) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override protected void logDebug(IgniteLogger log, String msg) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryResultsEnlistFuture.class, this, super.toString());
    }

    /**
     * A future tracking a request for a batch of rows
     * to be enlisted in transaction and locked on data node.
     */
    private class BatchFuture extends GridFutureAdapter<Long> {
        /** */
        private boolean completed;

        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** Rows. */
        private ArrayList<Object> rows = new ArrayList<>();

        /** Readiness flag. Set when batch is full or no new rows are expected. */
        private boolean ready;

        /**
         * @param node Cluster node.
         */
        private BatchFuture(ClusterNode node) {
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
         */
        public void add(Object row) {
            rows.add(row);
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

        /**
         * @param res Response.
         * @param err Exception.
         * @return {@code True} if future was completed by this call.
         */
        public boolean onResult(GridNearTxQueryResultsEnlistResponse res, Throwable err) {
            assert res != null || err != null : this;

            synchronized (this) {
                if (completed)
                    return false;

                completed = true;
            }

            if (X.hasCause(err, ClusterTopologyCheckedException.class)
                || (res != null && res.removeMapping())) {
                assert tx.mappings().get(node.id()).empty();

                tx.removeMapping(node.id());
            }
            else if (res != null && res.result() > 0) {
                if (node.isLocal())
                    tx.colocatedLocallyMapped(true);
                else
                    tx.hasRemoteLocks(true);
            }

            return err != null ? onDone(err) : onDone(res.result(), res.error());
        }
    }

    /**
     * Lock request timeout object.
     */
    private class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            onDone(new IgniteTxTimeoutCheckedException("Failed to acquire lock within provided timeout for " +
                "transaction [timeout=" + tx.timeout() + ", tx=" + tx + ']'));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
