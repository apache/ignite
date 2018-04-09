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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Abstract future processing transaction enlisting and locking
 * of entries produced with DML queries.
 */
public abstract class GridDhtTxQueryEnlistAbstractFuture<T extends GridCacheIdMessage> extends GridCacheFutureAdapter<T>
    implements GridCacheVersionedFuture<T> {

    /** Future ID. */
    protected IgniteUuid futId;

    /** Cache registry. */
    @GridToStringExclude
    protected GridCacheContext<?, ?> cctx;

    /** Logger. */
    @GridToStringExclude
    protected IgniteLogger log;

    /** Thread. */
    protected long threadId;

    /** Future ID. */
    IgniteUuid nearFutId;

    /** Future ID. */
    int nearMiniId;

    /** Partitions. */
    protected final int[] parts;

    /** Transaction. */
    protected GridDhtTxLocalAdapter tx;

    /** Lock version. */
    protected GridCacheVersion lockVer;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** */
    protected final MvccSnapshot mvccSnapshot;

    /** Processed entries count. */
    protected long cnt;

    /** Near node ID. */
    protected UUID nearNodeId;

    /** Near lock version. */
    GridCacheVersion nearLockVer;

    /** Timeout object. */
    @GridToStringExclude
    protected LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    protected final long timeout;

    /** Trackable flag. */
    protected boolean trackable = true;

    /** Query cancel object. */
    @GridToStringExclude
    protected GridQueryCancel cancel;

    /** Query iterator */
    private UpdateSourceIterator<?> it;

    /**
     *
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param parts Partitions.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     */
    GridDhtTxQueryEnlistAbstractFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        @Nullable int[] parts,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx) {
        assert tx != null;
        assert timeout >= 0;
        assert nearNodeId != null;
        assert nearLockVer != null;
        assert topVer != null && topVer.topologyVersion() > 0;
        assert threadId == tx.threadId();

        this.threadId = threadId;
        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.mvccSnapshot = mvccSnapshot;
        this.topVer = topVer;
        this.timeout = timeout;
        this.tx = tx;
        this.parts = parts;

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        log = cctx.logger(GridDhtTxQueryEnlistAbstractFuture.class);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            cancel.cancel();

        return isCancelled();
    }

    /**
     * @return iterator.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract UpdateSourceIterator<?> createIterator() throws IgniteCheckedException;

    /**
     *
     */
    public void init() {
        cctx.mvcc().addFuture(this);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        try {
            checkPartitions(parts);

            UpdateSourceIterator<?> it = createIterator();

            if (!it.hasNext()) {
                T res = createResponse(0, tx.empty());

                U.close(it, log);

                onDone(res);

                return;
            }

            tx.addActiveCache(cctx, false);

            this.it = it;
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;

            return;
        }

        continueLoop(null);
    }

    /** */
    @SuppressWarnings("unchecked")
    private void continueLoop(WALPointer ptr) {
        if (isDone())
            return;

        GridDhtCacheAdapter cache = cctx.dhtCache();

        try {
            while (true) {
                if (!it.hasNext()) {
                    if (ptr != null && !cctx.tm().logTxRecords())
                        cctx.shared().wal().flush(ptr, true);

                    onDone(createResponse(cnt, false));

                    return;
                }

                Object row = it.next();
                KeyCacheObject key = key(row);

                GridDhtCacheEntry entry = cache.entryExx(key);

                if (log.isDebugEnabled())
                    log.debug("Adding entry: " + entry);

                assert !entry.detached();

                GridCacheOperation op = it.operation();

                Object[] row0 = row.getClass().isArray() ? (Object[])row : null;

                CacheObject val = null;

                if (op == CREATE || op == UPDATE) {
                    assert row0 != null;

                    val = cctx.toCacheObject(row0[1]);
                }

                GridCacheUpdateTxResult res;

                while (true) {
                    cctx.shared().database().checkpointReadLock();

                    try {
                        if (op == DELETE)
                            res = entry.mvccRemove(
                                tx,
                                cctx.localNodeId(),
                                topVer,
                                null,
                                mvccSnapshot);
                        else if (op == CREATE || op == UPDATE)
                            res = entry.mvccSet(
                                tx,
                                cctx.localNodeId(),
                                val,
                                0,
                                topVer,
                                null,
                                mvccSnapshot,
                                op);
                        else
                            throw new IgniteSQLException("Cannot acquire lock for operation [op= " + op + "]" + // TODO SELECT FOR UPDATE
                                "Operation is unsupported at the moment ", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                        break;
                    } catch (GridCacheEntryRemovedException ignored) {
                        entry = cctx.dhtCache().entryExx(entry.key(), topVer);
                    }
                    finally {
                        cctx.shared().database().checkpointReadUnlock();
                    }
                }

                ptr = res.loggedPointer();

                IgniteInternalFuture<GridCacheUpdateTxResult> updateFuture = res.updateFuture();

                if (updateFuture != null) {
                    CacheObject finalVal = val;
                    GridDhtCacheEntry finalEntry = entry;

                    it.beforeDetach();

                    updateFuture.listen(new CI1<IgniteInternalFuture<GridCacheUpdateTxResult>>() {
                        @Override public void apply(IgniteInternalFuture<GridCacheUpdateTxResult> fut) {
                            try {
                                GridCacheUpdateTxResult res = fut.get();

                                assert res.updateFuture() == null;

                                IgniteTxEntry txEntry = tx.entry(finalEntry.txKey());

                                if (txEntry != null) {
                                    throw new IgniteSQLException("One row cannot be changed twice in the same transaction. " +
                                        "Operation is unsupported at the moment.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                                }

                                txEntry = tx.addEntry(op,
                                    finalVal,
                                    null,
                                    null,
                                    finalEntry,
                                    null,
                                    CU.empty0(),
                                    false,
                                    -1L,
                                    -1L,
                                    null,
                                    true,
                                    true,
                                    false);

                                txEntry.markValid();
                                txEntry.queryEnlisted(true);
                                txEntry.cached(finalEntry);

                                cnt++;

                                continueLoop(res.loggedPointer());
                            } catch (Throwable e) {
                                onDone(e);
                            }
                        }
                    });

                    break;
                }

                IgniteTxEntry txEntry = tx.addEntry(op,
                    val,
                    null,
                    null,
                    entry,
                    null,
                    CU.empty0(),
                    false,
                    -1L,
                    -1L,
                    null,
                    true,
                    true,
                    false);

                txEntry.queryEnlisted(true);
                txEntry.markValid();

                cnt++;
            }
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @param row Query result row.
     * @return Extracted key.
     */
    private KeyCacheObject key(Object row) {
        return cctx.toCacheKeyObject(row.getClass().isArray() ? ((Object[])row)[0] : row);
    }


    /**
     * Checks whether all the necessary partitions are in {@link GridDhtPartitionState#OWNING} state.
     *
     * @param parts Partitions.
     * @throws ClusterTopologyCheckedException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void checkPartitions(@Nullable int[] parts) throws ClusterTopologyCheckedException {
        if(cctx.isLocal() || !cctx.rebalanceEnabled())
            return;

        if (parts == null)
            parts = U.toIntArray(
                cctx.affinity()
                    .primaryPartitions(cctx.localNodeId(), topVer));

        GridDhtPartitionTopology top = cctx.topology();

        try {
            top.readLock();

            for (int i = 0; i < parts.length; i++) {
                GridDhtLocalPartition p = top.localPartition(parts[i]);

                if (p == null || p.state() != GridDhtPartitionState.OWNING)
                    throw new ClusterTopologyCheckedException("Cannot run update query. " +
                        "Node must own all the necessary partitions."); // TODO IGNITE-7185 Send retry instead.
            }
        }
        finally {
            top.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @return Future ID.
     */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        return nearNodeId.equals(nodeId) && onDone(
            new ClusterTopologyCheckedException("Requesting node left the grid [nodeId=" + nodeId + ']'));
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable T res, @Nullable Throwable err) {
        assert res != null ^ err != null;

        if (err != null)
            res = createResponse(err);

        assert res != null;

        if (super.onDone(res, null)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            U.close(it, log);

            return true;
        }

        return false;
    }

    /**
     * @param err Error.
     * @return Prepared response.
     */
    public abstract T createResponse(@NotNull Throwable err);

    /**
     * @param cnt update count.
     * @param removeMapping {@code true} if tx mapping shall be removed.
     * @return Prepared response.
     */
    public abstract T createResponse(long cnt, boolean removeMapping);

    /**
     * Lock request timeout object.
     */
    protected class LockTimeoutObject extends GridTimeoutObjectAdapter {
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
