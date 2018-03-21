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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Objects;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryEnlistResponse;
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
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Cache lock future.
 */
public final class GridDhtTxQueryEnlistFuture extends GridCacheFutureAdapter<GridNearTxQueryEnlistResponse>
    implements GridCacheVersionedFuture<GridNearTxQueryEnlistResponse> {

    /** Involved cache ids. */
    private final int[] cacheIds;

    /** Partitions. */
    private final int[] parts;

    /** Schema name. */
    private final String schema;

    /** Query string. */
    private final String qry;

    /** Query parameters. */
    private final Object[] params;

    /** Flags. */
    private final int flags;

    /** Fetch page size. */
    private final int pageSize;

    /** Processed entries count. */
    private long cnt;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Near lock version. */
    private GridCacheVersion nearLockVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Future ID. */
    private IgniteUuid futId;

    /** Future ID. */
    private IgniteUuid nearFutId;

    /** Future ID. */
    private int nearMiniId;

    /** Transaction. */
    private GridDhtTxLocalAdapter tx;

    /** Lock timeout. */
    private final long timeout;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<?, ?> cctx;

    /** Logger. */
    @GridToStringExclude
    private IgniteLogger log;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Query iterator */
    private UpdateSourceIterator<?> it;

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param tx Transaction.
     * @param cacheIds Involved cache ids.
     * @param parts Partitions.
     * @param schema Schema name.
     * @param qry Query string.
     * @param params Query parameters.
     * @param flags Flags.
     * @param pageSize Fetch page size.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     */
    public GridDhtTxQueryEnlistFuture(
        UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        int[] cacheIds,
        int[] parts,
        String schema,
        String qry,
        Object[] params,
        int flags,
        int pageSize,
        long timeout,
        GridCacheContext<?, ?> cctx) {
        assert tx != null;
        assert timeout >= 0;
        assert nearNodeId != null;
        assert nearLockVer != null;
        assert topVer != null && topVer.topologyVersion() > 0;
        assert threadId == tx.threadId();

        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.mvccSnapshot = mvccSnapshot;
        this.topVer = topVer;
        this.cacheIds = cacheIds;
        this.parts = parts;
        this.schema = schema;
        this.qry = qry;
        this.params = params;
        this.flags = flags;
        this.pageSize = pageSize;
        this.timeout = timeout;
        this.tx = tx;

        tx.topologyVersion(topVer);

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        log = cctx.logger(GridDhtTxQueryEnlistFuture.class);
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
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

    /**
     *
     */
    public void init() {
        cctx.mvcc().addFuture(this);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        UpdateSourceIterator<?> it;

        try {
            checkPartitions();

            it = cctx.kernalContext().query()
                .prepareDistributedUpdate(cctx, cacheIds, parts, schema, qry,
                        params, flags, pageSize, 0, topVer, mvccSnapshot, new GridQueryCancel());

            if (!it.hasNext()) {
                GridNearTxQueryEnlistResponse res = createResponse(0);

                res.removeMapping(tx.empty());

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
                        cctx.shared().wal().fsync(ptr);

                    onDone(createResponse(cnt));

                    return;
                }

                Object row = it.next();
                KeyCacheObject key = key(row);

                GridDhtCacheEntry entry = cache.entryExx(key);

                if (log.isDebugEnabled())
                    log.debug("Adding entry: " + entry);

                assert !entry.detached();

                IgniteTxEntry txEntry = tx.entry(entry.txKey());

                if (txEntry != null) {
                    throw new IgniteSQLException("One row cannot be changed twice in the same transaction. " +
                            "Operation is unsupported at the moment.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                }

                Object[] row0 = row.getClass().isArray() ? (Object[])row : null;
                CacheObject val = row0 != null && (row0.length == 2 || row0.length == 4) ? cctx.toCacheObject(row0[1]) : null;
                EntryProcessor entryProcessor = row0 != null && row0.length == 4 ? (EntryProcessor)row0[2] : null;
                Object[] invokeArgs = entryProcessor != null ? (Object[])row0[3] : null;
                GridCacheOperation op = !row.getClass().isArray() ? DELETE : entryProcessor != null ? TRANSFORM : UPDATE;

                if (op == TRANSFORM) {
                    CacheObject oldVal = val;
                    CacheInvokeEntry invokeEntry;

                    while (true) {
                        entry.lockEntry();

                        if (entry.obsolete()) {
                            entry.unlockEntry();

                            entry = cache.entryExx(key);

                            continue;
                        }

                        break;
                    }

                    try {
                        // TODO move checking if entry is exist for INSERT operation to mvccSet method
                        if (oldVal == null)
                            oldVal = entry.innerGet(
                                null,
                                tx,
                                false,
                                false,
                                false,
                                tx.subjectId(),
                                null,
                                tx.resolveTaskName(),
                                null,
                                true,
                                mvccSnapshot);

                        invokeEntry = new CacheInvokeEntry(entry.key(), oldVal, entry.version(), true, entry);

                        entryProcessor.process(invokeEntry, invokeArgs);
                    } finally {
                        entry.unlockEntry();
                    }

                    val = cctx.toCacheObject(invokeEntry.value());

                    cctx.validateKeyAndValue(entry.key(), val);

                    if (oldVal == null && val != null)
                        op = CREATE;
                    else if (oldVal != null && val != null && invokeEntry.modified())
                        op = UPDATE;
                    else if (oldVal != null && val == null)
                        op = DELETE;
                    else
                        op = READ;
                }
                else if (op == UPDATE) {
                    assert val != null;

                    cctx.validateKeyAndValue(entry.key(), val);
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
                                    mvccSnapshot);
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
                    GridCacheOperation finalOp = op;
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

                                txEntry = tx.addEntry(finalOp,
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

                txEntry = tx.addEntry(op,
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

                txEntry.markValid();
                txEntry.queryEnlisted(true);
                txEntry.cached(entry);

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
     * Checks whether all the necessary partitions are in {@link GridDhtPartitionState#OWNING} state.
     * @throws ClusterTopologyCheckedException If failed.
     */
    private void checkPartitions() throws ClusterTopologyCheckedException {
        if(cctx.isLocal() || !cctx.rebalanceEnabled())
            return;

        int[] parts0 = parts;

        if (parts0 == null)
            parts0 = U.toIntArray(
                cctx.affinity()
                    .primaryPartitions(cctx.localNodeId(), topVer));

        GridDhtPartitionTopology top = cctx.topology();

        top.readLock();

        try {
            for (int i = 0; i < parts0.length; i++) {
                GridDhtLocalPartition p = top.localPartition(parts0[i]);

                if (p == null || p.state() != GridDhtPartitionState.OWNING)
                    throw new ClusterTopologyCheckedException("Cannot run update query. " +
                        "Node must own all the necessary partitions."); // TODO IGNITE-7185 Send retry instead.
            }
        }
        finally {
            top.readUnlock();
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
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        return nearNodeId.equals(nodeId) && onDone(
            new ClusterTopologyCheckedException("Requesting node left the grid [nodeId=" + nodeId + ']'));
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable GridNearTxQueryEnlistResponse res, @Nullable Throwable err) {
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
     * @return Prepare response.
     */
    @NotNull private GridNearTxQueryEnlistResponse createResponse(@NotNull Throwable err) {
        return new GridNearTxQueryEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, 0, err);
    }

    /**
     * @param res {@code True} if at least one entry was enlisted.
     * @return Prepare response.
     */
    @NotNull private GridNearTxQueryEnlistResponse createResponse(long res) {
        return new GridNearTxQueryEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, res, null);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridDhtTxQueryEnlistFuture future = (GridDhtTxQueryEnlistFuture)o;

        return Objects.equals(futId, future.futId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxQueryEnlistFuture.class, this);
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
