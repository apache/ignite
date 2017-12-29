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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheLockCandidates;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheLockTimeoutException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccLongList;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
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
    private final MvccVersion mvccVer;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Thread. */
    private long threadId;

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

    /** Pending locks. */
    @GridToStringExclude
    private final Collection<KeyCacheObject> pendingLocks;

    /** Keys locked so far. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    private List<GridDhtCacheEntry> entries;

    /** Query cancel object. */
    @GridToStringExclude
    private GridQueryCancel cancel;

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccVer Mvcc version.
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
        MvccVersion mvccVer,
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
        this.mvccVer = mvccVer;
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

        this.threadId = threadId;

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>();

        pendingLocks = new HashSet<>();

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

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            cancel.cancel();

        return isCancelled();
    }

    /**
     *
     */
    public void init() {
        cancel = new GridQueryCancel();

        cctx.mvcc().addFuture(this);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        GridDhtCacheAdapter<?, ?> cache = cctx.isNear() ? cctx.near().dht() : cctx.dht();

        try {
            checkPartitions();

            long cnt = 0;

            try (GridCloseableIterator<?> it = cctx.kernalContext().query()
                .prepareDistributedUpdate(cctx, cacheIds, parts, schema, qry, params, flags, pageSize, (int)timeout, topVer, mvccVer, cancel)) {
                while (it.hasNext()) {
                    Object row = it.next();

                    KeyCacheObject key = key(row);

                    while (true) {
                        if (isCancelled())
                            return;

                        GridDhtCacheEntry entry = cache.entryExx(key, topVer);

                        try {
                            addEntry(entry, row);

                            cnt++;

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry when adding lock (will retry): " + entry);
                        }
                        catch (GridDistributedLockCancelledException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to add entry [err=" + e + ", entry=" + entry + ']');

                            onDone(e);

                            return;
                        }
                    }
                }
            }

            if (cnt == 0) {
                GridNearTxQueryEnlistResponse res = createResponse(0);

                res.removeMapping(tx.empty());

                onDone(res);

                return;
            }

            tx.addActiveCache(cctx, false);

            this.cnt = cnt;
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;

            return;
        }

        readyLocks();
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

        try {
            top.readLock();

            for (int i = 0; i < parts0.length; i++) {
                GridDhtLocalPartition p = top.localPartition(parts0[i]);

                if (p == null || p.state() != GridDhtPartitionState.OWNING)
                    throw new ClusterTopologyCheckedException("Cannot run update query. " +
                        "Node must own all the necessary partitions."); // TODO IGNITE-4191 Send retry instead.
            }
        }
        finally {
            top.readUnlock();
        }
    }

    /**
     * Sets all local locks as ready.
     */
    private void readyLocks() {
        if (log.isDebugEnabled())
            log.debug("Marking local locks as ready for DHT lock future: " + this);

        for (int i = 0, size = entries.size(); i < size; i++) {
            while (true) {
                if (isDone())
                    return;

                GridDhtCacheEntry entry = entries.get(i);

                if (entry == null)
                    break; // While.

                try {
                    CacheLockCandidates owners = entry.readyLock(lockVer);

                    if (timeout < 0) {
                        if (owners == null || !owners.hasCandidate(lockVer)) {
                            String msg = "Failed to acquire lock with negative timeout: " + entry;

                            if (log.isDebugEnabled())
                                log.debug(msg);

                            onDone(new GridCacheLockTimeoutException(lockVer));

                            return;
                        }
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Current lock owners [entry=" + entry +
                            ", owners=" + owners +
                            ", fut=" + this + ']');
                    }

                    break; // Inner while loop.
                }
                // Possible in concurrent cases, when owner is changed after locks
                // have been released or cancelled.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to ready lock because entry was removed (will renew).");

                    entry = (GridDhtCacheEntry)cctx.cache().entryEx(entry.key(), topVer);

                    synchronized (this) {
                        entries.set(i, entry);
                    }
                }
            }
        }
    }

    /**
     * Undoes all locks.
     */
    private void undoLocks() {
        // Transactions will undo during rollback.
        Collection<GridDhtCacheEntry> entriesCp;

        synchronized (this) {
            entriesCp = new ArrayList<>(entries);
        }

        if (tx != null) {
            if (tx.setRollbackOnly()) {
                if (log.isDebugEnabled())
                    log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
            }
            else if (log.isDebugEnabled())
                log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
        }

        for (GridCacheEntryEx e : F.view(entriesCp, F.notNull())) {
            try {
                e.removeLock(lockVer);
            }
            catch (GridCacheEntryRemovedException ignored) {
                while (true) {
                    try {
                        e = cctx.cache().peekEx(e.key());

                        if (e != null)
                            e.removeLock(lockVer);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to remove lock on removed entry (will retry) [ver=" +
                                lockVer + ", entry=" + e + ']');
                    }
                }
            }
        }
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @param row Source row.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     * @throws GridDistributedLockCancelledException If lock is canceled.
     */
    @SuppressWarnings("unchecked")
    @Nullable private GridCacheMvccCandidate addEntry(GridDhtCacheEntry entry, Object row)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException, IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Adding entry: " + entry);

        if (entry == null)
            return null;

        // Check if the future is timed out.
        if (isCancelled())
            return null;

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
                    mvccVer);

            CacheInvokeEntry invokeEntry = new CacheInvokeEntry(entry.key(), oldVal, entry.version(), true, entry);

            entryProcessor.process(invokeEntry, invokeArgs);

            val = cctx.toCacheObject(invokeEntry.value());

            cctx.validateKeyAndValue(entry.key(), val);

            if (oldVal == null && val != null)
                op = CREATE;
            else if (oldVal != null && val == null)
                op = DELETE;
            else if (oldVal != null && val != null && invokeEntry.modified())
                op = UPDATE;
            else
                op = READ;
        }
        else if (op == UPDATE) {
            assert val != null;

            cctx.validateKeyAndValue(entry.key(), val);
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

        txEntry.cached(entry);
        txEntry.markValid();
        txEntry.queryEnlisted(true);

        if (tx.local() && !tx.dht())
            ((GridNearTxLocal)tx).colocatedLocallyMapped(true);

        GridCacheMvccCandidate c = entry.addDhtLocal(
            nearNodeId,
            nearLockVer,
            topVer,
            threadId,
            lockVer,
            null,
            timeout,
            false,
            true,
            false,
            false
        );

        if (c == null && timeout < 0) {

            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onDone(new GridCacheLockTimeoutException(lockVer));

            return null;
        }

        synchronized (this) {
            entries.add(c == null || c.reentry() ? null : entry);

            if (c != null && !c.reentry())
                pendingLocks.add(entry.key());
        }

        // Double check if the future has already timed out.
        if (isCancelled()) {
            entry.removeLock(lockVer);

            return null;
        }

        return c;
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

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (isDone() || tx.remainingTime() == -1)
            return false; // Check other futures.

        if (log.isDebugEnabled())
            log.debug("Received onOwnerChanged() callback [entry=" + entry + ", owner=" + owner + "]");

        if (owner != null && owner.version().equals(lockVer)) {
            try {
                if (!checkVersion(entry))
                    return false;
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                return false;
            }

            boolean done;

            synchronized (this) {
                if (!pendingLocks.remove(entry.key()))
                    return false;

                done = pendingLocks.isEmpty();
            }

            if(done)
                onDone(createResponse(cnt));

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable GridNearTxQueryEnlistResponse res, @Nullable Throwable err) {
        if (err != null)
            res = createResponse(err);

        if (super.onDone(res, null)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            if (((err = res.error()) != null && !X.hasCause(err, NodeStoppingException.class))) {
                GridQueryCancel cancel = this.cancel;

                if(cancel != null)
                    cancel.cancel();

                undoLocks();
            }

            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            synchronized (this) {
                pendingLocks.clear();
            }

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     * @param entry Cache entry.
     * @return {@code True} if entry has not been changed since mvcc version was acquired.
     * @throws IgniteCheckedException If failed.
     */
    private boolean checkVersion(GridCacheEntryEx entry) throws IgniteCheckedException {
        MvccVersion ver = cctx.offheap().findMaxMvccVersion(cctx, entry.key());

        if (ver == null)
            return true;

        int cmp = Long.compare(ver.coordinatorVersion(), mvccVer.coordinatorVersion());

        if (cmp == 0) {
            cmp = Long.compare(ver.counter(), mvccVer.counter());

            if (cmp < 0) {
                MvccLongList txs = mvccVer.activeTransactions();

                if (txs != null && txs.contains(ver.counter()))
                    cmp = 1;
            }
        }

        if (cmp > 0) {
            onDone(new IgniteCheckedException("Mvcc version mismatch."));

            return false;
        }

        return true;
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
        HashSet<KeyCacheObject> pending;

        synchronized (this) {
            pending = new HashSet<>(pendingLocks);
        }

        return S.toString(GridDhtTxQueryEnlistFuture.class, this,
            "pendingLocks", pending,
            "super", super.toString());
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

            onDone(new GridCacheLockTimeoutException(lockVer));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
