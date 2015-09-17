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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFuture;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;

/**
 * Cache lock future.
 */
public final class GridDhtLockFuture extends GridCompoundIdentityFuture<Boolean>
    implements GridCacheMvccFuture<Boolean>, GridDhtFuture<Boolean>, GridCacheMappedVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<?, ?> cctx;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Near lock version. */
    private GridCacheVersion nearLockVer;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Thread. */
    private long threadId;

    /** Keys locked so far. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    private List<GridDhtCacheEntry> entries;

    /** DHT mappings. */
    private Map<ClusterNode, List<GridDhtCacheEntry>> dhtMap =
        new ConcurrentHashMap8<>();

    /** Future ID. */
    private IgniteUuid futId;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Read flag. */
    private boolean read;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Timed out flag. */
    private volatile boolean timedOut;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private long timeout;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    /** Transaction. */
    private GridDhtTxLocalAdapter tx;

    /** All replies flag. */
    private AtomicBoolean mapped = new AtomicBoolean(false);

    /** */
    private Collection<Integer> invalidParts = new GridLeanSet<>();

    /** Trackable flag. */
    private boolean trackable = true;

    /** Mutex. */
    private final Object mux = new Object();

    /** Pending locks. */
    private final Collection<KeyCacheObject> pendingLocks = new GridConcurrentHashSet<>();

    /** TTL for read operation. */
    private long accessTtl;

    /** Need return value flag. */
    private boolean needReturnVal;

    /** Skip store flag. */
    private final boolean skipStore;

    /**
     * @param cctx Cache context.
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param cnt Number of keys to lock.
     * @param read Read flag.
     * @param needReturnVal Need return value flag.
     * @param timeout Lock acquisition timeout.
     * @param tx Transaction.
     * @param threadId Thread ID.
     * @param accessTtl TTL for read operation.
     * @param filter Filter.
     * @param skipStore Skip store flag.
     */
    public GridDhtLockFuture(
        GridCacheContext<?, ?> cctx,
        UUID nearNodeId,
        GridCacheVersion nearLockVer,
        @NotNull AffinityTopologyVersion topVer,
        int cnt,
        boolean read,
        boolean needReturnVal,
        long timeout,
        GridDhtTxLocalAdapter tx,
        long threadId,
        long accessTtl,
        CacheEntryPredicate[] filter,
        boolean skipStore) {
        super(cctx.kernalContext(), CU.boolReducer());

        assert nearNodeId != null;
        assert nearLockVer != null;
        assert topVer.topologyVersion() > 0;

        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.topVer = topVer;
        this.read = read;
        this.needReturnVal = needReturnVal;
        this.timeout = timeout;
        this.filter = filter;
        this.tx = tx;
        this.accessTtl = accessTtl;
        this.skipStore = skipStore;

        if (tx != null)
            tx.topologyVersion(topVer);

        assert tx == null || threadId == tx.threadId();

        this.threadId = threadId;

        if (tx != null)
            lockVer = tx.xidVersion();
        else {
            lockVer = cctx.mvcc().mappedVersion(nearLockVer);

            if (lockVer == null)
                lockVer = nearLockVer;
        }

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>(cnt);

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtLockFuture.class);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param cacheCtx Cache context.
     * @param invalidPart Partition to retry.
     */
    void addInvalidPartition(GridCacheContext<?, ?> cacheCtx, int invalidPart) {
        invalidParts.add(invalidPart);

        // Register invalid partitions with transaction.
        if (tx != null)
            tx.addInvalidPartition(cacheCtx, invalidPart);

        if (log.isDebugEnabled())
            log.debug("Added invalid partition to future [invalidPart=" + invalidPart + ", fut=" + this + ']');
    }

    /**
     * @return Participating nodes.
     */
    @Override public Collection<? extends ClusterNode> nodes() {
        return F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<?>, ClusterNode>() {
            @Nullable @Override public ClusterNode apply(IgniteInternalFuture<?> f) {
                if (isMini(f))
                    return ((MiniFuture)f).node();

                return cctx.discovery().localNode();
            }
        });
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
     * @return Entries.
     */
    public Collection<GridDhtCacheEntry> entries() {
        return F.view(entries, F.notNull());
    }

    /**
     * @return Entries.
     */
    public Collection<GridDhtCacheEntry> entriesCopy() {
        synchronized (mux) {
            return new ArrayList<>(entries());
        }
    }

    /**
     * @return Future ID.
     */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Near lock version.
     */
    public GridCacheVersion nearLockVersion() {
        return nearLockVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheVersion mappedVersion() {
        return tx == null ? nearLockVer : null;
    }

    /**
     * @return {@code True} if transaction is not {@code null}.
     */
    private boolean inTx() {
        return tx != null;
    }

    /**
     * @return {@code True} if transaction is implicit.
     */
    private boolean implicitSingle() {
        return tx != null && tx.implicitSingle();
    }

    /**
     * @return {@code True} if transaction is not {@code null} and has invalidate flag set.
     */
    private boolean isInvalidate() {
        return tx != null && tx.isInvalidate();
    }

    /**
     * @return Transaction isolation or {@code null} if no transaction.
     */
    @Nullable private TransactionIsolation isolation() {
        return tx == null ? null : tx.isolation();
    }

    /**
     * @param cached Entry.
     * @return {@code True} if locked.
     * @throws GridCacheEntryRemovedException If removed.
     */
    private boolean locked(GridCacheEntryEx cached) throws GridCacheEntryRemovedException {
        return (cached.lockedLocally(lockVer) && filter(cached)); // If filter failed, lock is failed.
    }

    /**
     * @param cached Entry.
     * @param owner Lock owner.
     * @return {@code True} if locked.
     */
    private boolean locked(GridCacheEntryEx cached, GridCacheMvccCandidate owner) {
        // Reentry-aware check (if filter failed, lock is failed).
        return owner != null && owner.matches(lockVer, cctx.nodeId(), threadId) && filter(cached);
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     * @throws GridDistributedLockCancelledException If lock is canceled.
     */
    @Nullable public GridCacheMvccCandidate addEntry(GridDhtCacheEntry entry)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException {
        if (log.isDebugEnabled())
            log.debug("Adding entry: " + entry);

        if (entry == null)
            return null;

        // Check if the future is timed out.
        if (timedOut)
            return null;

        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate c = entry.addDhtLocal(
            nearNodeId,
            nearLockVer,
            topVer,
            threadId,
            lockVer,
            timeout,
            /*reenter*/false,
            inTx(),
            implicitSingle()
        );

        if (c == null && timeout < 0) {
            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onFailed(false);

            return null;
        }

        synchronized (mux) {
            entries.add(c == null || c.reentry() ? null : entry);
        }

        if (c != null && !c.reentry())
            pendingLocks.add(entry.key());

        // Double check if the future has already timed out.
        if (timedOut) {
            entry.removeLock(lockVer);

            return null;
        }

        return c;
    }

    /**
     * Undoes all locks.
     *
     * @param dist If {@code true}, then remove locks from remote nodes as well.
     */
    private void undoLocks(boolean dist) {
        // Transactions will undo during rollback.
        Collection<GridDhtCacheEntry> entriesCp = entriesCopy();

        if (dist && tx == null) {
            cctx.dhtTx().removeLocks(nearNodeId, lockVer, F.viewReadOnly(entriesCp,
                new C1<GridDhtCacheEntry, KeyCacheObject>() {
                    @Override public KeyCacheObject apply(GridDhtCacheEntry e) {
                        return e.key();
                    }
                }), false);
        }
        else {
            if (tx != null) {
                if (tx.setRollbackOnly()) {
                    if (log.isDebugEnabled())
                        log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
                }
                else if (log.isDebugEnabled())
                    log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
            }

            for (GridCacheEntryEx e : entriesCp) {
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
    }

    /**
     *
     * @param dist {@code True} if need to distribute lock release.
     */
    private void onFailed(boolean dist) {
        undoLocks(dist);

        onComplete(false);
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new ClusterTopologyCheckedException("Remote node left grid (will ignore): " + nodeId));

                    found = true;
                }
            }
        }

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    void onResult(UUID nodeId, GridDhtLockResponse res) {
        if (!isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received lock response from node [nodeId=" + nodeId + ", res=" + res + ", fut=" + this + ']');

            boolean found = false;

            for (IgniteInternalFuture<Boolean> fut : pending()) {
                if (isMini(fut)) {
                    MiniFuture mini = (MiniFuture)fut;

                    if (mini.futureId().equals(res.miniId())) {
                        assert mini.node().id().equals(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Found mini future for response [mini=" + mini + ", res=" + res + ']');

                        found = true;

                        mini.onResult(res);

                        if (log.isDebugEnabled())
                            log.debug("Futures after processed lock response [fut=" + this + ", mini=" + mini +
                                ", res=" + res + ']');

                        break;
                    }
                }
            }

            if (!found)
                U.warn(log, "Failed to find mini future for response (perhaps due to stale message) [res=" + res +
                    ", fut=" + this + ']');
        }
    }

    /**
     * Sets all local locks as ready. After local locks are acquired, lock requests will be sent to remote nodes.
     * Thus, no reordering will occur for remote locks as they are added after local locks are acquired.
     */
    private void readyLocks() {
        if (log.isDebugEnabled())
            log.debug("Marking local locks as ready for DHT lock future: " + this);

        for (int i = 0; i < entries.size(); i++) {
            while (true) {
                GridDistributedCacheEntry entry = entries.get(i);

                if (entry == null)
                    break; // While.

                try {
                    GridCacheMvccCandidate owner = entry.readyLock(lockVer);

                    if (timeout < 0) {
                        if (owner == null || !owner.version().equals(lockVer)) {
                            // We did not send any requests yet.
                            onFailed(false);

                            return;
                        }
                    }

                    if (log.isDebugEnabled()) {
                        if (!locked(entry, owner))
                            log.debug("Entry is not locked (will keep waiting) [entry=" + entry +
                                ", fut=" + this + ']');
                    }

                    break; // Inner while loop.
                }
                // Possible in concurrent cases, when owner is changed after locks
                // have been released or cancelled.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to ready lock because entry was removed (will renew).");

                    entries.set(i, (GridDhtCacheEntry)cctx.cache().entryEx(entry.key(), topVer));
                }
            }
        }
    }

    /**
     * @param e Error.
     */
    public void onError(GridDistributedLockCancelledException e) {
        if (err.compareAndSet(null, e))
            onComplete(false);
    }

    /**
     * @param t Error.
     */
    public void onError(Throwable t) {
        if (err.compareAndSet(null, t))
            onComplete(false);
    }

    /**
     * @param cached Entry to check.
     * @return {@code True} if filter passed.
     */
    private boolean filter(GridCacheEntryEx cached) {
        try {
            if (!cctx.isAll(cached, filter)) {
                if (log.isDebugEnabled())
                    log.debug("Filter didn't pass for entry (will fail lock): " + cached);

                onFailed(true);

                return false;
            }

            return true;
        }
        catch (IgniteCheckedException e) {
            onError(e);

            return false;
        }
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (isDone())
            return false; // Check other futures.

        if (log.isDebugEnabled())
            log.debug("Received onOwnerChanged() callback [entry=" + entry + ", owner=" + owner + "]");

        if (owner != null && owner.version().equals(lockVer)) {
            pendingLocks.remove(entry.key());

            if (checkLocks())
                map(entries());

            return true;
        }

        return false;
    }

    /**
     * @return {@code True} if locks have been acquired.
     */
    private boolean checkLocks() {
        return pendingLocks.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            onComplete(false);

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Boolean success, @Nullable Throwable err) {
        // Protect against NPE.
        if (success == null) {
            assert err != null;

            success = false;
        }

        assert err == null || !success;
        assert !success || (initialized() && !hasPending()) : "Invalid done callback [success=" + success +
            ", fut=" + this + ']';

        if (log.isDebugEnabled())
            log.debug("Received onDone(..) callback [success=" + success + ", err=" + err + ", fut=" + this + ']');

        // If locks were not acquired yet, delay completion.
        if (isDone() || (err == null && success && !checkLocks()))
            return false;

        this.err.compareAndSet(null, err);

        return onComplete(success);
    }

    /**
     * Completeness callback.
     *
     * @param success {@code True} if lock was acquired.
     * @return {@code True} if complete by this operation.
     */
    private boolean onComplete(boolean success) {
        if (log.isDebugEnabled())
            log.debug("Received onComplete(..) callback [success=" + success + ", fut=" + this + ']');

        if (!success)
            undoLocks(true);

        if (tx != null)
            cctx.tm().txContext(tx);

        if (err.get() == null)
            loadMissingFromStore();

        if (super.onDone(success, err.get())) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     *
     */
    public void map() {
        if (F.isEmpty(entries)) {
            onComplete(true);

            return;
        }

        readyLocks();
    }

    /**
     * @param entries Entries.
     */
    private void map(Iterable<GridDhtCacheEntry> entries) {
        if (!mapped.compareAndSet(false, true)) {
            if (log.isDebugEnabled())
                log.debug("Will not map DHT lock future (other thread is mapping): " + this);

            return;
        }

        try {
            if (log.isDebugEnabled())
                log.debug("Mapping entry for DHT lock future: " + this);

            boolean hasRmtNodes = false;

            // Assign keys to primary nodes.
            for (GridDhtCacheEntry entry : entries) {
                try {
                    while (true) {
                        try {
                            hasRmtNodes = cctx.dhtMap(
                                nearNodeId,
                                topVer,
                                entry,
                                tx == null ? lockVer : null,
                                log,
                                dhtMap,
                                null);

                            GridCacheMvccCandidate cand = entry.candidate(lockVer);

                            // Possible in case of lock cancellation.
                            if (cand == null) {
                                onFailed(false);

                                // Will mark initialized in finally block.
                                return;
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry when mapping DHT lock future (will retry): " + entry);

                            entry = cctx.dht().entryExx(entry.key(), topVer);
                        }
                    }
                }
                catch (GridDhtInvalidPartitionException e) {
                    assert false : "DHT lock should never get invalid partition [err=" + e + ", fut=" + this + ']';
                }
            }

            if (tx != null)
                tx.needsCompletedVersions(hasRmtNodes);

            if (isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Mapping won't proceed because future is done: " + this);

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Mapped DHT lock future [dhtMap=" + F.nodeIds(dhtMap.keySet()) + ", dhtLockFut=" + this + ']');

            // Create mini futures.
            for (Map.Entry<ClusterNode, List<GridDhtCacheEntry>> mapped : dhtMap.entrySet()) {
                ClusterNode n = mapped.getKey();

                List<GridDhtCacheEntry> dhtMapping = mapped.getValue();

                int cnt = F.size(dhtMapping);

                if (cnt > 0) {
                    assert !n.id().equals(cctx.localNodeId());

                    MiniFuture fut = new MiniFuture(n, dhtMapping);

                    GridDhtLockRequest req = new GridDhtLockRequest(
                        cctx.cacheId(),
                        nearNodeId,
                        inTx() ? tx.nearXidVersion() : null,
                        threadId,
                        futId,
                        fut.futureId(),
                        lockVer,
                        topVer,
                        inTx(),
                        read,
                        isolation(),
                        isInvalidate(),
                        timeout,
                        cnt,
                        0,
                        inTx() ? tx.size() : cnt,
                        inTx() ? tx.subjectId() : null,
                        inTx() ? tx.taskNameHash() : 0,
                        read ? accessTtl : -1L,
                        skipStore);

                    try {
                        for (ListIterator<GridDhtCacheEntry> it = dhtMapping.listIterator(); it.hasNext();) {
                            GridDhtCacheEntry e = it.next();

                            boolean needVal = false;

                            try {
                                // Must unswap entry so that isNewLocked returns correct value.
                                e.unswap(false);

                                needVal = e.isNewLocked();

                                if (needVal) {
                                    List<ClusterNode> owners = cctx.topology().owners(e.partition(),
                                        tx != null ? tx.topologyVersion() : cctx.affinity().affinityTopologyVersion());

                                    // Do not preload if local node is partition owner.
                                    if (owners.contains(cctx.localNode()))
                                        needVal = false;
                                }
                            }
                            catch (GridCacheEntryRemovedException ex) {
                                assert false : "Entry cannot become obsolete when DHT local candidate is added " +
                                    "[e=" + e + ", ex=" + ex + ']';
                            }

                            // Skip entry if it is not new and is not present in updated mapping.
                            if (tx != null && !needVal)
                                continue;

                            boolean invalidateRdr = e.readerId(n.id()) != null;

                            req.addDhtKey(e.key(), invalidateRdr, cctx);

                            if (needVal) {
                                // Mark last added key as needed to be preloaded.
                                req.markLastKeyForPreload();

                                if (tx != null) {
                                    IgniteTxEntry txEntry = tx.entry(e.txKey());

                                    // NOOP entries will be sent to backups on prepare step.
                                    if (txEntry.op() == GridCacheOperation.READ)
                                        txEntry.op(GridCacheOperation.NOOP);
                                }
                            }

                            it.set(addOwned(req, e));
                        }

                        if (!F.isEmpty(req.keys())) {
                            if (tx != null)
                                tx.addLockTransactionNode(n);

                            add(fut); // Append new future.

                            if (log.isDebugEnabled())
                                log.debug("Sending DHT lock request to DHT node [node=" + n.id() + ", req=" + req + ']');

                            cctx.io().send(n, req, cctx.ioPolicy());
                        }
                    }
                    catch (IgniteCheckedException e) {
                        // Fail the whole thing.
                        if (e instanceof ClusterTopologyCheckedException)
                            fut.onResult((ClusterTopologyCheckedException)e);
                        else
                            fut.onResult(e);
                    }
                }
            }
        }
        finally {
            markInitialized();
        }
    }

    /**
     * @param req Request.
     * @param e Entry.
     * @return Entry.
     */
    private GridDhtCacheEntry addOwned(GridDhtLockRequest req, GridDhtCacheEntry e) {
        while (true) {
            try {
                GridCacheMvccCandidate added = e.candidate(lockVer);

                assert added != null;
                assert added.dhtLocal();

                if (added.ownerVersion() != null)
                    req.owned(e.key(), added.ownerVersion());

                break;
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Got removed entry when creating DHT lock request (will retry): " + e);

                e = cctx.dht().entryExx(e.key(), topVer);
            }
        }

        return e;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockFuture.class, this, super.toString());
    }

    /**
     *
     */
    private void loadMissingFromStore() {
        if (!skipStore && (read || cctx.loadPreviousValue()) && cctx.readThrough() && (needReturnVal || read)) {
            final Map<KeyCacheObject, GridDhtCacheEntry> loadMap = new LinkedHashMap<>();

            final GridCacheVersion ver = version();

            for (GridDhtCacheEntry entry : entries) {
                if (!entry.hasValue())
                    loadMap.put(entry.key(), entry);
            }

            try {
                cctx.store().loadAll(
                    null,
                    loadMap.keySet(),
                    new CI2<KeyCacheObject, Object>() {
                        @Override public void apply(KeyCacheObject key, Object val) {
                            // No value loaded from store.
                            if (val == null)
                                return;

                            GridDhtCacheEntry entry0 = loadMap.get(key);

                            try {
                                CacheObject val0 = cctx.toCacheObject(val);

                                entry0.initialValue(val0, ver, 0, 0, false, topVer, GridDrType.DR_LOAD);
                            }
                            catch (GridCacheEntryRemovedException e) {
                                assert false : "Should not get removed exception while holding lock on entry " +
                                    "[entry=" + entry0 + ", e=" + e + ']';
                            }
                            catch (IgniteCheckedException e) {
                                onDone(e);
                            }
                        }
                    });
            }
            catch (IgniteCheckedException e) {
                onDone(e);
            }
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
        @SuppressWarnings({"ThrowableInstanceNeverThrown"})
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            timedOut = true;

            onComplete(false);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Node. */
        @GridToStringExclude
        private ClusterNode node;

        /** DHT mapping. */
        @GridToStringInclude
        private List<GridDhtCacheEntry> dhtMapping;

        /**
         * @param node Node.
         * @param dhtMapping Mapping.
         */
        MiniFuture(ClusterNode node, List<GridDhtCacheEntry> dhtMapping) {
            assert node != null;

            this.node = node;
            this.dhtMapping = dhtMapping;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Node failure.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will ignore): " + this);

            if (tx != null)
                tx.removeMapping(node.id());

            onDone(true);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridDhtLockResponse res) {
            if (res.error() != null)
                // Fail the whole compound future.
                onError(res.error());
            else {
                Collection<Integer> invalidParts = res.invalidPartitions();

                // Removing mappings for invalid partitions.
                if (!F.isEmpty(invalidParts)) {
                    for (Iterator<GridDhtCacheEntry> it = dhtMapping.iterator(); it.hasNext();) {
                        GridDhtCacheEntry entry = it.next();

                        if (invalidParts.contains(entry.partition())) {
                            it.remove();

                            if (log.isDebugEnabled())
                                log.debug("Removed mapping for entry [nodeId=" + node.id() + ", entry=" + entry +
                                    ", fut=" + GridDhtLockFuture.this + ']');

                            if (tx != null)
                                tx.removeDhtMapping(node.id(), entry);
                            else
                                entry.removeMapping(lockVer, node);
                        }
                    }

                    if (dhtMapping.isEmpty())
                        dhtMap.remove(node);
                }

                boolean replicate = cctx.isDrEnabled();

                boolean rec = cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED);

                for (GridCacheEntryInfo info : res.preloadEntries()) {
                    try {
                        GridCacheEntryEx entry = cctx.cache().entryEx(info.key(), topVer);

                        if (entry.initialValue(info.value(), info.version(), info.ttl(),
                            info.expireTime(), true, topVer, replicate ? DR_PRELOAD : DR_NONE)) {
                            if (rec && !entry.isInternal())
                                cctx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(),
                                    (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, info.value(), true, null,
                                    false, null, null, null);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        onDone(e);

                        return;
                    }
                    catch (GridCacheEntryRemovedException e) {
                        assert false : "Entry cannot become obsolete when DHT local candidate is added " +
                            "[e=" + e + ", ex=" + e + ']';
                    }
                }

                // Finish mini future.
                onDone(true);
            }
        }

        /**
         * @param cacheCtx Context.
         * @param keys Keys to evict readers for.
         * @param nodeId Node ID.
         * @param msgId Message ID.
         * @param entries Entries to check.
         */
        @SuppressWarnings({"ForLoopReplaceableByForEach"})
        private void evictReaders(GridCacheContext<?, ?> cacheCtx, Collection<IgniteTxKey> keys, UUID nodeId, long msgId,
            @Nullable List<GridDhtCacheEntry> entries) {
            if (entries == null || keys == null || entries.isEmpty() || keys.isEmpty())
                return;

            for (ListIterator<GridDhtCacheEntry> it = entries.listIterator(); it.hasNext(); ) {
                GridDhtCacheEntry cached = it.next();

                if (keys.contains(cached.txKey())) {
                    while (true) {
                        try {
                            cached.removeReader(nodeId, msgId);

                            if (tx != null)
                                tx.removeNearMapping(nodeId, cached);

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            GridDhtCacheEntry e = cacheCtx.dht().peekExx(cached.key());

                            if (e == null)
                                break;

                            it.set(e);
                        }
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "nodeId", node.id(), "super", super.toString());
        }
    }
}