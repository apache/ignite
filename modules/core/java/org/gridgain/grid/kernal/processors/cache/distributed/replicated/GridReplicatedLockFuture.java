// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Cache lock future.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridReplicatedLockFuture<K, V> extends GridFutureAdapter<Boolean>
    implements GridCacheMvccFuture<K, V, Boolean> {
    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<K, V> cctx;

    /** Underlying cache. */
    @GridToStringExclude
    private GridReplicatedCache<K, V> cache;

    /** Lock owner thread. */
    @GridToStringInclude
    private long threadId;

    /** Keys to lock. */
    @GridToStringInclude
    private Collection<? extends K> keys;

    /** Participating nodes. */
    private Collection<? extends GridNode> nodes;

    /** Keys locked so far. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    private List<GridDistributedCacheEntry<K, V>> entries;

    /** Future ID. */
    private GridUuid futId;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Map of results. */
    @GridToStringExclude
    private ConcurrentMap<UUID, GridDistributedLockResponse<K, V>> results;

    /** Latch to count replies. */
    private AtomicInteger leftRess;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private long timeout;

    /** Timed out flag. */
    private volatile boolean timedOut;

    /** Logger. */
    @GridToStringExclude
    private GridLogger log;

    /** Filter. */
    private GridPredicate<GridCacheEntry<K, V>>[] filter;

    /** Transaction. */
    private GridCacheTxLocalEx<K, V> tx;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridReplicatedLockFuture() {
        // No-op.
    }

    /**
     * @param cctx Registry.
     * @param keys Keys to lock.
     * @param threadId Locking thread ID.
     * @param tx Transaction, if any.
     * @param cache Underlying cache.
     * @param nodes Nodes to expect replies from.
     * @param timeout Lock acquisition timeout.
     * @param filter Filter.
     */
    public GridReplicatedLockFuture(
        GridCacheContext<K, V> cctx,
        Collection<? extends K> keys,
        long threadId,
        @Nullable GridCacheTxLocalEx<K, V> tx,
        GridReplicatedCache<K, V> cache,
        Collection<GridNode> nodes,
        long timeout,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        super(cctx.kernalContext());

        assert cctx != null;
        assert keys != null;
        assert cache != null;
        assert nodes != null;

        this.cctx = cctx;
        this.keys = keys;
        this.cache = cache;
        this.nodes = nodes;
        this.timeout = timeout;
        this.filter = filter;
        this.tx = tx;

        this.threadId = tx == null ? threadId : tx.threadId();

        lockVer = tx != null ? tx.xidVersion() : cctx.versions().next();

        futId = GridUuid.randomUuid();

        entries = new ArrayList<>(keys.size());

        results = new ConcurrentHashMap8<>(nodes.size(), 1.0f);

        leftRess = new AtomicInteger(nodes.size());

        log = U.logger(ctx, logRef, GridReplicatedLockFuture.class);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }
    }

    /**
     * @return Participating nodes.
     */
    @Override public Collection<? extends GridNode> nodes() {
        return nodes;
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
     * @return Remaining replies.
     */
    public long remainingReplies() {
        return leftRess.get();
    }

    /**
     * @return Entries.
     */
    public List<GridDistributedCacheEntry<K, V>> entries() {
        return entries;
    }

    /**
     * @return Entries.
     */
    public List<GridDistributedCacheEntry<K, V>> entriesCopy() {
        return new ArrayList<>(entries);
    }

    /**
     * @return Future ID.
     */
    @Override public GridUuid futureId() {
        return futId;
    }

    /**
     * @return {@code True} if transaction is not {@code null}.
     */
    private boolean inTx() {
        return tx != null;
    }

    /**
     * @return {@code True} if implicit transaction.
     */
    private boolean implicitSingle() {
        return tx != null && tx.implicitSingle();
    }

    /**
     * @param cached Entry.
     * @return {@code True} if locked.
     * @throws GridCacheEntryRemovedException If removed.
     */
    private boolean locked(GridCacheEntryEx<K, V> cached) throws GridCacheEntryRemovedException {
        // Reentry-aware check.
        return (cached.lockedLocally(lockVer) || cached.lockedByThread(threadId)) &&
            filter(cached); // If filter failed, lock is failed.
    }

    /**
     * @param cached Entry.
     * @param owner Lock owner.
     * @return {@code True} if locked.
     */
    private boolean locked(GridCacheEntryEx<K, V> cached, GridCacheMvccCandidate<K> owner) {
        // Reentry-aware check (if filter failed, lock is failed).
        return owner != null && owner.matches(lockVer, cctx.nodeId(), threadId) && filter(cached);
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable public GridCacheMvccCandidate<K> addEntry(GridDistributedCacheEntry<K, V> entry)
        throws GridCacheEntryRemovedException {
        // Check if timeout has elapsed.
        if (timedOut)
            return null;

        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate<K> c = entry.addLocal(
            threadId,
            lockVer,
            timeout,
            !inTx(),
            inTx(),
            implicitSingle()
        );

        synchronized (mux) {
            entries.add(entry);
        }

        if (c == null && timeout < 0) {
            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onFailed(false);

            return null;
        }

        // Double check.
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
        leftRess.set(0);

        // Transactions will undo during rollback.
        if (dist && tx == null) {
            cache.removeLocks(lockVer, keys);
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

            for (GridCacheEntryEx<K, V> e : entriesCopy()) {
                try {
                    e.removeLock(lockVer);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    while (true)
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
     *
     * @param dist {@code True} if need to distribute lock release.
     */
    private void onFailed(boolean dist) {
        undoLocks(dist);

        complete(false);
    }

    /**
     * @param success Success flag.
     */
    public void complete(boolean success) {
        leftRess.set(0);

        onComplete(success);
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public boolean onNodeLeft(final UUID nodeId) {
        if (F.exist(nodes, new P1<GridNode>() {
            @Override public boolean apply(GridNode node) {
                return nodeId.equals(node.id());
            }
        })) {
            onResult(nodeId, new GridDistributedLockResponse<K, V>(lockVer, futId, new GridTopologyException(
                "Valid exception to signal departure of node: " + nodeId)));

            return true;
        }

        return false;
    }

    /**
     * @param t Error.
     */
    public void onError(Throwable t) {
        if (err.compareAndSet(null, t)) {
            undoLocks(true);

            onComplete(false);
        }
    }

    /**
     * @param cached Entry to check.
     * @return {@code True} if filter passed.
     */
    private boolean filter(GridCacheEntryEx<K, V> cached) {
        try {
            if (!cctx.isAll(cached, filter)) {
                if (log.isDebugEnabled())
                    log.debug("Filter didn't pass for entry (will fail lock): " + cached);

                onFailed(true);

                return false;
            }

            return true;
        }
        catch (GridException e) {
            onError(e);

            return false;
        }
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        if (!isDone() && leftRess.get() == 0) {
            boolean locked = true;
            boolean hasKey = false;

            for (int i = 0; i < entries.size(); i++) {
                while (true) {
                    GridCacheEntryEx<K, V> cached = entries.get(i);

                    if (cached.key().equals(entry.key())) {
                        hasKey = true;

                        if (log.isDebugEnabled())
                            log.debug("Found future for owner change: " + this);

                        if (!locked)
                            return true;
                    }

                    try {
                        if (!locked(cached)) {
                            locked = false;

                            if (hasKey)
                                return true;
                        }

                        break;
                    }
                    // Possible in concurrent cases, when owner is changed after locks
                    // have been released or cancelled.
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in onOwnerChanged method (will retry): " + cached);

                        // Replace old entry with new one.
                        entries.set(i, (GridDistributedCacheEntry<K, V>)cctx.cache().entryEx(cached.key()));
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("Local lock acquired for entries: " + entries);

            if (locked)
                onComplete(true);

            return hasKey;
        }

        // Don't check if this entry cares about this key for performance reasons.
        // In the worst case other futures within the same transaction will be checked.
        return false;
    }

    /**
     * @param nodeId Sender node.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridDistributedLockResponse<K, V> res) {
        // Skip if canceled and ignore duplicate responses.
        if (!isCancelled() && results.putIfAbsent(nodeId, res) == null) {
            if (res.error() != null) {
                // Node departure is a valid result.
                if (res.error() instanceof GridTopologyException) {
                    /* No-op. */
                    if (log.isDebugEnabled())
                        log.debug("Ignoring departed node for future: " + this);

                    if (leftRess.decrementAndGet() == 0)
                        readyLocks();
                }
                // In case of error, unlock only once.
                else {
                    U.warn(log, "Received failed result response to lock request (will unlock): " + res);

                    onError(res.error());
                }

                return;
            }

            if (err.get() == null) {
                for (int i = 0; i < entries.size(); i++) {
                    while (true) {
                        GridDistributedCacheEntry<K, V> entry = entries.get(i);

                        try {
                            // Sync up remote candidates.
                            entry.addRemoteCandidates(
                                res.candidatesByIndex(i),
                                lockVer,
                                res.committedVersions(),
                                res.rolledbackVersions());

                            if (log.isDebugEnabled())
                                log.debug("Processed response for entry [res=" + res + ", entry=" + entry + ']');

                            break; // Inner while loop.
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to add candidates because entry was removed (will renew).");

                            // Replace old entry with new one.
                            entries.set(i, (GridDistributedCacheEntry<K, V>)cctx.cache().entryEx(entry.key()));
                        }
                    }
                }

                if (leftRess.decrementAndGet() == 0)
                    readyLocks();
            }
        }
    }

    /**
     * Readies all locks whenever all replies are received.
     */
    public void readyLocks() {
        assert leftRess.get() == 0;

        boolean success = true;

        for (int i = 0; i < entries.size(); i++) {
            while (true) {
                GridDistributedCacheEntry<K, V> entry = entries.get(i);

                try {
                    GridCacheMvccCandidate<K> owner = entry.readyLock(lockVer);

                    if (timeout < 0) {
                        if (owner == null || !owner.version().equals(lockVer)) {
                            onFailed(true);

                            return;
                        }
                    }

                    if (!locked(entry, owner)) {
                        if (log.isDebugEnabled())
                            log.debug("Entry is not locked (will keep waiting) [entry=" + entry + ", fut=" + this + ']');

                        success = false;
                    }

                    break; // Inner while loop.
                }
                // Possible in concurrent cases, when owner is changed after locks
                // have been released or cancelled.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to ready lock because entry was removed (will renew).");

                    entries.set(i, (GridDistributedCacheEntry<K, V>)cctx.cache().entryEx(entry.key()));
                }
            }
        }

        if (success) {
            if (log.isDebugEnabled())
                log.debug("Lock acquired for all entries: " + this);

            onComplete(true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            onComplete(false);

        return isCancelled();
    }

    /**
     * Completeness callback.
     *
     * @param success {@code True} if lock was acquired.
     */
    private void onComplete(boolean success) {
        if (tx != null)
            cctx.tm().txContext(tx);

        if (onDone(success, err.get())) {
            if (!success)
                undoLocks(true);

            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);
        }
    }

    /**
     * Checks for errors.
     *
     * @throws GridException If execution failed.
     */
    private void checkError() throws GridException {
        if (err.get() != null)
            throw U.cast(err.get());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedLockFuture.class, this);
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
}
