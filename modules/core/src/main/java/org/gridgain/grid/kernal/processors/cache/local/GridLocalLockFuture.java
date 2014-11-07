/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Cache lock future.
 */
public final class GridLocalLockFuture<K, V> extends GridFutureAdapter<Boolean>
    implements GridCacheMvccFuture<K, V, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<K, V> cctx;

    /** Underlying cache. */
    @GridToStringExclude
    private GridLocalCache<K, V> cache;

    /** Lock owner thread. */
    @GridToStringInclude
    private long threadId;

    /** Keys locked so far. */
    @GridToStringExclude
    private List<GridLocalCacheEntry<K, V>> entries;

    /** Future ID. */
    private GridUuid futId;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private long timeout;

    /** Logger. */
    @GridToStringExclude
    private GridLogger log;

    /** Filter. */
    private GridPredicate<GridCacheEntry<K, V>>[] filter;

    /** Transaction. */
    private GridCacheTxLocalEx<K, V> tx;

    /** Trackable flag. */
    private boolean trackable = true;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridLocalLockFuture() {
        // No-op.
    }

    /**
     * @param cctx Registry.
     * @param keys Keys to lock.
     * @param tx Transaction.
     * @param cache Underlying cache.
     * @param timeout Lock acquisition timeout.
     * @param filter Filter.
     */
    GridLocalLockFuture(
        GridCacheContext<K, V> cctx,
        Collection<? extends K> keys,
        GridCacheTxLocalEx<K, V> tx,
        GridLocalCache<K, V> cache,
        long timeout,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        super(cctx.kernalContext());

        assert keys != null;
        assert cache != null;

        this.cctx = cctx;
        this.cache = cache;
        this.timeout = timeout;
        this.filter = filter;
        this.tx = tx;

        threadId = tx == null ? Thread.currentThread().getId() : tx.threadId();

        lockVer = tx != null ? tx.xidVersion() : cctx.versions().next();

        futId = GridUuid.randomUuid();

        entries = new ArrayList<>(keys.size());

        log = U.logger(ctx, logRef, GridLocalLockFuture.class);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }
    }

    /** {@inheritDoc} */
    @Override public GridUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends GridNode> nodes() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        return false;
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
     * @return Lock version.
     */
    GridCacheVersion lockVersion() {
        return lockVer;
    }

    /**
     * @return Entries.
     */
    List<GridLocalCacheEntry<K, V>> entries() {
        return entries;
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
        return (cached.lockedLocally(lockVer) || (cached.lockedByThread(threadId))) &&
            filter(cached); // If filter failed, lock is failed.
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable GridCacheMvccCandidate<K> addEntry(GridLocalCacheEntry<K, V> entry)
        throws GridCacheEntryRemovedException {
        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate<K> c = entry.addLocal(
            threadId,
            lockVer,
            timeout,
            !inTx(),
            inTx(),
            implicitSingle()
        );

        entries.add(entry);

        if (c == null && timeout < 0) {
            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onFailed();

            return null;
        }

        if (c != null) {
            // Immediately set lock to ready.
            entry.readyLocal(c);
        }

        return c;
    }

    /**
     * Undoes all locks.
     */
    private void undoLocks() {
        for (GridLocalCacheEntry<K, V> e : entries) {
            try {
                e.removeLock(lockVer);
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Got removed entry while undoing locks: " + e);
            }
        }
    }

    /**
     *
     */
    void onFailed() {
        undoLocks();

        onComplete(false);
    }

    /**
     * @param t Error.
     */
    void onError(Throwable t) {
        if (err.compareAndSet(null, t))
            onFailed();
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

                onFailed();

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
     * Explicitly check if lock was acquired.
     */
    void checkLocks() {
        if (!isDone()) {
            for (int i = 0; i < entries.size(); i++) {
                while (true) {
                    GridCacheEntryEx<K, V> cached = entries.get(i);

                    try {
                        if (!locked(cached))
                            return;

                        break;
                    }
                    // Possible in concurrent cases, when owner is changed after locks
                    // have been released or cancelled.
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in onOwnerChanged method (will retry): " + cached);

                        // Replace old entry with new one.
                        entries.add(i, (GridLocalCacheEntry<K,V>)cache.entryEx(cached.key()));
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("Local lock acquired for entries: " + entries);

            onComplete(true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        if (!isDone()) {
            for (int i = 0; i < entries.size(); i++) {
                while (true) {
                    GridCacheEntryEx<K, V> cached = entries.get(i);

                    try {
                        if (!locked(cached))
                            return true;

                        break;
                    }
                    // Possible in concurrent cases, when owner is changed after locks
                    // have been released or cancelled.
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry in onOwnerChanged method (will retry): " + cached);

                        // Replace old entry with new one.
                        entries.add(i, (GridLocalCacheEntry<K,V>)cache.entryEx(cached.key()));
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("Local lock acquired for entries: " + entries);

            onComplete(true);
        }

        return false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public boolean cancel() {
        if (onCancelled()) {
            // Remove all locks.
            undoLocks();

            onComplete(false);
        }

        return isCancelled();
    }

    /**
     * Completeness callback.
     *
     * @param success If {@code true}, then lock has been acquired.
     */
    private void onComplete(boolean success) {
        if (!success)
            undoLocks();

        if (onDone(success, err.get())) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            cache.onFutureDone(this);

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
        return S.toString(GridLocalLockFuture.class, this);
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

            onComplete(false);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
