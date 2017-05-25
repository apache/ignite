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

package org.apache.ignite.internal.processors.cache.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlock;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache lock future.
 */
public final class GridLocalLockFuture<K, V> extends GridCacheFutureAdapter<Boolean>
    implements GridCacheMvccFuture<Boolean> {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Error updater. */
    private static final AtomicReferenceFieldUpdater<GridLocalLockFuture, Throwable> ERR_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridLocalLockFuture.class, Throwable.class, "err");

    /** Logger. */
    private static IgniteLogger log;

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
    private List<GridLocalCacheEntry> entries;

    /** Future ID. */
    private IgniteUuid futId;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Error. */
    @SuppressWarnings("UnusedDeclaration")
    private volatile Throwable err;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private final long timeout;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    /** Transaction. */
    private IgniteTxLocalEx tx;

    /** Trackable flag. */
    private boolean trackable = true;

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
        Collection<KeyCacheObject> keys,
        IgniteTxLocalEx tx,
        GridLocalCache<K, V> cache,
        long timeout,
        CacheEntryPredicate[] filter) {
        assert keys != null;
        assert cache != null;
        assert (tx != null && timeout >= 0) || tx == null;

        this.cctx = cctx;
        this.cache = cache;
        this.timeout = timeout;
        this.filter = filter;
        this.tx = tx;

        ignoreInterrupts();

        threadId = tx == null ? Thread.currentThread().getId() : tx.threadId();

        lockVer = tx != null ? tx.xidVersion() : cctx.versions().next();

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>(keys.size());

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridLocalLockFuture.class);
    }

    /**
     * @param keys Keys.
     * @return {@code False} in case of error.
     * @throws IgniteCheckedException If failed.
     */
    public boolean addEntries(Collection<KeyCacheObject> keys) throws IgniteCheckedException {
        for (KeyCacheObject key : keys) {
            while (true) {
                GridLocalCacheEntry entry = null;

                try {
                    entry = cache.entryExx(key);

                    entry.unswap(false);

                    if (!cctx.isAll(entry, filter)) {
                        onFailed();

                        return false;
                    }

                    // Removed exception may be thrown here.
                    GridCacheMvccCandidate cand = addEntry(entry);

                    if (cand == null && isDone())
                        return false;

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in lockAsync(..) method (will retry): " + entry);
                }
            }
        }

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
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
     * @return Entries.
     */
    private List<GridLocalCacheEntry> entries() {
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
    private boolean locked(GridCacheEntryEx cached) throws GridCacheEntryRemovedException {
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
    private @Nullable GridCacheMvccCandidate addEntry(GridLocalCacheEntry entry)
        throws GridCacheEntryRemovedException {
        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate c = entry.addLocal(
            threadId,
            lockVer,
            null,
            null,
            timeout,
            !inTx(),
            inTx(),
            implicitSingle(),
            false
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
        for (GridLocalCacheEntry e : entries) {
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
        if (ERR_UPD.compareAndSet(this, null, t))
            onFailed();
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

                onFailed();

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
     * Explicitly check if lock was acquired.
     */
    void checkLocks() {
        if (!isDone()) {
            for (int i = 0; i < entries.size(); i++) {
                while (true) {
                    GridCacheEntryEx cached = entries.get(i);

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
                        entries.add(i, (GridLocalCacheEntry)cache.entryEx(cached.key()));
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("Local lock acquired for entries: " + entries);

            onComplete(true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (!isDone()) {
            for (int i = 0; i < entries.size(); i++) {
                while (true) {
                    GridCacheEntryEx cached = entries.get(i);

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
                        entries.add(i, (GridLocalCacheEntry)cache.entryEx(cached.key()));
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

        if (onDone(success, err)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            cache.onFutureDone(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);
        }
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
        @SuppressWarnings({"ThrowableInstanceNeverThrown", "ForLoopReplaceableByForEach"})
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            if (inTx() && cctx.tm().deadlockDetectionEnabled()) {
                Set<IgniteTxKey> keys = new HashSet<>();

                List<GridLocalCacheEntry> entries = entries();

                for (int i = 0; i < entries.size(); i++) {
                    GridLocalCacheEntry e = entries.get(i);

                    List<GridCacheMvccCandidate> mvcc = e.mvccAllLocal();

                    if (mvcc == null)
                        continue;

                    GridCacheMvccCandidate cand = mvcc.get(0);

                    if (cand.owner() && cand.tx() && !cand.version().equals(tx.xidVersion()))
                        keys.add(e.txKey());
                }

                IgniteInternalFuture<TxDeadlock> fut = cctx.tm().detectDeadlock(tx, keys);

                fut.listen(new IgniteInClosure<IgniteInternalFuture<TxDeadlock>>() {
                    @Override public void apply(IgniteInternalFuture<TxDeadlock> fut) {
                        try {
                            TxDeadlock deadlock = fut.get();

                            if (deadlock != null)
                                ERR_UPD.compareAndSet(GridLocalLockFuture.this, null,
                                    new TransactionDeadlockException(deadlock.toString(cctx.shared())));
                        }
                        catch (IgniteCheckedException e) {
                            U.warn(log, "Failed to detect deadlock.", e);

                            ERR_UPD.compareAndSet(GridLocalLockFuture.this, null, e);
                        }

                        onComplete(false);
                    }
                });
            }
            else
                onComplete(false);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}