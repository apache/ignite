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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache lock future.
 */
public final class GridLocalLockFuture<K, V> extends GridFutureAdapter<Boolean>
    implements GridCacheMvccFuture<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

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
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private long timeout;

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

        this.cctx = cctx;
        this.cache = cache;
        this.timeout = timeout;
        this.filter = filter;
        this.tx = tx;

        ignoreInterrupts(true);

        threadId = tx == null ? Thread.currentThread().getId() : tx.threadId();

        lockVer = tx != null ? tx.xidVersion() : cctx.versions().next();

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>(keys.size());

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridLocalLockFuture.class);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }
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
    @Override public Collection<? extends ClusterNode> nodes() {
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
    List<GridLocalCacheEntry> entries() {
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
    @Nullable GridCacheMvccCandidate addEntry(GridLocalCacheEntry entry)
        throws GridCacheEntryRemovedException {
        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate c = entry.addLocal(
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
        if (err.compareAndSet(null, t))
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
     * @throws IgniteCheckedException If execution failed.
     */
    private void checkError() throws IgniteCheckedException {
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