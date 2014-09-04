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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Local cache implementation.
 */
public class GridLocalCache<K, V> extends GridCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCachePreloader<K,V> preldr;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridLocalCache() {
        // No-op.
    }

    /**
     * @param ctx Cache registry.
     */
    public GridLocalCache(GridCacheContext<K, V> ctx) {
        super(ctx, ctx.config().getStartSize());

        preldr = new GridCachePreloaderAdapter<>(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader<K, V> preloader() {
        return preldr;
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory<K, V>() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash,
                V val, GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
                return new GridLocalCacheEntry<>(ctx, key, hash, val, next, ttl, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxLocalAdapter<K, V> newTx(
        boolean implicit,
        boolean implicitSingle,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean syncCommit,
        boolean syncRollback,
        boolean swapOrOffheapEnabled,
        boolean storeEnabled,
        int txSize,
        @Nullable Object grpLockKey,
        boolean partLock
    ) {
        if (grpLockKey != null)
            throw new IllegalStateException("Group locking is not supported for LOCAL cache.");

        // Use null as subject ID for transactions if subject per call is not set.
        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        UUID subjId = prj == null ? null : prj.subjectId();

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        return new GridLocalTx<>(
            ctx,
            implicit,
            implicitSingle,
            concurrency,
            isolation,
            timeout,
            invalidate,
            swapOrOffheapEnabled,
            storeEnabled,
            txSize,
            subjId,
            taskNameHash);
    }

    /**
     * @param key Key of entry.
     * @return Cache entry.
     */
    @Nullable GridLocalCacheEntry<K, V> peekExx(K key) {
        return (GridLocalCacheEntry<K,V>)peekEx(key);
    }

    /**
     * @param key Key of entry.
     * @return Cache entry.
     */
    GridLocalCacheEntry<K, V> entryExx(K key) {
        return (GridLocalCacheEntry<K, V>)entryEx(key);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> txLockAsync(Collection<? extends K> keys, long timeout,
        GridCacheTxLocalEx<K, V> tx, boolean isRead,
        boolean retval, GridCacheTxIsolation isolation, boolean invalidate,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return lockAllAsync(keys, timeout, tx, filter);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        GridCacheTxLocalEx<K, V> tx = ctx.tm().localTx();

        return lockAllAsync(keys, timeout, tx, filter);
    }

    /**
     * @param keys Keys.
     * @param timeout Timeout.
     * @param tx Transaction.
     * @param filter Filter.
     * @return Future.
     */
    public GridFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        @Nullable GridCacheTxLocalEx<K, V> tx, GridPredicate<GridCacheEntry<K, V>>[] filter) {
        if (F.isEmpty(keys)) {
            return new GridFinishedFuture<>(ctx.kernalContext(), true);
        }

        GridLocalLockFuture<K, V> fut = new GridLocalLockFuture<>(ctx, keys, tx, this, timeout, filter);

        try {
            for (K key : keys) {
                while (true) {
                    GridLocalCacheEntry<K, V> entry = null;

                    try {
                        entry = entryExx(key);

                        if (!ctx.isAll(entry, filter)) {
                            fut.onFailed();

                            return fut;
                        }

                        // Removed exception may be thrown here.
                        GridCacheMvccCandidate<K> cand = fut.addEntry(entry);

                        if (cand == null && fut.isDone())
                            return fut;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log().isDebugEnabled()) {
                            log().debug("Got removed entry in lockAsync(..) method (will retry): " + entry);
                        }
                    }
                }
            }

            if (!ctx.mvcc().addFuture(fut))
                fut.onError(new GridException("Duplicate future ID (internal error): " + fut));

            // Must have future added prior to checking locks.
            fut.checkLocks();

            return fut;
        }
        catch (GridException e) {
            fut.onError(e);

            return fut;
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        long topVer = ctx.affinity().affinityTopologyVersion();

        for (K key : keys) {
            GridLocalCacheEntry<K, V> entry = peekExx(key);

            if (entry != null && ctx.isAll(entry, filter)) {
                entry.releaseLocal();

                ctx.evicts().touch(entry, topVer);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }

    /**
     * @param fut Clears future from cache.
     */
    void onFutureDone(GridCacheFuture<?> fut) {
        if (ctx.mvcc().removeFuture(fut)) {
            if (log().isDebugEnabled()) {
                log().debug("Explicitly removed future from map of futures: " + fut);
            }
        }
    }
}
