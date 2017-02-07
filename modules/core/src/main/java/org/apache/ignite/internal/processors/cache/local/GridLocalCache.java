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

import java.io.Externalizable;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.GridCachePreloaderAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Local cache implementation.
 */
public class GridLocalCache<K, V> extends GridCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCachePreloader preldr;

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

        preldr = new GridCachePreloaderAdapter(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader preloader() {
        return preldr;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMapEntryFactory entryFactory() {
        return new GridCacheMapEntryFactory() {
            @Override public GridCacheMapEntry create(
                GridCacheContext ctx,
                AffinityTopologyVersion topVer,
                KeyCacheObject key,
                int hash,
                CacheObject val
            ) {
                return new GridLocalCacheEntry(ctx, key, hash, val);
            }
        };
    }

    /**
     * @param key Key of entry.
     * @return Cache entry.
     */
    @Nullable GridLocalCacheEntry peekExx(KeyCacheObject key) {
        return (GridLocalCacheEntry)peekEx(key);
    }

    /**
     * @param key Key of entry.
     * @return Cache entry.
     */
    GridLocalCacheEntry entryExx(KeyCacheObject key) {
        return (GridLocalCacheEntry)entryEx(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> txLockAsync(Collection<KeyCacheObject> keys,
        long timeout,
        IgniteTxLocalEx tx,
        boolean isRead,
        boolean retval,
        TransactionIsolation isolation,
        boolean invalidate,
        long createTtl,
        long accessTtl) {
        return lockAllAsync(keys, timeout, tx, CU.empty0());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout) {
        IgniteTxLocalEx tx = ctx.tm().localTx();

        return lockAllAsync(ctx.cacheKeysView(keys), timeout, tx, CU.empty0());
    }

    /**
     * @param keys Keys.
     * @param timeout Timeout.
     * @param tx Transaction.
     * @param filter Filter.
     * @return Future.
     */
    public IgniteInternalFuture<Boolean> lockAllAsync(Collection<KeyCacheObject> keys,
        long timeout,
        @Nullable IgniteTxLocalEx tx,
        CacheEntryPredicate[] filter) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(true);

        GridLocalLockFuture<K, V> fut = new GridLocalLockFuture<>(ctx, keys, tx, this, timeout, filter);

        try {
            for (KeyCacheObject key : keys) {
                while (true) {
                    GridLocalCacheEntry entry = null;

                    try {
                        entry = entryExx(key);

                        entry.unswap(false);

                        if (!ctx.isAll(entry, filter)) {
                            fut.onFailed();

                            return fut;
                        }

                        // Removed exception may be thrown here.
                        GridCacheMvccCandidate cand = fut.addEntry(entry);

                        if (cand == null && fut.isDone())
                            return fut;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log().isDebugEnabled())
                            log().debug("Got removed entry in lockAsync(..) method (will retry): " + entry);
                    }
                }
            }

            if (!ctx.mvcc().addFuture(fut))
                fut.onError(new IgniteCheckedException("Duplicate future ID (internal error): " + fut));

            // Must have future added prior to checking locks.
            fut.checkLocks();

            return fut;
        }
        catch (IgniteCheckedException e) {
            fut.onError(e);

            return fut;
        }
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(
        Collection<? extends K> keys
    ) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        for (K key : keys) {
            GridLocalCacheEntry entry = peekExx(ctx.toCacheKeyObject(key));

            if (entry != null && ctx.isAll(entry, CU.empty0())) {
                entry.releaseLocal();

                ctx.evicts().touch(entry, topVer);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        return ctx.closures().callLocalSafe(new Callable<Void>() {
            @Override public Void call() throws Exception {
                removeAll();

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }

    /**
     * @param fut Clears future from cache.
     */
    void onFutureDone(GridLocalLockFuture fut) {
        if (ctx.mvcc().removeMvccFuture(fut)) {
            if (log().isDebugEnabled())
                log().debug("Explicitly removed future from map of futures: " + fut);
        }
    }
}
