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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

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
    @Override public IgniteInternalFuture<Boolean> txLockAsync(Collection<? extends K> keys,
        long timeout,
        IgniteTxLocalEx<K, V> tx,
        boolean isRead,
        boolean retval,
        TransactionIsolation isolation,
        boolean invalidate,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        return lockAllAsync(keys, timeout, tx, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        IgniteTxLocalEx<K, V> tx = ctx.tm().localTx();

        return lockAllAsync(keys, timeout, tx, filter);
    }

    /**
     * @param keys Keys.
     * @param timeout Timeout.
     * @param tx Transaction.
     * @param filter Filter.
     * @return Future.
     */
    public IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        @Nullable IgniteTxLocalEx<K, V> tx, IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext(), true);

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
    @Override public void unlockAll(Collection<? extends K> keys,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) throws IgniteCheckedException {
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
    @SuppressWarnings("unchecked")
    @Override public void removeAll() throws IgniteCheckedException {
        removeAll(keySet());
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
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }

    /**
     * @param fut Clears future from cache.
     */
    void onFutureDone(GridCacheFuture<?> fut) {
        if (ctx.mvcc().removeFuture(fut)) {
            if (log().isDebugEnabled())
                log().debug("Explicitly removed future from map of futures: " + fut);
        }
    }
}
