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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.GridCachePeekMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;

/**
 * Colocated cache entry public API.
 */
public class GridDhtCacheEntryImpl<K, V> extends GridCacheEntryImpl<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtCacheEntryImpl() {
        // No-op.
    }

    /**
     * @param nearPrj Parent projection or {@code null} if entry belongs to default cache.
     * @param ctx Near cache context.
     * @param key key.
     * @param cached Cached entry (either from near or dht cache map).
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    public GridDhtCacheEntryImpl(GridCacheProjectionImpl<K, V> nearPrj, GridCacheContext<K, V> ctx, K key,
        @Nullable GridCacheEntryEx<K, V> cached) {
        super(nearPrj, ctx, key, cached);

        assert !this.ctx.isDht() || !isNearEnabled(ctx);
    }

    /**
     * @return Dht cache.
     */
    private GridDhtCacheAdapter<K, V> dht() {
        return ctx.dht();
    }

    /** {@inheritDoc} */
    @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) throws IgniteCheckedException {
        if (!ctx.isNear() && modes.contains(NEAR_ONLY))
            return null;

        V val = null;

        if (!modes.contains(PARTITIONED_ONLY))
            val = super.peek(modes);

        if (val == null)
            val = peekDht0(modes, CU.<K, V>empty());

        return val;
    }

    /**
     * @param filter Filter.
     * @return Peeked value.
     */
    @Nullable private V peekDht(@Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        try {
            return peekDht0(SMART, filter);
        }
        catch (IgniteCheckedException e) {
            // Should never happen.
            throw new IgniteException("Unable to perform entry peek() operation.", e);
        }
    }

    /**
     * @param modes Peek modes.
     * @param filter Optional entry filter.
     * @return Peeked value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private V peekDht0(@Nullable Collection<GridCachePeekMode> modes,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        if (F.isEmpty(modes))
            return peekDht0(SMART, filter);

        assert modes != null;

        for (GridCachePeekMode mode : modes) {
            V val = peekDht0(mode, filter);

            if (val != null)
                return val;
        }

        return null;
    }

    /**
     * @param mode Peek mode.
     * @param filter Optional entry filter.
     * @return Peeked value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private V peekDht0(@Nullable GridCachePeekMode mode,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        if (mode == null)
            mode = SMART;

        while (true) {
            GridCacheProjectionImpl<K, V> prjPerCall = proxy.gateProjection();

            if (prjPerCall != null)
                filter = ctx.vararg(F.and(ctx.vararg(proxy.predicate()), filter));

            GridCacheProjectionImpl<K, V> prev = ctx.gate().enter(prjPerCall);

            try {
                GridCacheEntryEx<K, V> entry = dht().peekEx(key);

                return entry == null ? null : ctx.cloneOnFlag(entry.peek(mode, filter));
            }
            catch (GridCacheEntryRemovedException ignore) {
                // No-op.
            }
            finally {
                ctx.gate().leave(prev);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked() {
        // Check colocated explicit locks.
        return ctx.mvcc().isLockedByThread(key, -1);
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread() {
        // Check colocated explicit locks.
        return ctx.mvcc().isLockedByThread(key, Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtCacheEntryImpl.class, this, super.toString());
    }
}
