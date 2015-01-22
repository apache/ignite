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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.GridCachePeekMode.*;

/**
 * Partitioned cache entry public API.
 */
public class GridPartitionedCacheEntryImpl<K, V> extends GridCacheEntryImpl<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridPartitionedCacheEntryImpl() {
        // No-op.
    }

    /**
     * @param nearPrj Parent projection or {@code null} if entry belongs to default cache.
     * @param ctx Near cache context.
     * @param key key.
     * @param cached Cached entry (either from near or dht cache map).
     */
    public GridPartitionedCacheEntryImpl(GridCacheProjectionImpl<K, V> nearPrj, GridCacheContext<K, V> ctx, K key,
        @Nullable GridCacheEntryEx<K, V> cached) {
        super(nearPrj, ctx, key, cached);

        assert !this.ctx.isDht() || ctx.isColocated();
    }

    /**
     * @return Dht cache.
     */
    public GridDhtCacheAdapter<K, V> dht() {
        return ctx.isColocated() ? ctx.colocated() : ctx.isDhtAtomic() ? ctx.dht() : ctx.near().dht();
    }

    /**
     * @return Near cache.
     */
    public GridNearCacheAdapter<K, V> near() {
        return ctx.near();
    }

    /** {@inheritDoc} */
    @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) throws IgniteCheckedException {
        if (modes.contains(NEAR_ONLY) && ctx.isNear())
            return peekNear0(modes, CU.<K, V>empty());

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
    @Nullable public V peekDht(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
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
    @Nullable private V peekNear0(@Nullable Collection<GridCachePeekMode> modes,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        if (F.isEmpty(modes))
            return peekNear0(SMART, filter);

        assert modes != null;

        for (GridCachePeekMode mode : modes) {
            V val = peekNear0(mode, filter);

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
    @Nullable private V peekNear0(@Nullable GridCachePeekMode mode,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        if (mode == null)
            mode = SMART;

        while (true) {
            GridCacheProjectionImpl<K, V> prjPerCall = proxy.gateProjection();

            if (prjPerCall != null)
                filter = ctx.vararg(F0.and(ctx.vararg(proxy.predicate()), filter));

            GridCacheProjectionImpl<K, V> prev = ctx.gate().enter(prjPerCall);

            try {
                GridCacheEntryEx<K, V> entry = near().peekEx(key);

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

    /**
     * @param modes Peek modes.
     * @param filter Optional entry filter.
     * @return Peeked value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private V peekDht0(@Nullable Collection<GridCachePeekMode> modes,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
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
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        if (mode == null)
            mode = SMART;

        while (true) {
            GridCacheProjectionImpl<K, V> prjPerCall = proxy.gateProjection();

            if (prjPerCall != null)
                filter = ctx.vararg(F0.and(ctx.vararg(proxy.predicate()), filter));

            GridCacheProjectionImpl<K, V> prev = ctx.gate().enter(prjPerCall);

            try {
                GridCacheEntryEx<K, V> entry = dht().peekEx(key);

                if (entry == null)
                    return null;
                else {
                    GridTuple<V> peek = entry.peek0(false, mode, filter, ctx.tm().localTxx());

                    return peek != null ? ctx.cloneOnFlag(peek.get()) : null;
                }
            }
            catch (GridCacheEntryRemovedException ignore) {
                // No-op.
            }
            catch (GridCacheFilterFailedException e) {
                e.printStackTrace();

                assert false;

                return null;
            }
            finally {
                ctx.gate().leave(prev);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx<K, V> entryEx(boolean touch, long topVer) {
        try {
            return ctx.affinity().localNode(key, topVer) ? dht().entryEx(key, touch) :
                ctx.isNear() ? near().entryEx(key, touch) :
                    new GridDhtDetachedCacheEntry<>(ctx, key, 0, null, null, 0, 0);
        }
        catch (GridDhtInvalidPartitionException ignore) {
            return ctx.isNear() ? near().entryEx(key) :
                new GridDhtDetachedCacheEntry<>(ctx, key, 0, null, null, 0, 0);
        }
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx<K, V> peekEx(long topVer) {
        try {
            return ctx.affinity().localNode(key, topVer) ? dht().peekEx(key) :
                ctx.isNear() ? near().peekEx(key) : null;
        }
        catch (GridDhtInvalidPartitionException ignore) {
            return ctx.isNear() ? near().peekEx(key) : null;
        }
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 addMeta(String name, V1 val) {
        V1 v = null;

        GridDhtCacheEntry<K, V> de = dht().peekExx(key);

        if (de != null)
            v = de.addMeta(name, val);

        if (ctx.isNear()) {
            GridNearCacheEntry<K, V> ne = de != null ? near().peekExx(key) :
                near().entryExx(key, ctx.affinity().affinityTopologyVersion());

            if (ne != null) {
                V1 v1 = ne.addMeta(name, val);

                if (v == null)
                    v = v1;
            }
        }

        return v;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"RedundantCast"})
    @Override public <V1> V1 meta(String name) {
        V1 v = null;

        GridDhtCacheEntry<K, V> de = dht().peekExx(key);

        if (de != null)
            v = (V1)de.meta(name);

        if (ctx.isNear()) {
            GridNearCacheEntry<K, V> ne = near().peekExx(key);

            if (ne != null) {
                V1 v1 = (V1)ne.meta(name);

                if (v == null)
                    v = v1;
            }
        }

        return v;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"RedundantCast"})
    @Override public <V1> V1 putMetaIfAbsent(String name, Callable<V1> c) {
        V1 v = null;

        GridDhtCacheEntry<K, V> de = dht().peekExx(key);

        if (de != null)
            v = (V1)de.putMetaIfAbsent(name, c);

        if (ctx.isNear()) {
            GridNearCacheEntry<K, V> ne = de != null ? near().peekExx(key) :
                near().entryExx(key, ctx.affinity().affinityTopologyVersion());

            if (ne != null) {
                V1 v1 = (V1)ne.putMetaIfAbsent(name, c);

                if (v == null)
                    v = v1;
            }
        }

        return v;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"RedundantCast"})
    @Override public <V1> V1 putMetaIfAbsent(String name, V1 val) {
        V1 v = null;

        GridDhtCacheEntry<K, V> de = dht().peekExx(key);

        if (de != null)
            v = (V1)de.putMetaIfAbsent(name, val);

        GridNearCacheEntry<K, V> ne = de != null ? near().peekExx(key) :
            near().entryExx(key, ctx.affinity().affinityTopologyVersion());

        if (ne != null) {
            V1 v1 = (V1)ne.putMetaIfAbsent(name, val);

            if (v == null)
                v = v1;
        }

        return v;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"RedundantCast"})
    @Override public <V1> V1 removeMeta(String name) {
        V1 v = null;

        GridDhtCacheEntry<K, V> de = dht().peekExx(key);

        if (de != null)
            v = (V1)de.removeMeta(name);

        if (ctx.isNear()) {
            GridNearCacheEntry<K, V> ne = near().peekExx(key);

            if (ne != null) {
                V1 v1 = (V1)ne.removeMeta(name);

                if (v == null)
                    v = v1;
            }
        }

        return v;
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean removeMeta(String name, V1 val) {
        boolean b = false;

        GridDhtCacheEntry<K, V> de = dht().peekExx(key);

        if (de != null)
            b = de.removeMeta(name, val);

        if (ctx.isNear()) {
            GridNearCacheEntry<K, V> ne = near().peekExx(key);

            if (ne != null)
                b |= ne.removeMeta(name, val);
        }

        return b;
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean replaceMeta(String name, V1 curVal, V1 newVal) {
        boolean b = false;

        GridDhtCacheEntry<K, V> de = dht().peekExx(key);

        if (de != null)
            b = de.replaceMeta(name, curVal, newVal);

        if (ctx.isNear()) {
            GridNearCacheEntry<K, V> ne = near().peekExx(key);

            if (ne != null)
                b |= ne.replaceMeta(name, curVal, newVal);
        }

        return b;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionedCacheEntryImpl.class, this, super.toString());
    }
}
