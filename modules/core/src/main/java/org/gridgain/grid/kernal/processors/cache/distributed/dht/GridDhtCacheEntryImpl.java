/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;

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
    @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) throws GridException {
        if (!ctx.isNear() && !ctx.isReplicated() && modes.contains(NEAR_ONLY))
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
    @Nullable private V peekDht(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        try {
            return peekDht0(SMART, filter);
        }
        catch (GridException e) {
            // Should never happen.
            throw new GridRuntimeException("Unable to perform entry peek() operation.", e);
        }
    }

    /**
     * @param modes Peek modes.
     * @param filter Optional entry filter.
     * @return Peeked value.
     * @throws GridException If failed.
     */
    @Nullable private V peekDht0(@Nullable Collection<GridCachePeekMode> modes,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
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
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private V peekDht0(@Nullable GridCachePeekMode mode,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
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
