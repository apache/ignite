/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;

/**
 * Entry wrapper that never obscures obsolete entries from user.
 */
public class GridCacheEvictionEntry<K, V> implements GridCacheEntry<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Static logger to avoid re-creation. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static volatile IgniteLogger log;

    /** Cached entry. */
    @GridToStringInclude
    protected GridCacheEntryEx<K, V> cached;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheEvictionEntry() {
        // No-op.
    }

    /**
     * @param cached Cached entry.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    protected GridCacheEvictionEntry(GridCacheEntryEx<K, V> cached) {
        this.cached = cached;

        log = U.logger(cached.context().kernalContext(), logRef, this);
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> projection() {
        return cached.context().cache();
    }

    /** {@inheritDoc} */
    @Override public K getKey() throws IllegalStateException {
        return cached.key();
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public V getValue() throws IllegalStateException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V setValue(V val) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public Object version() {
        try {
            return cached.version().drVersion();
        }
        catch (GridCacheEntryRemovedException ignore) {
            return cached.obsoleteVersion().drVersion();
        }
    }

    /** {@inheritDoc} */
    @Override public long expirationTime() {
        return cached.rawExpireTime();
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        GridCacheContext<K, V> ctx = cached.context();

        return ctx.config().getCacheMode() != PARTITIONED ||
            ctx.nodeId().equals(ctx.affinity().primary(cached.key(), ctx.affinity().affinityTopologyVersion()).id());
    }

    /** {@inheritDoc} */
    @Override public boolean backup() {
        GridCacheContext<K, V> ctx = cached.context();

        return ctx.config().getCacheMode() == PARTITIONED &&
            ctx.affinity().backups(cached.key(), ctx.affinity().affinityTopologyVersion()).contains(ctx.localNode());
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return cached.partition();
    }

    /** {@inheritDoc} */
    @Override public V peek() {
        try {
            return peek0(SMART, null, cached.context().tm().localTxx());
        }
        catch (GridException e) {
            // Should never happen.
            throw new GridRuntimeException("Unable to perform entry peek() operation.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) throws GridException {
        return peek0(modes, CU.<K, V>empty(), cached.context().tm().localTxx());
    }

    /**
     * @param mode Peek mode.
     * @param filter Optional entry filter.
     * @param tx Transaction to peek at (if mode is TX).
     * @return Peeked value.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private V peek0(@Nullable GridCachePeekMode mode,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter, @Nullable GridCacheTxEx<K, V> tx)
        throws GridException {
        assert tx == null || tx.local();

        if (mode == null)
            mode = SMART;

        try {
            GridTuple<V> peek = cached.peek0(false, mode, filter, tx);

            return peek != null ? peek.get() : null;
        }
        catch (GridCacheEntryRemovedException ignore) {
            return null;
        }
        catch (GridCacheFilterFailedException e) {
            e.printStackTrace();

            assert false;

            return null;
        }
    }

    /**
     * @param modes Peek modes.
     * @param filter Optional entry filter.
     * @param tx Transaction to peek at (if modes contains TX value).
     * @return Peeked value.
     * @throws GridException If failed.
     */
    @Nullable private V peek0(@Nullable Collection<GridCachePeekMode> modes,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter, GridCacheTxEx<K, V> tx) throws GridException {
        if (F.isEmpty(modes))
            return peek0(SMART, filter, tx);

        assert modes != null;

        for (GridCachePeekMode mode : modes) {
            V val = peek0(mode, filter, tx);

            if (val != null)
                return val;
        }

        return null;
    }

    /**
     * @return Unsupported exception.
     */
    private RuntimeException unsupported() {
        return new UnsupportedOperationException("Operation not supported during eviction.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public V reload() throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> reloadAsync() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean evict() {
        GridCacheContext<K, V> ctx = cached.context();

        try {
            assert ctx != null;
            assert ctx.evicts() != null;

            return ctx.evicts().evict(cached, null, false, null);
        }
        catch (GridException e) {
            U.error(log, "Failed to evict entry from cache: " + cached, e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean clear() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean compact() throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get() throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V set(V val, IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> setAsync(V val, IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean setx(V val, IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> setxAsync(V val, IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public void transform(IgniteClosure<V, V> transformer) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> transformAsync(IgniteClosure<V, V> transformer) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V replace(V val) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> replaceAsync(V val) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean replace(V oldVal, V newVal) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(V oldVal, V newVal) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public long timeToLive() {
        return cached.rawTtl();
    }

    /** {@inheritDoc} */
    @Override public void timeToLive(long ttl) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V setIfAbsent(V val) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> setIfAbsentAsync(V val) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean setxIfAbsent(V val) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> setxIfAbsentAsync(V val) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(V val) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replacexAsync(V val) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V remove(IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> removeAsync(IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean removex(IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removexAsync(IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean remove(V val) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(V val) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public <V> V addMeta(String name, V val) {
        return cached.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V> V meta(String name) {
        return cached.meta(name);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V> V removeMeta(String name) {
        return cached.removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> Map<String, V> allMeta() {
        return cached.allMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return cached.hasMeta(name);
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name, Object val) {
        return cached.hasMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V putMetaIfAbsent(String name, V val) {
        return cached.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V putMetaIfAbsent(String name, Callable<V> c) {
        return cached.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMetaIfAbsent(String name, V val) {
        return cached.addMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMetaIfAbsent(String name, Callable<V> c) {
        return cached.addMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean replaceMeta(String name, V curVal, V newVal) {
        return cached.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(GridMetadataAware from) {
        cached.copyMeta(from);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> data) {
        cached.copyMeta(data);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean removeMeta(String name, V val) {
        return cached.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean lock(long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> lockAsync(long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public void unlock(IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws GridException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean isCached() {
        return !cached.obsolete();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cached.context());
        out.writeObject(cached.key());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        GridCacheContext<K, V> ctx = (GridCacheContext<K, V>)in.readObject();
        K key = (K)in.readObject();

        cached = ctx.cache().entryEx(key);
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws GridException{
        return cached.memorySize();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return cached.key().hashCode();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj instanceof GridCacheEvictionEntry) {
            GridCacheEvictionEntry<K, V> other = (GridCacheEvictionEntry<K, V>)obj;

            V v1 = peek();
            V v2 = other.peek();

            return
                cached.key().equals(other.cached.key()) &&
                F.eq(cached.context().cache().name(), other.cached.context().cache().name()) &&
                F.eq(v1, v2);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionEntry.class, this);
    }
}
