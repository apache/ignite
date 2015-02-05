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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.GridCachePeekMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;

/**
 * Entry wrapper that never obscures obsolete entries from user.
 */
public class GridCacheEntryImpl<K, V> implements CacheEntry<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of one peek mode to avoid collection creation. */
    public static final List<GridCachePeekMode> MODES_SMART = F.asList(SMART);

    /** Cache context. */
    protected GridCacheContext<K, V> ctx;

    /** Parent projection. */
    protected GridCacheProxyImpl<K, V> proxy;

    /** Key. */
    @GridToStringInclude
    protected K key;

    /** Cached entry. */
    @GridToStringInclude
    protected volatile GridCacheEntryEx<K, V> cached;

    /** Time to live. If not set, leaves cache entry ttl unchanged. */
    private long ttl = -1;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheEntryImpl() {
        // No-op.
    }

    /**
     * @param prj Parent projection or {@code null} if entry belongs to default cache.
     * @param ctx Context.
     * @param key key.
     * @param cached Cached entry.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    protected GridCacheEntryImpl(GridCacheProjectionImpl<K, V> prj,
        GridCacheContext<K, V> ctx, K key,
        GridCacheEntryEx<K, V> cached) {
        assert ctx != null;
        assert key != null;

        this.ctx = ctx;
        this.key = key;
        this.cached = cached;

        proxy = new GridCacheProxyImpl<>(ctx, prj != null ? prj : ctx.cache(), prj);
    }

    /** {@inheritDoc} */
    @Override public CacheProjection<K, V> projection() {
        return proxy;
    }

    /**
     * @return Cache entry.
     */
    @Nullable public GridCacheEntryEx<K, V> unwrapNoCreate() {
        GridCacheEntryEx<K, V> cached = this.cached;

        if (cached == null || cached.obsolete())
            this.cached = cached = peekEx(ctx.affinity().affinityTopologyVersion());

        return cached;
    }

    /**
     * Unwraps cache entry and returns tuple containing unwrapped entry and boolean flag
     * indicating whether entry was actually created.
     *
     * @param create Flag to create entry if it does not exists.
     * @return Tuple.
     */
    private IgniteBiTuple<GridCacheEntryEx<K, V>, Boolean> unwrapChecked(boolean create) {
        GridCacheEntryEx<K, V> cached = this.cached;

        try {
            if (cached == null) {
                long topVer = ctx.affinity().affinityTopologyVersion();

                this.cached = cached = create ? entryEx(false, topVer) : peekEx(topVer);

                return F.t(cached, create);
            }
            else
                return F.t(cached, false);
        }
        catch (GridDhtInvalidPartitionException ignore) {
            return F.t(null, false);
        }
    }

    /**
     * Gets cache entry for adding metadata. Will create entry only if {@code allowEmptyEntries} set to false
     * on cache configuration.
     *
     * @return Cache entry.
     */
    private GridCacheEntryEx<K, V> unwrapForMeta() {
        GridCacheEntryEx<K, V> cached = this.cached;

        long topVer = ctx.affinity().affinityTopologyVersion();

        if (cached == null || cached.obsolete())
            this.cached = cached = peekEx(topVer);

        // Try create only if cache allows empty entries.
        if (cached == null)
            throw new IgniteException("Failed to access cache entry metadata (entry is not present). " +
                "Put value to cache before accessing metadata: " + key);

        this.cached = cached = entryEx(true, topVer);

        assert cached != null;

        return cached;
    }

    /** {@inheritDoc} */
    protected GridCacheEntryEx<K, V> entryEx(boolean touch, long topVer) {
        return ctx.cache().entryEx(key, touch);
    }

    /** {@inheritDoc} */
    @Nullable protected GridCacheEntryEx<K, V> peekEx(long topVer) {
        return ctx.cache().peekEx(key);
    }

    /**
     * Reset cached value so it will be re-cached.
     */
    protected void reset() {
        cached = null;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getValue() {
        try {
            return get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V setValue(V val) {
        try {
            return set(val, CU.<K, V>empty());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object version() {
        while (true) {
            try {
                GridCacheEntryEx<K, V> e = unwrapNoCreate();

                return e == null ? ctx.versions().next() : e.version().drVersion();
            }
            catch (GridCacheEntryRemovedException ignore) {
                reset();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long expirationTime() {
        if (ttl >= 0L)
            return CU.toExpireTime(ttl);

        while (true) {
            try {
                GridCacheEntryEx<K, V> entry = unwrapNoCreate();

                return entry != null ? entry.expireTime() : 0L;
            }
            catch (GridCacheEntryRemovedException ignore) {
                reset();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        return ctx.config().getCacheMode() == LOCAL ||
            ctx.affinity().primary(ctx.localNode(), key, ctx.affinity().affinityTopologyVersion());
    }

    /** {@inheritDoc} */
    @Override public boolean backup() {
        return ctx.config().getCacheMode() != LOCAL &&
            ctx.affinity().backups(key, ctx.affinity().affinityTopologyVersion()).contains(ctx.localNode());
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        GridCacheEntryEx<K, V> e = unwrapNoCreate();

        return e == null ? ctx.cache().affinity().partition(key) : e.partition();
    }

    /** {@inheritDoc} */
    @Override public V peek() {
        try {
            return peek(MODES_SMART);
        }
        catch (IgniteCheckedException e) {
            // Should never happen.
            throw new IgniteException("Unable to perform entry peek() operation.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) throws IgniteCheckedException {
        return peek0(modes, CU.<K, V>empty(), ctx.atomic() ? null : ctx.tm().localTxx());
    }

    /**
     * @param mode Peek mode.
     * @param filter Optional entry filter.
     * @param tx Transaction to peek at (if mode is TX).
     * @return Peeked value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private V peek0(@Nullable GridCachePeekMode mode,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter, @Nullable IgniteInternalTx<K, V> tx)
        throws IgniteCheckedException {
        assert tx == null || tx.local();

        if (mode == null)
            mode = SMART;

        GridCacheProjectionImpl<K, V> prjPerCall = proxy.gateProjection();

        if (prjPerCall != null)
            filter = ctx.vararg(F.and(ctx.vararg(proxy.predicate()), filter));

        GridCacheProjectionImpl<K, V> prev = ctx.gate().enter(prjPerCall);

        try {
            while (true) {
                boolean created = false;

                GridCacheEntryEx<K, V> entry = null;

                try {
                    if (mode == DB || mode == SWAP) {
                        IgniteBiTuple<GridCacheEntryEx<K, V>, Boolean> tup = unwrapChecked(true);

                        assert tup.get2() != null;

                        created = tup.get2();

                        entry = tup.get1();
                    }
                    else
                        entry = unwrapNoCreate();

                    if (entry != null) {
                        GridTuple<V> peek = entry.peek0(false, mode, filter, tx);

                        return peek != null ? ctx.cloneOnFlag(peek.get()) : null;
                    }
                    else
                        return null;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    reset();
                }
                catch (GridCacheFilterFailedException ignored) {
                    assert false;

                    return null;
                }
                finally {
                    if (created) {
                        assert entry != null;

                        if (entry.markObsolete(ctx.versions().next()))
                            entry.context().cache().removeEntry(entry);
                    }
                }
            }
        }
        finally {
            ctx.gate().leave(prev);
        }
    }

    /**
     * @param modes Peek modes.
     * @param filter Optional entry filter.
     * @param tx Transaction to peek at (if modes contains TX value).
     * @return Peeked value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private V peek0(@Nullable Collection<GridCachePeekMode> modes,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter, IgniteInternalTx<K, V> tx) throws IgniteCheckedException {
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

    /** {@inheritDoc} */
    @Nullable @Override public V reload() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> old = ctx.gate().enter(proxy.gateProjection());

        try {
            return proxy.reload(key);
        }
        finally {
            ctx.gate().leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> reloadAsync() {
        GridCacheProjectionImpl<K, V> old = ctx.gate().enter(proxy.gateProjection());

        try {
            return proxy.reloadAsync(key);
        }
        finally {
            ctx.gate().leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean evict() {
        return proxy.evict(key);
    }

    /** {@inheritDoc} */
    @Override public boolean clear() {
        return proxy.clear(key);
    }

    /** {@inheritDoc} */
    @Override public boolean compact() throws IgniteCheckedException {
        return proxy.compact(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get() throws IgniteCheckedException {
        return proxy.get(key, isNearEnabled(ctx) ? null : cached, !ctx.keepPortable());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> getAsync() {
        return proxy.getAsync(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V set(V val, IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        // Should not pass dht entries as to near cache.
        return proxy.put(key, val, isNearEnabled(ctx) ? null : cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> setAsync(V val, IgnitePredicate<CacheEntry<K, V>>[] filter) {
        // Should not pass dht entries as to near cache.
        return proxy.putAsync(key, val, isNearEnabled(ctx) ? null : cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean setx(V val, IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        // Should not pass dht entries as to near cache.
        return proxy.putx(key, val, isNearEnabled(ctx) ? null : cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> setxAsync(V val, IgnitePredicate<CacheEntry<K, V>>[] filter) {
        // Should not pass dht entries as to near cache.
        return proxy.putxAsync(key, val, isNearEnabled(ctx) ? null : cached, ttl, filter);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V replace(V val) throws IgniteCheckedException {
        return set(val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> replaceAsync(V val) {
        return setAsync(val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(V oldVal, V newVal) throws IgniteCheckedException {
        return setx(newVal, ctx.equalsPeekArray(newVal));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replaceAsync(V oldVal, V newVal) {
        return setxAsync(newVal, ctx.equalsPeekArray(newVal));
    }

    /** {@inheritDoc} */
    @Override public long timeToLive() {
        if (ttl >= 0L)
            return ttl;

        while (true) {
            try {
                GridCacheEntryEx<K, V> entry = unwrapNoCreate();

                return entry != null ? entry.ttl() : 0L;
            }
            catch (GridCacheEntryRemovedException ignore) {
                reset();
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override public void timeToLive(long ttl) {
        A.ensure(ttl >= 0, "ttl should not be negative");

        this.ttl = ttl;

        // Make sure to update only user transaction.
        IgniteTxLocalAdapter<K, V> tx;

        if (ctx.isDht())
            tx = ctx.dht().near().context().tm().localTx();
        else
            tx = ctx.tm().localTx();

        if (tx != null)
            tx.entryTtl(ctx.txKey(key), ttl);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V setIfAbsent(V val) throws IgniteCheckedException {
        return set(val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> setIfAbsentAsync(V val) {
        return setAsync(val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean setxIfAbsent(V val) throws IgniteCheckedException {
        return setx(val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> setxIfAbsentAsync(V val) {
        return setxAsync(val, ctx.noPeekArray());
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(V val) throws IgniteCheckedException {
        return setx(val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> replacexAsync(V val) {
        return setxAsync(val, ctx.hasPeekArray());
    }

    /** {@inheritDoc} */
    @Nullable @Override public V remove(IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return proxy.remove(key, isNearEnabled(ctx) ? null : cached, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<V> removeAsync(IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return proxy.removeAsync(key, isNearEnabled(ctx) ? null : cached, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return proxy.removex(key, isNearEnabled(ctx) ? null : cached, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removexAsync(IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return proxy.removexAsync(key, isNearEnabled(ctx) ? null : cached, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(V val) throws IgniteCheckedException {
        return proxy.remove(key, val);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> removeAsync(V val) {
        return proxy.removeAsync(key, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 addMeta(String name, V1 val) {
        GridCacheEntryEx<K, V> cached = unwrapForMeta();

        return cached.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V1> V1 meta(String name) {
        GridCacheEntryEx<K, V> e = unwrapForMeta();

        return e.meta(name);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V1> V1 removeMeta(String name) {
        GridCacheEntryEx<K, V> e = unwrapForMeta();

        return e.removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 putMetaIfAbsent(String name, V1 val) {
        GridCacheEntryEx<K, V> cached = unwrapForMeta();

        return cached.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 putMetaIfAbsent(String name, Callable<V1> c) {
        GridCacheEntryEx<K, V> cached = unwrapForMeta();

        return cached.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean replaceMeta(String name, V1 curVal, V1 newVal) {
        GridCacheEntryEx<K, V> cached = unwrapForMeta();

        return cached.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean removeMeta(String name, V1 val) {
        GridCacheEntryEx e = unwrapForMeta();

        return e.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked() {
        while (true) {
            try {
                GridCacheEntryEx<K, V> e = unwrapNoCreate();

                return e != null && e.lockedByAny();
            }
            catch (GridCacheEntryRemovedException ignore) {
                reset();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread() {
        while (true) {
            try {
                GridCacheEntryEx<K, V> e = unwrapNoCreate();

                if (e == null)
                    return false;

                // Delegate to near if dht.
                if (e.isDht() && isNearEnabled(ctx)) {
                    GridCache<K, V> near = ctx.isDht() ? ctx.dht().near() : ctx.near();

                    return near.isLockedByThread(key) || e.lockedByThread();
                }

                return e.lockedByThread();
            }
            catch (GridCacheEntryRemovedException ignore) {
                reset();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean lock(long timeout,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        return proxy.lock(key, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAsync(long timeout,
        @Nullable IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return proxy.lockAsync(key, timeout, filter);
    }

    /** {@inheritDoc} */
    @Override public void unlock(IgnitePredicate<CacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        proxy.unlock(key, filter);
    }

    /** {@inheritDoc} */
    @Override public boolean isCached() {
        GridCacheEntryEx<K, V> cached = unwrapNoCreate();

        return cached != null && !cached.obsolete();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
        out.writeObject(proxy);
        out.writeObject(key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridCacheContext<K, V>)in.readObject();
        proxy = (GridCacheProxyImpl<K, V>)in.readObject();
        key = (K)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        GridCacheEntryEx<K, V> cached = this.cached;

        if (cached == null)
            this.cached = cached = entryEx(true, ctx.affinity().affinityTopologyVersion());

        return cached.memorySize();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if(clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof GridCacheEntryImpl))
            return false;

        GridCacheEntryImpl<K, V> other = (GridCacheEntryImpl<K, V>)obj;

        V v1 = peek();
        V v2 = other.peek();

        return key.equals(other.key) && F.eq(ctx.cache().name(), other.ctx.cache().name()) && F.eq(v1, v2);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEntryImpl.class, this);
    }
}
