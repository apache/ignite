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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCachePeekMode.*;

/**
 * Entry implementation for passing raw cache map entries through filters.
 * Needed to protect original entry from invalidation by filter's peek.
 */
@SuppressWarnings("unchecked")
public class GridCacheFilterEvaluationEntry<K, V> implements GridCacheEntry<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Allow transactional peeks flag. */
    private boolean allowTx;

    /** Key. */
    private K key;

    /** Value. */
    private V val;

    /** Entry implementation. */
    private GridCacheEntryEx<K, V> impl;

    /**
     * @param key Key.
     * @param val Value.
     * @param impl Entry implementation.
     * @param allowTx Whether value access is allowed in transactional mode.
     */
    public GridCacheFilterEvaluationEntry(K key, V val, GridCacheEntryEx<K, V> impl, boolean allowTx) {
        this(key, val, impl);

        this.allowTx = allowTx;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param impl Entry implementation.
     */
    public GridCacheFilterEvaluationEntry(K key, V val, GridCacheEntryEx<K, V> impl) {
        assert key != null;

        this.key = key;
        this.val = val;
        this.impl = impl;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public V setValue(V val) {
        throw new UnsupportedOperationException("setValue");
    }

    /** {@inheritDoc} */
    @Nullable @Override public V peek() {
        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V peek(@Nullable Collection<GridCachePeekMode> modes) {
        for (GridCachePeekMode mode : modes) {
            V val = peek(mode);

            if (val != null)
                return val;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable private  V peek(@Nullable GridCachePeekMode mode) {
        return !allowTx && mode == TX ? null : val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get() throws IgniteCheckedException {
        throw new UnsupportedOperationException("get");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync() {
        throw new UnsupportedOperationException("getAsync");
    }

    /** {@inheritDoc} */
    @Override public GridCacheProjection<K, V> projection() {
        throw new UnsupportedOperationException("parent");
    }

    /** {@inheritDoc} */
    @Nullable @Override public V reload() throws IgniteCheckedException {
        throw new UnsupportedOperationException("reload");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> reloadAsync() {
        throw new UnsupportedOperationException("reloadAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Object version() {
        try {
            return impl.version().drVersion();
        }
        catch (GridCacheEntryRemovedException e) {
            assert false : "Entry should not become obsolete while holding lock";

            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long expirationTime() {
        try {
            return impl.expireTime();
        }
        catch (GridCacheEntryRemovedException e) {
            assert false : "Entry should not become obsolete while holding lock";

            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long timeToLive() {
        try {
            return impl.ttl();
        }
        catch (GridCacheEntryRemovedException e) {
            assert false : "Entry should not become obsolete while holding lock";

            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void timeToLive(long ttl) {
        throw new UnsupportedOperationException("timeToLive(long)");
    }

    /** {@inheritDoc} */
    @Override public boolean primary() {
        throw new UnsupportedOperationException("primary");
    }

    /** {@inheritDoc} */
    @Override public boolean backup() {
        throw new UnsupportedOperationException("backup");
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return impl.partition();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V set(V val, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws IgniteCheckedException {
        throw new UnsupportedOperationException("set");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> setAsync(V val, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        throw new UnsupportedOperationException("setAsync");
    }

    /** {@inheritDoc} */
    @Nullable @Override public V setIfAbsent(V val) throws IgniteCheckedException {
        throw new UnsupportedOperationException("setIfAbsent");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> setIfAbsentAsync(V val) {
        throw new UnsupportedOperationException("setIfAbsentAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean setx(V val, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws IgniteCheckedException {
        throw new UnsupportedOperationException("setx");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> setxAsync(V val,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        throw new UnsupportedOperationException("setxAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean setxIfAbsent(@Nullable V val) throws IgniteCheckedException {
        throw new UnsupportedOperationException("setxIfAbsent");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> setxIfAbsentAsync(V val) {
        throw new UnsupportedOperationException("setxIfAbsentAsync");
    }

    /** {@inheritDoc} */
    @Nullable @Override public V replace(V val) throws IgniteCheckedException {
        throw new UnsupportedOperationException("replace");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> replaceAsync(V val) {
        throw new UnsupportedOperationException("replaceAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean replacex(V val) throws IgniteCheckedException {
        throw new UnsupportedOperationException("replacex");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replacexAsync(V val) {
        throw new UnsupportedOperationException("replacexAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean replace(V oldVal, V newVal) throws IgniteCheckedException {
        throw new UnsupportedOperationException("replace");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(V oldVal, V newVal) {
        throw new UnsupportedOperationException("replaceAsync");
    }

    /** {@inheritDoc} */
    @Nullable @Override public V remove(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws IgniteCheckedException {
        throw new UnsupportedOperationException("remove");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> removeAsync(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        throw new UnsupportedOperationException("removeAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean removex(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        throw new UnsupportedOperationException("removex");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removexAsync(@Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        throw new UnsupportedOperationException("removexAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean remove(V val) throws IgniteCheckedException {
        throw new UnsupportedOperationException("remove");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(V val) {
        throw new UnsupportedOperationException("removeAsync");
    }

    /** {@inheritDoc} */
    @Override public boolean evict() {
        throw new UnsupportedOperationException("evict");
    }

    /** {@inheritDoc} */
    @Override public boolean clear() {
        throw new UnsupportedOperationException("clear");
    }

    /** {@inheritDoc} */
    @Override public boolean compact()
        throws IgniteCheckedException {
        throw new UnsupportedOperationException("compact");
    }

    /** {@inheritDoc} */
    @Override public boolean lock(long timeout, @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws IgniteCheckedException {
        throw new UnsupportedOperationException("lock");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> lockAsync(long timeout,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>... filter) {
        throw new UnsupportedOperationException("lockAsync");
    }

    /** {@inheritDoc} */
    @Override public void unlock(IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException {
        throw new UnsupportedOperationException("unlock");
    }

    /** {@inheritDoc} */
    @Override public boolean isCached() {
        return !impl.obsolete();
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        return impl.memorySize();
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(GridMetadataAware from) {
        impl.copyMeta(from);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> data) {
        impl.copyMeta(data);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V addMeta(String name, V val) {
        return impl.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V putMetaIfAbsent(String name, V val) {
        return impl.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V putMetaIfAbsent(String name, Callable<V> c) {
        return impl.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMetaIfAbsent(String name, V val) {
        return impl.addMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V addMetaIfAbsent(String name, @Nullable Callable<V> c) {
        return impl.addMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Override public <V> V meta(String name) {
        return impl.meta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> V removeMeta(String name) {
        return impl.removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean removeMeta(String name, V val) {
        return impl.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> Map<String, V> allMeta() {
        return impl.allMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return impl.hasMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean hasMeta(String name, V val) {
        return impl.hasMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean replaceMeta(String name, V curVal, V newVal) {
        return impl.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if(clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheFilterEvaluationEntry.class, this);
    }
}
