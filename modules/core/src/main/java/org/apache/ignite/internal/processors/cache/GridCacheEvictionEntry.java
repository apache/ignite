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
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Entry wrapper that never obscures obsolete entries from user.
 */
public class GridCacheEvictionEntry<K, V> implements EvictableEntry<K, V> {
    /** */
    private static final String META_KEY = "ignite-eviction-entry-meta";

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
    }

    /** {@inheritDoc} */
    @Override public K getKey() throws IllegalStateException {
        return cached.key();
    }

    @Override public boolean isCached() {
        return !cached.obsoleteOrDeleted();
    }

    @Override public boolean evict() {
        GridCacheContext<K, V> ctx = cached.context();

        try {
            assert ctx != null;
            assert ctx.evicts() != null;

            return ctx.evicts().evict(cached, null, false, null);
        }
        catch (IgniteCheckedException e) {
            U.error(ctx.grid().log(), "Failed to evict entry from cache: " + cached, e);

            return false;
        }
    }

    /**
     * @return Peeks value.
     */
    @Nullable public V peek() {
        try {
            return cached.peek(GridCachePeekMode.GLOBAL);
        }
        catch (GridCacheEntryRemovedException e) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getValue() throws IllegalStateException {
        throw new UnsupportedOperationException("Operation not supported during eviction.");
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <T> T addMeta(T val) {
        return cached.addMeta(META_KEY, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <T> T meta() {
        return cached.meta(META_KEY);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <T> T removeMeta() {
        return cached.removeMeta(META_KEY);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <T> boolean removeMeta(T val) {
        return cached.removeMeta(META_KEY, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <T> T putMetaIfAbsent(T val) {
        return cached.putMetaIfAbsent(META_KEY, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <T> boolean replaceMeta(T curVal, T newVal) {
        return cached.replaceMeta(META_KEY,curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if(clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
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

            return cached.key().equals(other.getKey());
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionEntry.class, this);
    }
}
