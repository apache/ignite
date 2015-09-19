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

package org.apache.ignite.internal.processors.cache.eviction;

import javax.cache.Cache;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Mock cache entry.
 */
public class GridCacheMockEntry<K, V> extends GridMetadataAwareAdapter implements Cache.Entry<K, V>, EvictableEntry<K, V> {
    /** */
    private static final int META_KEY = EntryKey.values().length; //+1 to maximum value (test only case)

    /** */
    @GridToStringInclude
    private K key;

    /** */
    @GridToStringInclude
    private boolean evicted;

    /**
     * Constructor.
     *
     * @param key Key.
     */
    public GridCacheMockEntry(K key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public K getKey() throws IllegalStateException {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() throws IllegalStateException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean evict() {
        evicted = true;

        onEvicted();

        return true;
    }

    /**
     *
     */
    private void onEvicted() {
        removeAllMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean isCached() {
        return !evicted;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 0;
    }

    /**
     * @return Evicted or not.
     */
    public boolean isEvicted() {
        return evicted;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T addMeta(T val) {
        return addMeta(META_KEY, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T meta() {
        return meta(META_KEY);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T removeMeta() {
        return removeMeta(META_KEY);
    }

    /** {@inheritDoc} */
    @Override public <T> boolean removeMeta(T val) {
        return removeMeta(META_KEY, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T putMetaIfAbsent(T val) {
        return putMetaIfAbsent(META_KEY, val);
    }

    /** {@inheritDoc} */
    @Override public <T> boolean replaceMeta(T curVal, T newVal) {
        return replaceMeta(META_KEY, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMockEntry.class, this);
    }
}