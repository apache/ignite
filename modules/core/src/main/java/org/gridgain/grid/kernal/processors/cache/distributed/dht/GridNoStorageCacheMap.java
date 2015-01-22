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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Empty cache map that will never store any entries.
 */
public class GridNoStorageCacheMap<K, V> extends GridCacheConcurrentMap<K, V> {
    /** Empty triple. */
    private final GridTriple<GridCacheMapEntry<K,V>> emptyTriple =
        new GridTriple<>(null, null, null);

    /**
     * @param ctx Cache context.
     */
    public GridNoStorageCacheMap(GridCacheContext<K, V> ctx) {
        super(ctx, 0, 0.75f, 1);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int publicSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> randomEntry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> getEntry(Object key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> putEntry(long topVer, K key, @Nullable V val, long ttl) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public GridTriple<GridCacheMapEntry<K, V>> putEntryIfObsoleteOrAbsent(long topVer, K key, @Nullable V val,
        long ttl, boolean create) {
        if (create) {
            GridCacheMapEntry<K, V> entry = new GridDhtCacheEntry<>(ctx, topVer, key, hash(key.hashCode()), val,
                null, 0, 0);

            return new GridTriple<>(entry, null, null);
        }
        else
            return emptyTriple;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m, long ttl) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(GridCacheEntryEx<K, V> e) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> removeEntryIfObsolete(K key) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNoStorageCacheMap.class, this);
    }
}
