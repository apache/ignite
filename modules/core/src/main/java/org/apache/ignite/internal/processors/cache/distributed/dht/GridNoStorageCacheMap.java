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

import java.util.Map;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Empty cache map that will never store any entries.
 */
public class GridNoStorageCacheMap extends GridCacheConcurrentMap {
    /** Empty triple. */
    private final GridTriple<GridCacheMapEntry> emptyTriple =
        new GridTriple<>(null, null, null);

    /**
     * @param ctx Cache context.
     */
    public GridNoStorageCacheMap(GridCacheContext ctx) {
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
    @Override public GridCacheMapEntry randomEntry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry getEntry(Object key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry putEntry(AffinityTopologyVersion topVer,
        KeyCacheObject key,
        @Nullable CacheObject val)
    {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(
        AffinityTopologyVersion topVer,
        KeyCacheObject key,
        @Nullable CacheObject val,
        boolean create)
    {
        if (create) {
            GridCacheMapEntry entry = ctx.useOffheapEntry() ?
                new GridDhtOffHeapCacheEntry(ctx, topVer, key, hash(key.hashCode()), val, null, 0) :
                new GridDhtCacheEntry(ctx, topVer, key, hash(key.hashCode()), val, null, 0);

            return new GridTriple<>(entry, null, null);
        }
        else
            return emptyTriple;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<KeyCacheObject, CacheObject> m) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(GridCacheEntryEx e) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNoStorageCacheMap.class, this);
    }
}