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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtOffHeapCacheEntry;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.jetbrains.annotations.Nullable;

/**
 * Empty cache map that will never store any entries.
 */
public class GridNoStorageCacheMap implements GridCacheConcurrentMap {

    /** Empty triple. */
    private final GridTriple<GridCacheMapEntry> emptyTriple =
        new GridTriple<>(null, null, null);

    private final GridCacheContext ctx;

    /**
     * @param ctx Cache context.
     */
    public GridNoStorageCacheMap(GridCacheContext ctx) {
        this.ctx = ctx;
    }

    @Nullable @Override public GridCacheMapEntry getEntry(Object key) {
        return null;
    }

    @Override
    public GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(AffinityTopologyVersion topVer, KeyCacheObject key,
        @Nullable CacheObject val, boolean create) {
        if (create) {
            GridCacheMapEntry entry = ctx.useOffheapEntry() ?
                new GridDhtOffHeapCacheEntry(ctx, topVer, key, key.hashCode(), val) :
                new GridDhtCacheEntry(ctx, topVer, key, key.hashCode(), val);

            return new GridTriple<>(entry, null, null);
        }
        else
            return emptyTriple;
    }

    @Override public boolean removeEntry(GridCacheEntryEx entry) {
        throw new AssertionError();
    }

    @Override public GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key) {
        throw new AssertionError();
    }

    @Override public int size() {
        return 0;
    }

    @Override public int publicSize() {
        return 0;
    }

    @Override public void incrementPublicSize(GridCacheEntryEx e) {
        // noop
    }

    @Override public void decrementPublicSize(GridCacheEntryEx e) {
        // noop
    }

    @Nullable @Override public GridCacheEntryEx randomEntry() {
        return null;
    }

    @Override public Set<KeyCacheObject> keySet(CacheEntryPredicate... filter) {
        return Collections.emptySet();
    }

    @Override public Iterable<GridCacheEntryEx> entries(CacheEntryPredicate... filter) {
        return Collections.emptySet();
    }

    @Override public Set<GridCacheEntryEx> entrySet(CacheEntryPredicate... filter) {
        return Collections.emptySet();
    }
}
