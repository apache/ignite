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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapInterface;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

public class GridCachePartitionedConcurrentMap implements GridCacheConcurrentMapInterface {

    private final GridCacheContext ctx;

    public GridCachePartitionedConcurrentMap(GridCacheContext ctx) {
        this.ctx = ctx;
    }

    @Nullable @Override public GridCacheMapEntry getEntry(Object key) {
        GridDhtLocalPartition part = ctx.topology().localPartition(key, false);

        if (part == null)
            return null;

        return part.getEntry(key);
    }

    @Override
    public GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(AffinityTopologyVersion topVer, KeyCacheObject key,
        @Nullable CacheObject val, boolean create) {
        GridDhtLocalPartition part = ctx.topology().localPartition(key, create);

        if (part == null)
            return new GridTriple<>(null, null, null);

        return part.putEntryIfObsoleteOrAbsent(topVer, key, val, create);
    }

    @Override public GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key) {
        GridDhtLocalPartition part = ctx.topology().localPartition(key, false);

        if (part == null)
            return null;

        return part.removeEntryIfObsolete(key);
    }

    @Override public int size() {
        int size = 0;

        for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
            size += part.size();
        }

        return size;
    }

    @Override public boolean removeEntry(GridCacheEntryEx entry) {
        GridDhtLocalPartition part = ctx.topology().localPartition(entry.key(), false);

        if (part == null)
            return false;

        return part.removeEntry(entry);
    }

    @Nullable @Override public GridCacheMapEntry randomEntry() {
        return null;
    }

    @Override public Set<KeyCacheObject> keySet(CacheEntryPredicate... filter) {
        Set<KeyCacheObject> set = new HashSet<>();

        for (GridDhtLocalPartition partition : ctx.topology().localPartitions()) {
            set.addAll(partition.keySet(filter));
        }

        return set;
    }

    @Override public Iterable<? extends GridCacheEntryEx> entries(CacheEntryPredicate... filter) {
        final List<Iterator<GridCacheEntryEx>> iterators = new ArrayList<>();

        for (GridDhtLocalPartition partition : ctx.topology().localPartitions()) {
            iterators.add((Iterator<GridCacheEntryEx>) partition.entries(filter).iterator());
        }

        return new Iterable<GridCacheEntryEx>() {
            @Override public Iterator<GridCacheEntryEx> iterator() {
                return F.flatIterators(iterators);
            }
        };
    }
}
