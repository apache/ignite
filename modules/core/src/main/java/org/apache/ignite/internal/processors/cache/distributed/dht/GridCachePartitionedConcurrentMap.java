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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.PartitionedReadOnlySet;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of GridCacheConcurrentMap that will delegate all method calls to corresponding local partition.
 */
public class GridCachePartitionedConcurrentMap implements GridCacheConcurrentMap {

    /** Context. */
    private final GridCacheContext ctx;

    private volatile GridDhtPartitionTopology topology;

    /**
     * Constructor.
     * @param ctx Context.
     */
    public GridCachePartitionedConcurrentMap(GridCacheContext ctx) {
        this.ctx = ctx;
    }

    private GridDhtPartitionTopology topology() {
        if (topology == null)
            topology = ctx.topology();

        return topology;
    }

    @Nullable private GridDhtLocalPartition localPartition(KeyCacheObject key, boolean create) {
        if (key.partition() == -1)
            return topology().localPartition(key, create);
        else
            return topology().localPartition(key.partition(), AffinityTopologyVersion.NONE, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry getEntry(KeyCacheObject key) {
        GridDhtLocalPartition part = localPartition(key, false);

        if (part == null)
            return null;

        return part.getEntry(key);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry putEntryIfObsoleteOrAbsent(AffinityTopologyVersion topVer, KeyCacheObject key,
        @Nullable CacheObject val, boolean create, boolean touch) {
        GridDhtLocalPartition part = localPartition(key, create);

        if (part == null)
            return null;

        return part.putEntryIfObsoleteOrAbsent(topVer, key, val, create, touch);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int size = 0;

        for (GridDhtLocalPartition part : topology().localPartitions()) {
            size += part.size();
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public int publicSize() {
        int size = 0;

        for (GridDhtLocalPartition part : topology().localPartitions()) {
            size += part.publicSize();
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(GridCacheEntryEx e) {
        localPartition(e.key(), true).incrementPublicSize(e);
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(GridCacheEntryEx e) {
        localPartition(e.key(), true).decrementPublicSize(e);
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(GridCacheEntryEx entry) {
        GridDhtLocalPartition part = localPartition(entry.key(), false);

        if (part == null)
            return false;

        return part.removeEntry(entry);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry randomEntry() {
        return entries().iterator().next();
    }

    /** {@inheritDoc} */
    @Override public Set<KeyCacheObject> keySet(CacheEntryPredicate... filter) {
        Collection<Set<KeyCacheObject>> sets = new ArrayList<>();

        for (GridDhtLocalPartition partition : topology().localPartitions()) {
            sets.add(partition.keySet(filter));
        }

        return new PartitionedReadOnlySet<>(sets);
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridCacheMapEntry> entries(final CacheEntryPredicate... filter) {
        return new Iterable<GridCacheMapEntry>() {
            @Override public Iterator<GridCacheMapEntry> iterator() {
                List<Iterator<GridCacheMapEntry>> iterators = new ArrayList<>();

                for (GridDhtLocalPartition partition : ctx.topology().localPartitions()) {
                    iterators.add(partition.entries(filter).iterator());
                }

                return F.flatIterators(iterators);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridCacheMapEntry> allEntries(final CacheEntryPredicate... filter) {
        return new Iterable<GridCacheMapEntry>() {
            @Override public Iterator<GridCacheMapEntry> iterator() {
                List<Iterator<GridCacheMapEntry>> iterators = new ArrayList<>();

                for (GridDhtLocalPartition partition : topology().localPartitions()) {
                    iterators.add(partition.allEntries(filter).iterator());
                }

                return F.flatIterators(iterators);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheMapEntry> entrySet(CacheEntryPredicate... filter) {
        Collection<Set<GridCacheMapEntry>> sets = new ArrayList<>();

        for (GridDhtLocalPartition partition : topology().localPartitions()) {
            sets.add(partition.entrySet(filter));
        }

        return new PartitionedReadOnlySet<>(sets);
    }
}
