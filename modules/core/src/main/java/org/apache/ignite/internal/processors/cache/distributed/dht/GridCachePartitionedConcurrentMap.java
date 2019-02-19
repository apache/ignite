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

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of GridCacheConcurrentMap that will delegate all method calls to corresponding local partition.
 */
public class GridCachePartitionedConcurrentMap implements GridCacheConcurrentMap {
    /** Cache group. */
    private final CacheGroupContext grp;

    /**
     * @param grp Cache group.
     */
    GridCachePartitionedConcurrentMap(CacheGroupContext grp) {
        this.grp = grp;
    }

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param topVer Topology version.
     * @param create Create flag.
     * @return Local partition.
     */
    @Nullable private GridDhtLocalPartition localPartition(
        GridCacheContext cctx,
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        boolean create
    ) {
        int p = key.partition();

        if (p == -1)
            p = cctx.affinity().partition(key);

        return grp.topology().localPartition(p, topVer, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry getEntry(GridCacheContext ctx, KeyCacheObject key) {
        GridDhtLocalPartition part = localPartition(ctx, key, AffinityTopologyVersion.NONE, false);

        if (part == null)
            return null;

        return part.getEntry(ctx, key);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry putEntryIfObsoleteOrAbsent(GridCacheContext ctx, AffinityTopologyVersion topVer,
        KeyCacheObject key,
        boolean create,
        boolean touch) {
        while (true) {
            GridDhtLocalPartition part = localPartition(ctx, key, topVer, create);

            if (part == null)
                return null;

            GridCacheMapEntry res = part.putEntryIfObsoleteOrAbsent(ctx, topVer, key, create, touch);

            if (res != null || !create)
                return res;

            // Otherwise partition was concurrently evicted and should be re-created on next iteration.
        }
    }

    /** {@inheritDoc} */
    @Override public int internalSize() {
        int size = 0;

        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
            size += part.internalSize();

        return size;
    }

    /** {@inheritDoc} */
    @Override public int publicSize(int cacheId) {
        int size = 0;

        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
            size += part.publicSize(cacheId);

        return size;
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(CacheMapHolder hld, GridCacheEntryEx e) {
        localPartition(e.context(), e.key(), AffinityTopologyVersion.NONE, true).incrementPublicSize(hld, e);
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(CacheMapHolder hld, GridCacheEntryEx e) {
        localPartition(e.context(), e.key(), AffinityTopologyVersion.NONE, true).decrementPublicSize(hld, e);
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(GridCacheEntryEx entry) {
        GridDhtLocalPartition part = localPartition(entry.context(), entry.key(), AffinityTopologyVersion.NONE, false);

        if (part == null)
            return false;

        return part.removeEntry(entry);
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridCacheMapEntry> entries(final int cacheId, final CacheEntryPredicate... filter) {
        return new Iterable<GridCacheMapEntry>() {
            @Override public Iterator<GridCacheMapEntry> iterator() {
                return new PartitionedIterator<GridCacheMapEntry>() {
                    @Override protected Iterator<GridCacheMapEntry> iterator(GridDhtLocalPartition part) {
                        return part.entries(cacheId, filter).iterator();
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheMapEntry> entrySet(final int cacheId, final CacheEntryPredicate... filter) {
        return new PartitionedSet<GridCacheMapEntry>() {
            @Override protected Set<GridCacheMapEntry> set(GridDhtLocalPartition part) {
                return part.entrySet(cacheId, filter);
            }
        };
    }

    /**
     * Combined iterator over current local partitions.
     */
    private abstract class PartitionedIterator<T> implements Iterator<T> {
        /** Partitions iterator. */
        private Iterator<GridDhtLocalPartition> partsIter = grp.topology().currentLocalPartitions().iterator();

        /** Current partition iterator. */
        private Iterator<T> currIter = partsIter.hasNext() ? iterator(partsIter.next()) :
            Collections.<T>emptyIterator();

        /**
         * @param part Partition.
         * @return Iterator over entries of given partition.
         */
        protected abstract Iterator<T> iterator(GridDhtLocalPartition part);

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (currIter.hasNext())
                return true;

            while (partsIter.hasNext()) {
                currIter = iterator(partsIter.next());

                if (currIter.hasNext())
                    return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public T next() {
            if (hasNext())
                return currIter.next();
            else
                throw new NoSuchElementException();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }

    /**
     * Read-only partitioned set.
     */
    private abstract class PartitionedSet<T> extends AbstractSet<T> {
        /**
         * @param part Partition.
         * @return Set of entries from given partition.
         */
        protected abstract Set<T> set(GridDhtLocalPartition part);

        /** {@inheritDoc} */
        @Override public Iterator<T> iterator() {
            return new PartitionedIterator<T>() {
                @Override protected Iterator<T> iterator(GridDhtLocalPartition part) {
                    return set(part).iterator();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public int size() {
            int size = 0;

            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                size += set(part).size();

            return size;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
                if (set(part).contains(o))
                    return true;
            }

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePartitionedConcurrentMap.class, this);
    }
}
