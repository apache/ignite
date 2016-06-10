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
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of GridCacheConcurrentMap that will delegate all method calls to corresponding local partition.
 */
public class GridCachePartitionedConcurrentMap implements GridCacheConcurrentMap {
    /** Context. */
    private final GridCacheContext ctx;

    /**
     * Constructor.
     * @param ctx Context.
     */
    public GridCachePartitionedConcurrentMap(GridCacheContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @param create Create flag.
     * @return Local partition.
     */
    @Nullable private GridDhtLocalPartition localPartition(
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        boolean create
    ) {
        int p = key.partition();

        if (p == -1)
            p = ctx.affinity().partition(key);

        return ctx.topology().localPartition(p, topVer, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry getEntry(KeyCacheObject key) {
        GridDhtLocalPartition part = localPartition(key, AffinityTopologyVersion.NONE, false);

        if (part == null)
            return null;

        return part.getEntry(key);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry putEntryIfObsoleteOrAbsent(AffinityTopologyVersion topVer, KeyCacheObject key,
        @Nullable CacheObject val, boolean create, boolean touch) {
        GridDhtLocalPartition part = localPartition(key, topVer, create);

        if (part == null)
            return null;

        return part.putEntryIfObsoleteOrAbsent(topVer, key, val, create, touch);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int size = 0;

        for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions())
            size += part.size();

        return size;
    }

    /** {@inheritDoc} */
    @Override public int publicSize() {
        int size = 0;

        for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions())
            size += part.publicSize();

        return size;
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(GridCacheEntryEx e) {
        localPartition(e.key(), AffinityTopologyVersion.NONE, true).incrementPublicSize(e);
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(GridCacheEntryEx e) {
        localPartition(e.key(), AffinityTopologyVersion.NONE, true).decrementPublicSize(e);
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(GridCacheEntryEx entry) {
        GridDhtLocalPartition part = localPartition(entry.key(), AffinityTopologyVersion.NONE, false);

        if (part == null)
            return false;

        return part.removeEntry(entry);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry randomEntry() {
        return entries().iterator().next();
    }

    /** {@inheritDoc} */
    @Override public Set<KeyCacheObject> keySet(final CacheEntryPredicate... filter) {
        return new PartitionedSet<KeyCacheObject>() {
            @Override protected Set<KeyCacheObject> set(GridDhtLocalPartition part) {
                return part.keySet(filter);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridCacheMapEntry> entries(final CacheEntryPredicate... filter) {
        return new Iterable<GridCacheMapEntry>() {
            @Override public Iterator<GridCacheMapEntry> iterator() {
                return new PartitionedIterator<GridCacheMapEntry>() {
                    @Override protected Iterator<GridCacheMapEntry> iterator(GridDhtLocalPartition part) {
                        return part.entries(filter).iterator();
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridCacheMapEntry> allEntries(final CacheEntryPredicate... filter) {
        return new Iterable<GridCacheMapEntry>() {
            @Override public Iterator<GridCacheMapEntry> iterator() {
                return new PartitionedIterator<GridCacheMapEntry>() {
                    @Override protected Iterator<GridCacheMapEntry> iterator(GridDhtLocalPartition part) {
                        return part.allEntries(filter).iterator();
                    }
                };
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheMapEntry> entrySet(final CacheEntryPredicate... filter) {
        return new PartitionedSet<GridCacheMapEntry>() {
            @Override protected Set<GridCacheMapEntry> set(GridDhtLocalPartition part) {
                return part.entrySet(filter);
            }
        };
    }

    private abstract class PartitionedIterator<T> implements Iterator<T> {
        private Iterator<GridDhtLocalPartition> partsIterator = ctx.topology().currentLocalPartitions().iterator();

        private Iterator<T> currentIterator = partsIterator.hasNext() ? iterator(partsIterator.next()) :
            Collections.<T>emptyIterator();

        protected abstract Iterator<T> iterator(GridDhtLocalPartition part);

        @Override public boolean hasNext() {
            if (currentIterator.hasNext())
                return true;

            while (partsIterator.hasNext()) {
                currentIterator = iterator(partsIterator.next());

                if (currentIterator.hasNext())
                    return true;
            }

            return false;
        }

        @Override public T next() {
            if (hasNext())
                return currentIterator.next();
            else
                throw new NoSuchElementException();
        }
    }

    private abstract class PartitionedSet<T> extends AbstractSet<T> {
        protected abstract Set<T> set(GridDhtLocalPartition part);

        @Override public Iterator<T> iterator() {
            return new PartitionedIterator<T>() {
                @Override protected Iterator<T> iterator(GridDhtLocalPartition part) {
                    return set(part).iterator();
                }
            };
        }

        @Override public int size() {
            return F.size(iterator());
        }

        @Override public boolean contains(Object o) {
            for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions()) {
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
