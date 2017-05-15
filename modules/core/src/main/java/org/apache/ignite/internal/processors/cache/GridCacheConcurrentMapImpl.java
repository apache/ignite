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

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_DESTROYED;

/**
 * Implementation of concurrent cache map.
 */
public abstract class GridCacheConcurrentMapImpl implements GridCacheConcurrentMap {
    /** Default load factor. */
    private static final float DFLT_LOAD_FACTOR = 0.75f;

    /** Default concurrency level. */
    private static final int DFLT_CONCUR_LEVEL = Runtime.getRuntime().availableProcessors() * 2;

    /** Internal map. */
    private final ConcurrentMap<KeyCacheObject, GridCacheMapEntry> map;

    /** Map entry factory. */
    private final GridCacheMapEntryFactory factory;

    /** Cache context. */
    private final GridCacheContext ctx;

    /**
     * Creates a new, empty map with the specified initial
     * capacity.
     *
     * @param ctx Cache context.
     * @param factory Entry factory.
     * @param initialCapacity the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.

     * @throws IllegalArgumentException if the initial capacity is
     *      negative.
     */
    public GridCacheConcurrentMapImpl(GridCacheContext ctx, GridCacheMapEntryFactory factory, int initialCapacity) {
        this(ctx, factory, initialCapacity, DFLT_LOAD_FACTOR, DFLT_CONCUR_LEVEL);
    }

    /**
     * Creates a new, empty map with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param ctx Cache context.
     * @param factory Entry factory.
     * @param initialCapacity the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param concurrencyLevel the estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurrencyLevel are
     *      non-positive.
     */
    public GridCacheConcurrentMapImpl(
        GridCacheContext ctx,
        GridCacheMapEntryFactory factory,
        int initialCapacity,
        float loadFactor,
        int concurrencyLevel
    ) {
        this.ctx = ctx;
        this.factory = factory;

        map = new ConcurrentHashMap8<>(initialCapacity, loadFactor, concurrencyLevel);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry getEntry(KeyCacheObject key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry putEntryIfObsoleteOrAbsent(final AffinityTopologyVersion topVer,
        KeyCacheObject key,
        final boolean create,
        final boolean touch) {
        GridCacheMapEntry cur = null;
        GridCacheMapEntry created = null;
        GridCacheMapEntry created0 = null;
        GridCacheMapEntry doomed = null;

        boolean done = false;
        boolean reserved = false;
        int sizeChange = 0;

        try {
            while (!done) {
                GridCacheMapEntry entry = map.get(key);
                created = null;
                doomed = null;

                if (entry == null) {
                    if (create) {
                        if (created0 == null) {
                            if (!reserved) {
                                if (!reserve())
                                    return null;

                                reserved = true;
                            }

                            created0 = factory.create(ctx, topVer, key);
                        }

                        cur = created = created0;

                        done = map.putIfAbsent(created.key(), created) == null;
                    }
                    else
                        done = true;
                }
                else {
                    if (entry.obsolete()) {
                        doomed = entry;

                        if (create) {
                            if (created0 == null) {
                                if (!reserved) {
                                    if (!reserve())
                                        return null;

                                    reserved = true;
                                }

                                created0 = factory.create(ctx, topVer, key);
                            }

                            cur = created = created0;

                            done = map.replace(entry.key(), doomed, created);
                        }
                        else
                            done = map.remove(entry.key(), doomed);
                    }
                    else {
                        cur = entry;

                        done = true;
                    }
                }
            }

            sizeChange = 0;

            if (doomed != null) {
                synchronized (doomed) {
                    if (!doomed.deleted())
                        sizeChange--;
                }

                if (ctx.events().isRecordable(EVT_CACHE_ENTRY_DESTROYED))
                    ctx.events().addEvent(doomed.partition(),
                        doomed.key(),
                        ctx.localNodeId(),
                        (IgniteUuid)null,
                        null,
                        EVT_CACHE_ENTRY_DESTROYED,
                        null,
                        false,
                        null,
                        false,
                        null,
                        null,
                        null,
                        true);
            }

            if (created != null) {
                sizeChange++;

                if (ctx.events().isRecordable(EVT_CACHE_ENTRY_CREATED))
                    ctx.events().addEvent(created.partition(),
                        created.key(),
                        ctx.localNodeId(),
                        (IgniteUuid)null,
                        null,
                        EVT_CACHE_ENTRY_CREATED,
                        null,
                        false,
                        null,
                        false,
                        null,
                        null,
                        null,
                        true);

                if (touch)
                    ctx.evicts().touch(
                        cur,
                        topVer);
            }

            assert Math.abs(sizeChange) <= 1;

            return cur;
        }
        finally {
            if (reserved)
                release(sizeChange, cur);
            else {
                if (sizeChange != 0) {
                    assert sizeChange == -1;

                    decrementPublicSize(cur);
                }
            }
        }
    }

    /**
     *
     */
    protected boolean reserve() {
        return true;
    }

    /**
     *
     */
    protected void release() {
        // No-op.
    }

    /**
     * @param sizeChange Size delta.
     * @param e Map entry.
     */
    protected void release(int sizeChange, GridCacheEntryEx e) {
        if (sizeChange == 1)
            incrementPublicSize(e);
        else if (sizeChange == -1)
            decrementPublicSize(e);
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(final GridCacheEntryEx entry) {
        boolean removed = map.remove(entry.key(), entry);

        if (removed) {
            if (ctx.events().isRecordable(EVT_CACHE_ENTRY_DESTROYED))
                // Event notification.
                ctx.events().addEvent(entry.partition(), entry.key(), ctx.localNodeId(), (IgniteUuid)null, null,
                    EVT_CACHE_ENTRY_DESTROYED, null, false, null, false, null, null, null, false);

            synchronized (entry) {
                if (!entry.deleted())
                    decrementPublicSize(entry);
            }
        }

        return removed;
    }

    /** {@inheritDoc} */
    @Override public int internalSize() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMapEntry> entries(final CacheEntryPredicate... filter) {
        final IgnitePredicate<GridCacheMapEntry> p = new IgnitePredicate<GridCacheMapEntry>() {
            @Override public boolean apply(GridCacheMapEntry entry) {
                return entry.visitable(filter);
            }
        };

        return F.viewReadOnly(map.values(), F.<GridCacheMapEntry>identity(), p);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMapEntry> allEntries(final CacheEntryPredicate... filter) {
        final IgnitePredicate<GridCacheMapEntry> p = new IgnitePredicate<GridCacheMapEntry>() {
            @Override public boolean apply(GridCacheMapEntry entry) {
                return F.isAll(entry, filter);
            }
        };

        return F.viewReadOnly(map.values(), F.<GridCacheMapEntry>identity(), p);
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheMapEntry> entrySet(final CacheEntryPredicate... filter) {
        final IgnitePredicate<GridCacheMapEntry> p = new IgnitePredicate<GridCacheMapEntry>() {
            @Override public boolean apply(GridCacheMapEntry entry) {
                return entry.visitable(filter);
            }
        };

        return new AbstractSet<GridCacheMapEntry>() {
            @Override public Iterator<GridCacheMapEntry> iterator() {
                return F.iterator0(map.values(), true, p);
            }

            @Override public int size() {
                return F.size(iterator());
            }

            @Override public boolean contains(Object o) {
                if (!(o instanceof GridCacheMapEntry))
                    return false;

                GridCacheMapEntry entry = (GridCacheMapEntry)o;

                return entry.equals(map.get(entry.key())) && p.apply(entry);
            }
        };
    }
}
