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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridSerializableSet;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Implementation of concurrent cache map.
 */
public class GridCacheConcurrentMapImpl implements GridCacheConcurrentMap {

    /** Default load factor. */
    private static final float DFLT_LOAD_FACTOR = 0.75f;

    /** Default concurrency level. */
    private static final int DFLT_CONCUR_LEVEL = Runtime.getRuntime().availableProcessors() * 2;

    /** Internal map. */
    private final ConcurrentHashMap8<KeyCacheObject, GridCacheMapEntry> map;

    /** Map entry factory. */
    private final GridCacheMapEntryFactory factory;

    /** Cache context. */
    private final GridCacheContext ctx;

    private final AtomicInteger pubSize = new AtomicInteger();

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
    public GridCacheConcurrentMapImpl(GridCacheContext ctx, GridCacheMapEntryFactory factory, int initialCapacity,
        float loadFactor, int concurrencyLevel) {
        this.ctx = ctx;
        this.factory = factory;
        map = new ConcurrentHashMap8<>(initialCapacity, loadFactor, concurrencyLevel);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry getEntry(Object key) {
        return map.get(ctx.cacheObjects().toCacheKeyObject(ctx.cacheObjectContext(), key, true));
    }

    /** {@inheritDoc} */
    @Override public GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(final AffinityTopologyVersion topVer,
        final KeyCacheObject key,
        @Nullable final CacheObject val, final boolean create) {

        final AtomicReference<GridCacheMapEntry> created = new AtomicReference<>();
        final AtomicReference<GridCacheMapEntry> doomed = new AtomicReference<>();

        GridCacheMapEntry cur = map.compute(key, new ConcurrentHashMap8.BiFun<KeyCacheObject, GridCacheMapEntry, GridCacheMapEntry>() {
            @Override public GridCacheMapEntry apply(KeyCacheObject object, GridCacheMapEntry entry) {
                GridCacheMapEntry cur = null;
                GridCacheMapEntry created0 = null;
                GridCacheMapEntry doomed0 = null;

                if (entry == null) {
                    if (create)
                        entry = cur = created0 = factory.create(ctx, topVer, key, key.hashCode(), val);
                }
                else {
                    if (entry.obsolete()) {
                        doomed0 = entry;

                        if (create)
                            cur = created0 = factory.create(ctx, topVer, key, key.hashCode(), val);
                    }
                    else
                        cur = entry;
                }

                if (entry != null)
                    synchronized (entry) {
                        if (created0 != null && doomed0 == null)
                            incrementPublicSize(entry);
                        else if (doomed0 != null && created0 == null)
                            decrementPublicSize(entry);
                    }

                created.set(created0);
                doomed.set(doomed0);
                return cur;
            }
        });

        return new GridTriple<>(cur, created.get(), doomed.get());
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(final GridCacheEntryEx entry) {
        final AtomicBoolean result = new AtomicBoolean();

        map.computeIfPresent(entry.key(), new ConcurrentHashMap8.BiFun<KeyCacheObject, GridCacheMapEntry, GridCacheMapEntry>() {
            @Override public GridCacheMapEntry apply(KeyCacheObject object, GridCacheMapEntry entry0) {
                if (entry.equals(entry0)) {
                    result.set(true);

                    synchronized (entry) {
                        if (!entry.deleted())
                            decrementPublicSize(entry);
                    }

                    return null;
                }
                return entry0;
            }
        });

        return result.get();
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key) {
        final AtomicReference<GridCacheMapEntry> result = new AtomicReference<>();

        map.computeIfPresent(key, new ConcurrentHashMap8.BiFun<KeyCacheObject, GridCacheMapEntry, GridCacheMapEntry>() {
            @Override public GridCacheMapEntry apply(KeyCacheObject object, GridCacheMapEntry entry) {
                if (!entry.obsolete())
                    return entry;

                result.set(entry);

                synchronized (entry) {
                    if (!entry.deleted())
                        decrementPublicSize(entry);
                }

                return null;
            }
        });

        return result.get();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    @Override public int publicSize() {
        return pubSize.get();
    }

    @Override public void incrementPublicSize(GridCacheEntryEx e) {
        pubSize.incrementAndGet();
    }

    @Override public void decrementPublicSize(GridCacheEntryEx e) {
        pubSize.decrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public Set keySet(final CacheEntryPredicate... filter) {
        final IgnitePredicate<KeyCacheObject> p = new IgnitePredicate<KeyCacheObject>() {
            @Override public boolean apply(KeyCacheObject key) {
                return F.isAll(map.get(key), filter);
            }
        };

        return new GridSerializableSet<KeyCacheObject>() {
            @Override public Iterator<KeyCacheObject> iterator() {
                return F.iterator0(map.keySet(), true, p);
            }

            @Override public int size() {
                return map.size();
            }

            @Override public boolean contains(Object o) {
                if (!(o instanceof KeyCacheObject))
                    return false;

                return map.keySet().contains(o) && p.apply((KeyCacheObject)o);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheEntryEx> entries(CacheEntryPredicate... filter) {
        return F.viewReadOnly(map.values(), F.<GridCacheEntryEx>identity(), filter);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntryEx randomEntry() {
        // TODO
        return map.values().iterator().next();
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntryEx> entrySet(final CacheEntryPredicate... filter) {
        return new AbstractSet<GridCacheEntryEx>() {
            @Override public Iterator<GridCacheEntryEx> iterator() {
                return F.<GridCacheEntryEx>iterator0(map.values(), true, filter);
            }

            @Override public int size() {
                return map.size();
            }

            @Override public boolean contains(Object o) {
                return o instanceof GridCacheEntryEx && map.containsKey(((GridCacheEntryEx)o).key());

            }
        };
    }
}
