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
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_DESTROYED;

/**
 * Implementation of concurrent cache map.
 */
public abstract class GridCacheConcurrentMapImpl implements GridCacheConcurrentMap {
    /** Map entry factory. */
    private final GridCacheMapEntryFactory factory;

    /**
     * Creates a new, empty map with the specified initial
     * capacity.
     *
     * @param factory Entry factory.

     * @throws IllegalArgumentException if the initial capacity is
     *      negative.
     */
    public GridCacheConcurrentMapImpl(GridCacheMapEntryFactory factory) {
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry getEntry(GridCacheContext ctx, KeyCacheObject key) {
        CacheMapHolder hld = entriesMapIfExists(ctx.cacheIdBoxed());

        key = (KeyCacheObject)ctx.kernalContext().cacheObjects().prepareForCache(key, ctx);

        return hld != null ? hld.map.get(key) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheMapEntry putEntryIfObsoleteOrAbsent(
        GridCacheContext ctx,
        final AffinityTopologyVersion topVer,
        KeyCacheObject key,
        final boolean create,
        final boolean touch) {
        return putEntryIfObsoleteOrAbsent(null, ctx, topVer, key, create, touch);
    }

    protected final GridCacheMapEntry putEntryIfObsoleteOrAbsent(
        @Nullable CacheMapHolder hld,
        GridCacheContext ctx,
        final AffinityTopologyVersion topVer,
        KeyCacheObject key,
        final boolean create,
        final boolean touch) {
        if (hld == null)
            hld = entriesMapIfExists(ctx.cacheIdBoxed());

        GridCacheMapEntry cur = null;
        GridCacheMapEntry created = null;
        GridCacheMapEntry created0 = null;
        GridCacheMapEntry doomed = null;

        boolean done = false;
        boolean reserved = false;
        int sizeChange = 0;

        try {
            while (!done) {
                GridCacheMapEntry entry = hld != null ? hld.map.get(ctx.kernalContext().cacheObjects().prepareForCache(key, ctx)) : null;
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

                            if (hld == null) {
                                hld = entriesMap(ctx);

                                assert hld != null;
                            }

                            created0 = factory.create(ctx, topVer, key);
                        }

                        cur = created = created0;

                        done = hld.map.putIfAbsent(created.key(), created) == null;
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

                            done = hld.map.replace(entry.key(), doomed, created);
                        }
                        else
                            done = hld.map.remove(entry.key(), doomed);
                    }
                    else {
                        cur = entry;

                        done = true;
                    }
                }
            }

            sizeChange = 0;

            if (doomed != null) {
                doomed.lockEntry();

                try {
                    if (!doomed.deleted())
                        sizeChange--;
                }
                finally {
                    doomed.unlockEntry();
                }

                if (ctx.events().isRecordable(EVT_CACHE_ENTRY_DESTROYED))
                    ctx.events().addEvent(doomed.partition(),
                        doomed.key(),
                        ctx.localNodeId(),
                        null,
                        null,
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
                        null,
                        null,
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
                    cur.touch();
            }

            assert Math.abs(sizeChange) <= 1;

            return cur;
        }
        finally {
            if (reserved)
                release(sizeChange, hld, cur);
            else {
                if (sizeChange != 0) {
                    assert sizeChange == -1;
                    assert doomed != null;

                    decrementPublicSize(hld, doomed);
                }
            }
        }
    }

    /**
     * @param cctx Cache context.
     * @return Map for given cache ID.
     */
    @Nullable protected abstract CacheMapHolder entriesMap(GridCacheContext cctx);

    /**
     * @param cacheId Cache ID.
     * @return Map for given cache ID.
     */
    @Nullable protected abstract CacheMapHolder entriesMapIfExists(Integer cacheId);

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
     * @param hld Map holder.
     * @param e Map entry.
     */
    protected void release(int sizeChange, CacheMapHolder hld, GridCacheEntryEx e) {
        if (sizeChange == 1)
            incrementPublicSize(hld, e);
        else if (sizeChange == -1)
            decrementPublicSize(hld, e);
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(final GridCacheEntryEx entry) {
        GridCacheContext ctx = entry.context();

        CacheMapHolder hld = entriesMapIfExists(ctx.cacheIdBoxed());

        boolean rmv = hld != null && hld.map.remove(entry.key(), entry);

        if (rmv) {
            if (ctx.events().isRecordable(EVT_CACHE_ENTRY_DESTROYED)) {
                // Event notification.
                ctx.events().addEvent(entry.partition(),
                    entry.key(),
                    ctx.localNodeId(),
                    null,
                    null,
                    null,
                    EVT_CACHE_ENTRY_DESTROYED,
                    null,
                    false,
                    null,
                    false,
                    null,
                    null,
                    null,
                    false);
            }

            entry.lockEntry();

            try {
                if (!entry.deleted())
                    decrementPublicSize(hld, entry);
            }
            finally {
                entry.unlockEntry();
            }
        }

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMapEntry> entries(int cacheId, final CacheEntryPredicate... filter) {
        CacheMapHolder hld = entriesMapIfExists(cacheId);

        if (hld == null)
            return Collections.emptyList();

        final IgnitePredicate<GridCacheMapEntry> p = new IgnitePredicate<GridCacheMapEntry>() {
            @Override public boolean apply(GridCacheMapEntry entry) {
                return entry.visitable(filter);
            }
        };

        return F.viewReadOnly(hld.map.values(), F.<GridCacheMapEntry>identity(), p);
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheMapEntry> entrySet(int cacheId, final CacheEntryPredicate... filter) {
        final CacheMapHolder hld = entriesMapIfExists(cacheId);

        if (hld == null)
            return Collections.emptySet();

        final IgnitePredicate<GridCacheMapEntry> p = new IgnitePredicate<GridCacheMapEntry>() {
            @Override public boolean apply(GridCacheMapEntry entry) {
                return entry.visitable(filter);
            }
        };

        return new AbstractSet<GridCacheMapEntry>() {
            @Override public Iterator<GridCacheMapEntry> iterator() {
                return F.iterator0(hld.map.values(), true, p);
            }

            @Override public int size() {
                return F.size(iterator());
            }

            @Override public boolean contains(Object o) {
                if (!(o instanceof GridCacheMapEntry))
                    return false;

                GridCacheMapEntry entry = (GridCacheMapEntry)o;

                return entry.equals(hld.map.get(entry.key())) && p.apply(entry);
            }
        };
    }
}
