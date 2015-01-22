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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Base runnable for {@link GridCacheAdapter#clearAll()} routine.
 */
public class GridCacheClearAllRunnable<K, V> implements Runnable {
    /** Cache to be cleared. */
    protected final GridCacheAdapter<K, V> cache;

    /**  Obsolete version. */
    protected final GridCacheVersion obsoleteVer;

    /** Mod for the given runnable. */
    protected final int id;

    /** Mods count across all spawned clearAll runnables. */
    protected final int totalCnt;

    /** Cache context. */
    protected final GridCacheContext<K, V> ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param cache Cache to be cleared.
     * @param obsoleteVer Obsolete version.
     * @param id Mod for the given runnable.
     * @param totalCnt Mods count across all spawned clearAll runnables.
     */
    public GridCacheClearAllRunnable(GridCacheAdapter<K, V> cache, GridCacheVersion obsoleteVer, int id, int totalCnt) {
        assert cache != null;
        assert obsoleteVer != null;
        assert id >= 0;
        assert totalCnt > 0;
        assert id < totalCnt;

        this.cache = cache;
        this.obsoleteVer = obsoleteVer;
        this.id = id;
        this.totalCnt = totalCnt;

        ctx = cache.context();
        log = ctx.gridConfig().getGridLogger().getLogger(getClass());
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Iterator<GridCacheEntryEx<K, V>> iter = cache.map().stripedEntryIterator(id, totalCnt);

        while (iter.hasNext())
            clearEntry(iter.next());

        // Clear swapped entries.
        if (!ctx.isNear()) {
            if (ctx.swap().offHeapEnabled()) {
                if (ctx.config().isQueryIndexEnabled()) {
                    for (Iterator<Map.Entry<K, V>> it = ctx.swap().lazyOffHeapIterator(); it.hasNext();) {
                        Map.Entry<K, V> e = it.next();

                        if (owns(e.getKey()))
                            clearEntry(cache.entryEx(e.getKey()));

                    }
                }
                else if (id == 0)
                    ctx.swap().clearOffHeap();
            }

            if (ctx.isSwapOrOffheapEnabled()) {
                if (ctx.swap().swapEnabled()) {
                    if (ctx.config().isQueryIndexEnabled()) {
                        Iterator<Map.Entry<K, V>> it = null;

                        try {
                            it = ctx.swap().lazySwapIterator();
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to get iterator over swap.", e);
                        }

                        if (it != null) {
                            while (it.hasNext()) {
                                Map.Entry<K, V> e = it.next();

                                if (owns(e.getKey()))
                                    clearEntry(cache.entryEx(e.getKey()));
                            }
                        }
                    }
                    else if (id == 0) {
                        try {
                            ctx.swap().clearSwap();
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to clear entries from swap storage.", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Clear entry from cache.
     *
     * @param e Entry.
     */
    protected void clearEntry(GridCacheEntryEx<K, V> e) {
        try {
            e.clear(obsoleteVer, false, CU.<K, V>empty());
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to clear entry from cache (will continue to clear other entries): " + e, ex);
        }
    }

    /**
     * Check whether this worker owns particular key.
     *
     * @param key Key.
     * @return {@code True} in case this worker should process this key.
     */
    protected boolean owns(K key) {
        assert key != null;

        // Avoid hash code and remainder calculation in case ther is no actual split.
        return totalCnt == 1 || key.hashCode() % totalCnt == id;
    }

    /**
     * @return ID for the given runnable.
     */
    public int id() {
        return id;
    }

    /**
     * @return Total count across all spawned clearAll runnables.
     */
    public int totalCount() {
        return totalCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheClearAllRunnable.class, this);
    }
}
