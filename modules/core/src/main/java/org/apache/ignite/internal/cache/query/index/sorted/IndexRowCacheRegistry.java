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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.jetbrains.annotations.Nullable;

/**
 * Index row cache registry.
 */
public class IndexRowCacheRegistry {
    /** Mutex. */
    private final Object mux = new Object();

    /** Row caches for specific cache groups. */
    private volatile Map<Integer, IndexRowCache> caches;

    /**
     * Get row cache for the given cache group.
     *
     * @param grpId Cache group ID.
     * @return Row cache or {@code null} if none available.
     */
    @Nullable public IndexRowCache forGroup(int grpId) {
        return caches != null ? caches.get(grpId) : null;
    }

    /**
     * Callback invoked on cache registration within indexing.
     *
     * @param cacheInfo Cache info context.
     */
    public void onCacheRegistered(GridCacheContextInfo cacheInfo) {
        if (!cacheInfo.config().isSqlOnheapCacheEnabled())
            return;

        synchronized (mux) {
            int grpId = cacheInfo.groupId();

            if (caches != null) {
                IndexRowCache cache = caches.get(grpId);

                if (cache != null) {
                    cache.onCacheRegistered();

                    return;
                }
            }

            HashMap<Integer, IndexRowCache> caches0 = copy();

            if (cacheInfo.affinityNode()) {
                GridCacheContext cacheCtx = cacheInfo.cacheContext();

                assert cacheCtx != null;

                IndexRowCache rowCache = new IndexRowCache(cacheCtx.group(), cacheInfo.config().getSqlOnheapCacheMaxSize());

                caches0.put(grpId, rowCache);

                caches = caches0;

                // Inject row cache cleaner into store on cache creation.
                // Used in case the cache with enabled SqlOnheapCache is created in exists cache group
                // and SqlOnheapCache is disbaled for the caches have been created before.
                for (IgniteCacheOffheapManager.CacheDataStore ds : cacheCtx.offheap().cacheDataStores())
                    ds.setRowCacheCleaner(rowCache);
            }
        }
    }

    /**
     * Callback invoked when cache gets unregistered.
     *
     * @param cacheInfo Cache context info.
     */
    public void onCacheUnregistered(GridCacheContextInfo cacheInfo) {
        if (!cacheInfo.config().isSqlOnheapCacheEnabled())
            return;

        synchronized (mux) {
            int grpId = cacheInfo.groupId();

            assert caches != null;

            IndexRowCache cache = caches.get(grpId);

            assert cache != null;

            if (cache.onCacheUnregistered(cacheInfo)) {
                HashMap<Integer, IndexRowCache> caches0 = copy();

                caches0.remove(grpId);

                caches = caches0;
            }
        }
    }

    /**
     * Create copy of caches map under lock.
     *
     * @return Copy.
     */
    private HashMap<Integer, IndexRowCache> copy() {
        assert Thread.holdsLock(mux);

        if (caches == null)
            return new HashMap<>();
        else
            return new HashMap<>(caches);
    }
}
