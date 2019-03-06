/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.jetbrains.annotations.Nullable;

/**
 * H2 row cache registry.
 */
public class H2RowCacheRegistry {
    /** Mutex. */
    private final Object mux = new Object();

    /** Row caches for specific cache groups. */
    private volatile Map<Integer, H2RowCache> caches;

    /**
     * Get row cache for the given cache group.
     *
     * @param grpId Cache group ID.
     * @return Row cache or {@code null} if none available.
     */
    @Nullable public H2RowCache forGroup(int grpId) {
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
                H2RowCache cache = caches.get(grpId);

                if (cache != null) {
                    cache.onCacheRegistered();

                    return;
                }
            }

            HashMap<Integer, H2RowCache> caches0 = copy();

            if (cacheInfo.affinityNode()) {
                GridCacheContext cacheCtx = cacheInfo.cacheContext();

                assert cacheCtx != null;

                H2RowCache rowCache = new H2RowCache(cacheCtx.group(), cacheInfo.config().getSqlOnheapCacheMaxSize());

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

            H2RowCache cache = caches.get(grpId);

            assert cache != null;

            if (cache.onCacheUnregistered(cacheInfo)) {
                HashMap<Integer, H2RowCache> caches0 = copy();

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
    private HashMap<Integer, H2RowCache> copy() {
        assert Thread.holdsLock(mux);

        if (caches == null)
            return new HashMap<>();
        else
            return new HashMap<>(caches);
    }
}
