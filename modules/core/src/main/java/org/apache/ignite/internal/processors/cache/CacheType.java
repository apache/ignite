/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;

/**
 *
 */
public enum CacheType {
    /**
     * Regular cache created by user, visible via public API (e.g. {@link org.apache.ignite.Ignite#cache(String)}).
     */
    USER(true, SYSTEM_POOL),

    /**
     * Internal cache, should not be visible via public API.
     */
    INTERNAL(false, SYSTEM_POOL),

    /**
     * Cache for data structures, should not be visible via public API.
     */
    DATA_STRUCTURES(false, SYSTEM_POOL),

    /**
     * Internal replicated cache, should use separate thread pool.
     */
    UTILITY(false, UTILITY_CACHE_POOL);

    /** */
    private final boolean userCache;

    /** */
    private final byte ioPlc;

    /**
     * @param userCache {@code True} if cache created by user.
     * @param ioPlc Cache IO policy.
     */
    CacheType(boolean userCache, byte ioPlc) {
        this.userCache = userCache;
        this.ioPlc = ioPlc;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache type.
     */
    public static CacheType cacheType(String cacheName) {
        if (CU.isUtilityCache(cacheName))
            return UTILITY;
        else if (DataStructuresProcessor.isDataStructureCache(cacheName))
            return DATA_STRUCTURES;
        else
            return USER;
    }

    /**
     * @return Cache IO policy.
     */
    public byte ioPolicy() {
        return ioPlc;
    }

    /**
     * @return {@code True} if cache created by user.
     */
    public boolean userCache() {
        return userCache;
    }
}
