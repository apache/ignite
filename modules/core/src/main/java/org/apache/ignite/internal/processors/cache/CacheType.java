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

package org.apache.ignite.internal.processors.cache;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MARSH_CACHE_POOL;
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
     * Internal cache, should not be visible via public API (caches used by IGFS, Hadoop, data structures).
     */
    INTERNAL(false, SYSTEM_POOL),

    /**
     * Internal replicated cache, should use separate thread pool.
     */
    UTILITY(false, UTILITY_CACHE_POOL),

    /**
     * Internal marshaller cache, should use separate thread pool.
     */
    MARSHALLER(false, MARSH_CACHE_POOL);

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
