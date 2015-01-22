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

package org.apache.ignite.cache;

import org.jetbrains.annotations.*;

/**
 * Cache preload mode. When preloading is enabled (i.e. has value other than {@link #NONE}), distributed caches
 * will attempt to preload all necessary values from other grid nodes. This enumeration is used to configure
 * preloading via {@link CacheConfiguration#getPreloadMode()} configuration property. If not configured
 * explicitly, then {@link CacheConfiguration#DFLT_PRELOAD_MODE} is used.
 * <p>
 * Replicated caches will try to load the full set of cache entries from other nodes (or as defined by
 * pluggable {@link org.apache.ignite.cache.affinity.GridCacheAffinityFunction}), while partitioned caches will only load the entries for which
 * current node is primary or back up.
 * <p>
 * Note that preload mode only makes sense for {@link GridCacheMode#REPLICATED} and {@link GridCacheMode#PARTITIONED}
 * caches. Caches with {@link GridCacheMode#LOCAL} mode are local by definition and therefore cannot preload
 * any values from neighboring nodes.
 */
public enum GridCachePreloadMode {
    /**
     * Synchronous preload mode. Distributed caches will not start until all necessary data
     * is loaded from other available grid nodes.
     */
    SYNC,

    /**
     * Asynchronous preload mode. Distributed caches will start immediately and will load all necessary
     * data from other available grid nodes in the background.
     */
    ASYNC,

    /**
     * In this mode no preloading will take place which means that caches will be either loaded on
     * demand from persistent store whenever data is accessed, or will be populated explicitly.
     */
    NONE;

    /** Enumerated values. */
    private static final GridCachePreloadMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCachePreloadMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
