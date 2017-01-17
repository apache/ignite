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

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Enumeration of all supported caching modes. Cache mode is specified in {@link org.apache.ignite.configuration.CacheConfiguration}
 * and cannot be changed after cache has started.
 */
public enum CacheMode {
    /**
     * Specifies local-only cache behaviour. In this mode caches residing on
     * different grid nodes will not know about each other.
     * <p>
     * Other than distribution, {@code local} caches still have all
     * the caching features, such as eviction, expiration, swapping,
     * querying, etc... This mode is very useful when caching read-only data
     * or data that automatically expires at a certain interval and
     * then automatically reloaded from persistence store.
     */
    LOCAL,

    /**
     * Specifies fully replicated cache behavior. In this mode all the keys are distributed
     * to all participating nodes. User still has affinity control
     * over subset of nodes for any given key via {@link AffinityFunction}
     * configuration.
     */
    REPLICATED,

    /**
     * Specifies partitioned cache behaviour. In this mode the overall
     * key set will be divided into partitions and all partitions will be split
     * equally between participating nodes. User has affinity
     * control over key assignment via {@link AffinityFunction}
     * configuration.
     * <p>
     * Note that partitioned cache is always fronted by local
     * {@code 'near'} cache which stores most recent data. You
     * can configure the size of near cache via {@link NearCacheConfiguration#getNearEvictionPolicy()}
     * configuration property.
     */
    PARTITIONED;

    /** Enumerated values. */
    private static final CacheMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}