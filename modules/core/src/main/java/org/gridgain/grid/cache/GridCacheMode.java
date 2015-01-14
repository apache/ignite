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

package org.gridgain.grid.cache;

import org.gridgain.grid.cache.affinity.*;
import org.jetbrains.annotations.*;

/**
 * Enumeration of all supported caching modes. Cache mode is specified in {@link GridCacheConfiguration}
 * and cannot be changed after cache has started.
 */
public enum GridCacheMode {
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
     * over subset of nodes for any given key via {@link GridCacheAffinityFunction}
     * configuration.
     */
    REPLICATED,

    /**
     * Specifies partitioned cache behaviour. In this mode the overall
     * key set will be divided into partitions and all partitions will be split
     * equally between participating nodes. User has affinity
     * control over key assignment via {@link GridCacheAffinityFunction}
     * configuration.
     * <p>
     * Note that partitioned cache is always fronted by local
     * {@code 'near'} cache which stores most recent data. You
     * can configure the size of near cache via {@link GridCacheConfiguration#getNearEvictionPolicy()}
     * configuration property.
     */
    PARTITIONED;

    /** Enumerated values. */
    private static final GridCacheMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCacheMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
