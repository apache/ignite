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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Set;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

import static org.apache.ignite.IgniteCommonsSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.DFLT_CACHED_STRINGS_THRESHOLD;

/** Caches strings for performance statistics writing. */
class StringCache {
    /** Maximum cached strings threshold. String caching will stop on threshold excess. */
    private final int cachedStrsThreshold = getInteger(IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, DFLT_CACHED_STRINGS_THRESHOLD);

    /** Hashcodes of cached strings. */
    private final Set<Integer> knownStrs = new GridConcurrentHashSet<>();

    /** @return {@code True} if string was cached and can be written as hashcode. */
    public boolean cacheIfPossible(String str) {
        if (knownStrs.size() >= cachedStrsThreshold)
            return false;

        return !knownStrs.add(str.hashCode());
    }

    /** Clear cache. */
    public void clear() {
        knownStrs.clear();
    }
}
