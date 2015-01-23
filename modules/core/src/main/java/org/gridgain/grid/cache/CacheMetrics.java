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

import org.apache.ignite.*;

/**
 * Cache metrics used to obtain statistics on cache itself.
 * Use {@link IgniteCache#metrics()} to obtain metrics for a cache.
 */
public interface CacheMetrics {
    /**
     * Clears the statistics counters to 0 for the associated Cache.
     */
    void clear();

    /**
     * The number of get requests that were satisfied by the cache.
     *
     * @return the number of hits
     */
    long getCacheHits();

    /**
     * This is a measure of cache efficiency.
     *
     * @return the percentage of successful hits, as a decimal e.g 75.
     */
    float getCacheHitPercentage();

    /**
     * A miss is a get request that is not satisfied.
     *
     * @return the number of misses
     */
    long getCacheMisses();

    /**
     * Returns the percentage of cache accesses that did not find a requested entry
     * in the cache.
     *
     * @return the percentage of accesses that failed to find anything
     */
    float getCacheMissPercentage();

    /**
     * The total number of requests to the cache. This will be equal to the sum of
     * the hits and misses.
     *
     * @return the number of gets
     */
    long getCacheGets();

    /**
     * The total number of puts to the cache.
     *
     * @return the number of puts
     */
    long getCachePuts();

    /**
     * The total number of removals from the cache. This does not include evictions,
     * where the cache itself initiates the removal to make space.
     *
     * @return the number of removals
     */
    long getCacheRemovals();

    /**
     * The total number of evictions from the cache. An eviction is a removal
     * initiated by the cache itself to free up space. An eviction is not treated as
     * a removal and does not appear in the removal counts.
     *
     * @return the number of evictions
     */
    long getCacheEvictions();

    /**
     * The mean time to execute gets.
     *
     * @return the time in µs
     */
    float getAverageGetTime();

    /**
     * The mean time to execute puts.
     *
     * @return the time in µs
     */
    float getAveragePutTime();

    /**
     * The mean time to execute removes.
     *
     * @return the time in µs
     */
    float getAverageRemoveTime();


    /**
     * The mean time to execute tx commit.
     *
     * @return the time in µs
     */
    public float getAverageTxCommitTime();

    /**
     * The mean time to execute tx rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public float getAverageTxRollbackTime();


    /**
     * Gets total number of transaction commits.
     *
     * @return Number of transaction commits.
     */
    public long getCacheTxCommits();

    /**
     * Gets total number of transaction rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public long getCacheTxRollbacks();
}
