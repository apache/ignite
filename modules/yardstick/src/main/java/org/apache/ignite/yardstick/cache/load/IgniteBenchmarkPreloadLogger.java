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

package org.apache.ignite.yardstick.cache.load;

import org.apache.ignite.IgniteCache;
import org.yardstickframework.BenchmarkUtils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Prints a log of preloading process to the BenchmarkUtils output
 */
public class IgniteBenchmarkPreloadLogger implements Runnable {
    /**
     * Map for keeping previous values to make sure all the caches work correctly.
     */
    private final Map<String, Long> counters;

    /**
     * String template used in String.format() to make output readable.
     */
    private final String fmtstr;

    /**
     * List of caches whose size to be printed during preload
     */
    private final Collection<IgniteCache<Object, Object>> caches;

    /**
     * Creates new thread which prints a number of an entries in a cache and a number of an entries loaded
     * during each time interval.
     * @param caches List of available caches
     */
    public IgniteBenchmarkPreloadLogger(final Collection<IgniteCache<Object, Object>> caches) {
        this.caches = caches;

        counters = new HashMap<>();

        int longestNameLgh = 0;

        // Set up an initial values to the map
        for (IgniteCache<Object, Object> availableCache : caches) {
            counters.put(availableCache.getName(), 0L);

            //Find out the length of the longest cache name
            longestNameLgh = Math.max(availableCache.getName().length(), longestNameLgh);
        }

        fmtstr = "Preloading log:%-" + (longestNameLgh + 4) + "s%-8d\t(+%d)";
    }

    /** {@inheritDoc} */
    @Override public void run() {
        for (IgniteCache<Object, Object> cache : caches) {
            String cacheName = cache.getName();

            long cacheSize = cache.sizeLong();

            long recentlyLoaded = cacheSize - counters.get(cacheName);

            String log = String.format(fmtstr, cacheName, cacheSize, recentlyLoaded);
            BenchmarkUtils.println(log);

            counters.put(cacheName, cacheSize);
        }
    }
}

