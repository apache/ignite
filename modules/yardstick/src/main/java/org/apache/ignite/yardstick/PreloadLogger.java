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

package org.apache.ignite.yardstick;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Prints non-system caches size.
 */
public class PreloadLogger implements Runnable {
    /** Benchmark configuration. */
    private BenchmarkConfiguration cfg;

    /** List of caches whose size to be printed during preload. */
    private Collection<IgniteCache<Object, Object>> caches;

    /** Map for keeping previous values to make sure all the caches are working correctly. */
    private Map<String, Long> cntrs;

    /** String template used in String.format() to make output readable. */
    private String strFmt;

    /** Future instance to stop print log. */
    private ScheduledFuture<?> fut;

    /**
     * @param node Ignite node.
     * @param cfg BenchmarkConfiguration.
     */
    public PreloadLogger(IgniteNode node, BenchmarkConfiguration cfg) {
        this.caches = new ArrayList<>();
        this.cntrs = new HashMap<>();
        this.cfg = cfg;

        init(node);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        printCachesStatistics();
    }

    /**
     * Prints non-system cache sizes.
     */
    public synchronized void printCachesStatistics() {
        for (IgniteCache<Object, Object> cache : caches) {
            try {
                printCacheStatistics(cache);
            }
            catch (Exception e) {
                BenchmarkUtils.println(cfg, "Failed to print cache size [cache=" + cache.getName()
                    + ", msg=" + e.getMessage() + "]");
            }
        }
    }

    /**
     * Print cache size along with amount of recently loaded entries.
     *
     * @param cache Ignite cache.
     */
    private void printCacheStatistics(IgniteCache<Object, Object> cache) {
        String cacheName = cache.getName();

        long cacheSize = cache.sizeLong();

        long recentlyLoaded = cacheSize - cntrs.get(cacheName);
        String recLoaded = recentlyLoaded == 0 ? String.valueOf(recentlyLoaded) : "+" + recentlyLoaded;

        BenchmarkUtils.println(cfg, String.format(strFmt, cacheName, cacheSize, recLoaded));

        cntrs.put(cacheName, cacheSize);
    }

    /**
     * Helper method for initializing the cache list and the counters map.
     *
     * @param node Ignite node.
     */
    private void init(IgniteNode node) {
        int longestName = 0;

        for (String cacheName : node.ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = node.ignite().cache(cacheName);

            caches.add(cache);

            // Set up an initial values to the map.
            cntrs.put(cache.getName(), 0L);

            // Find out the length of the longest cache name.
            longestName = Math.max(cache.getName().length(), longestName);
        }

        // Should look like "Preload:%-20s%-8d\t(%s)"
        strFmt = "Preload:%-" + (longestName + 4) + "s%-8d\t(%s)";
    }

    /**
     * Set future.
     */
    public void setFuture(ScheduledFuture<?> fut) {
        this.fut = fut;
    }

    /**
     * Terminates printing log.
     */
    public void stopAndPrintStatistics() {
        try {
            if (fut != null) {
                if (!fut.cancel(true)) {
                    U.sleep(200);

                    if (!fut.cancel(true)) {
                        BenchmarkUtils.println(cfg, "Failed to cancel Preload logger.");

                        return;
                    }
                }
            }

            printCachesStatistics();
        }
        catch (Exception e) {
            BenchmarkUtils.error("Failed to stop Preload logger.", e);
        }

        BenchmarkUtils.println(cfg, "Preload logger was stopped.");
    }
}
