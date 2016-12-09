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

import org.apache.ignite.IgniteCache;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Prints a log of preloading process to the BenchmarkUtils output
 */
public class IgniteBenchmarkPreloadLogger {
    /**
     * List of caches whose size to be printed during preload.
     */
    private final Collection<IgniteCache<Object, Object>> caches;

    /**
     * Map for keeping previous values to make sure all the caches are working correctly.
     */
    private final Map<String, Long> cntrs;

    /**
     * Benchmark configuration.
     */
    private final BenchmarkConfiguration cfg;

    /**
     * Runnable instance for printing log.
     */
    private final Runnable lgr;

    /**
     * String template used in String.format() to make output readable.
     */
    private String strFmt;

    /**
     * Executor for printing log in parallel thread.
     */
    private static ScheduledExecutorService exec;

    /**
     * Creates an instance and sets fields.
     * @param node Ignite node.
     * @param cnfg Benchmark configuration.
     */
    public IgniteBenchmarkPreloadLogger(IgniteNode node, BenchmarkConfiguration cnfg) {
        this.cfg = cnfg;
        this.caches = new ArrayList<>();
        this.cntrs = new HashMap<>();
        this.lgr = new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                for (IgniteCache<Object, Object> cache : caches) {
                    String cacheName = cache.getName();

                    long cacheSize = cache.sizeLong();

                    long recentlyLoaded = cacheSize - cntrs.get(cacheName);
                    String recLoaded = recentlyLoaded == 0 ?  "" + recentlyLoaded : "+" + recentlyLoaded;

                    BenchmarkUtils.println(cfg, String.format(strFmt, cacheName, cacheSize, recLoaded));

                    cntrs.put(cacheName, cacheSize);
                }
            }
        };
        exec = Executors.newSingleThreadScheduledExecutor();
        setCaches(node);
    }

    /**
     * Prints non-system cache sizes during preloading.
     * @param logsInterval Time interval in milliseconds between printing logs.
     */
    public void printLog(long logsInterval){
        exec.scheduleWithFixedDelay(lgr, 0L, logsInterval, TimeUnit.MILLISECONDS);
        BenchmarkUtils.println(cfg, "Preloading started.");
    }

    /**
     * Terminates printing log.
     * @throws Exception if failed.
     */
    public void stopPrint() throws Exception {
        exec.execute(lgr);
        exec.awaitTermination(1, TimeUnit.SECONDS);
        exec.shutdownNow();
        BenchmarkUtils.println(cfg, "Preloading finished.");
    }

    /**
     * Helper method for initializing the cache list and the counters map.
     * @param node Ignite node.
     */
    private void setCaches(IgniteNode node) {
        int longestName = 0;
        for (String cacheName : node.ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = node.ignite().cache(cacheName);
            caches.add(cache);

            // Set up an initial values to the map
            cntrs.put(cache.getName(), 0L);

            //Find out the length of the longest cache name
            longestName = Math.max(cache.getName().length(), longestName);
        }

        // Should look like "Preloading:%-20s%-8d\t(%s)"
        strFmt = "Preloading:%-" + (longestName + 4) + "s%-8d\t(%s)";
    }
}