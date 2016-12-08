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
     * Map for keeping previous values to make sure all the caches work correctly.
     */
    private static Map<String, Long> cntrs = new HashMap<>();

    /**
     * Executor for printing log in parallel thread.
     */
    private static ScheduledExecutorService exec;

    /**
     * Benchmark configuration.
     */
    private static BenchmarkConfiguration cfg;

    /**
     * String template used in String.format() to make output readable.
     */
    private static String strFmt;

    /**
     * List of caches whose size to be printed during preload.
     */
    private static Collection<IgniteCache<Object, Object>> caches;

    /**
     * Utility class constructor.
     */
    private IgniteBenchmarkPreloadLogger() {
       //No-op.
    }

    /**
     * Prints non-system caches sizes during preloading.
     * @param node Ignite node.
     * @param config Benchmark configuration.
     * @param args Benchmark arguments.
     */
    public static void printLog(IgniteNode node, BenchmarkConfiguration config, IgniteBenchmarkArguments args){
        cfg = config;
        exec = Executors.newSingleThreadScheduledExecutor();
        setCaches(node);
        setCounters();
        exec.scheduleWithFixedDelay(lgr, 0L, args.preloadLogsInterval(), TimeUnit.MILLISECONDS);
        BenchmarkUtils.println(cfg, "Preloading started.");
    }

    /**
     * Terminates printing log.
     * @throws Exception if failed.
     */
    public static void stopPrint() throws Exception {
        exec.execute(lgr);
        exec.awaitTermination(3, TimeUnit.SECONDS);
        exec.shutdownNow();
        BenchmarkUtils.println(cfg, "Preloading finished.");
    }

    /**
     * Helper method for initializing the cache list.
     * @param node Ignite node.
     */
    private static void setCaches(IgniteNode node) {
        caches = new ArrayList<>();
        for (String cacheName : node.ignite().cacheNames()) {
            IgniteCache<Object, Object> cache = node.ignite().cache(cacheName);
            caches.add(cache);
        }
    }

    /**
     * Helper method for initializing the counters map.
     */
    private static void setCounters() {
        int longestName = 0;

        // Set up an initial values to the map
        for (IgniteCache<Object, Object> availableCache : caches) {
            cntrs.put(availableCache.getName(), 0L);

            //Find out the length of the longest cache name
            longestName = Math.max(availableCache.getName().length(), longestName);
        }

        // Should look like "Preloading:%-20s%-8d\t(%s)"
        strFmt = "Preloading:%-" + (longestName + 4) + "s%-8d\t(%s)";
    }

    /**
     * Runnable instance for printing log.
     */
    private static final Runnable lgr = new Runnable() {
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
}