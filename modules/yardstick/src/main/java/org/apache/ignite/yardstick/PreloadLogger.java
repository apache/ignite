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
import org.apache.ignite.IgniteCache;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

public class PreloadLogger implements Runnable {
    /** */
    private BenchmarkConfiguration cfg;

    /** List of caches whose size to be printed during preload. */
    private Collection<IgniteCache<Object, Object>> caches;

    /**
     * Map for keeping previous values to make sure all the caches are working correctly.
     */
    private Map<String, Long> cntrs;

    /**
     * String template used in String.format() to make output readable.
     */
    private String strFmt;

    /**
     * String template used in String.format() to make output readable.
     */

    PreloadLogger(IgniteNode node, BenchmarkConfiguration cfg){
        this.caches = new ArrayList<>();
        this.cntrs = new HashMap<>();
        this.cfg = cfg;

        init(node);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        printCacheStatistics();
    }

    public synchronized void printCacheStatistics() {
        for (IgniteCache<Object, Object> cache : caches) {
            String cacheName = cache.getName();

            long cacheSize = cache.sizeLong();

            long recentlyLoaded = cacheSize - cntrs.get(cacheName);
            String recLoaded = recentlyLoaded == 0 ?  "" + recentlyLoaded : "+" + recentlyLoaded;

            BenchmarkUtils.println(cfg, String.format(strFmt, cacheName, cacheSize, recLoaded));

            cntrs.put(cacheName, cacheSize);
        }
    }

    /**
     * Helper method for initializing the cache list and the counters map.
     * @param node Ignite node.
     */
    private void init(IgniteNode node) {
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
