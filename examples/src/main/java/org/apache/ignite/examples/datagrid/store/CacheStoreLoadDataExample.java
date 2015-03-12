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

package org.apache.ignite.examples.datagrid.store;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;

/**
 * Loads data from persistent store at cache startup by calling
 * {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method on
 * all nodes.
 * <p>
 * Remote nodes should always be started using {@link CacheNodeWithStoreStartup}.
 * Also you can change type of underlying store modifying configuration in the
 * {@link CacheNodeWithStoreStartup#configure()} method.
 */
public class CacheStoreLoadDataExample {
    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 1024 * 1024 * 1024;

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 1000000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Ignite ignite = Ignition.start(CacheNodeWithStoreStartup.configure())) {
            System.out.println();
            System.out.println(">>> Cache store load data example started.");

            final IgniteCache<String, Integer> cache = ignite.jcache(null);

            // Clean up caches on all nodes before run.
            cache.clear();

            long start = System.currentTimeMillis();

            // Start loading cache on all caching nodes.
            ignite.compute(ignite.cluster().forCacheNodes(null)).broadcast(new IgniteRunnable() {
                @Override public void run() {
                    // Load cache from persistent store.
                    cache.loadCache(null, ENTRY_COUNT);
                }
            });

            long end = System.currentTimeMillis();

            System.out.println(">>> Loaded " + cache.size() +" keys with backups in " + (end - start) + "ms.");
        }
    }
}
