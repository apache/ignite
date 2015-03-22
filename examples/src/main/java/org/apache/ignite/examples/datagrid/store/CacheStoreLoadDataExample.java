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
import org.apache.ignite.configuration.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;

/**
 * Loads data on all cache nodes from persistent store at cache startup by calling
 * {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)} method.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheStoreLoadDataExample {
    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 1024 * 1024 * 1024;

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 100_000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache store load data example started.");

            CacheConfiguration<Long, Person> cacheCfg = CacheStoreExampleCacheConfigurator.cacheConfiguration();

            try (IgniteCache<Long, Person> cache = ignite.createCache(cacheCfg)) {
                long start = System.currentTimeMillis();

                // Start loading cache from persistent store on all caching nodes.
                cache.loadCache(null, ENTRY_COUNT);

                long end = System.currentTimeMillis();

                System.out.println(">>> Loaded " + cache.size() + " keys with backups in " + (end - start) + "ms.");
            }
        }
    }
}
