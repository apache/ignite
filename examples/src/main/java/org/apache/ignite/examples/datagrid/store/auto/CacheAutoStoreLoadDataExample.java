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

package org.apache.ignite.examples.datagrid.store.auto;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.examples.datagrid.store.Person;

/**
 * Demonstrates how to load data from database.
 * <p>
 * This example uses {@link CacheJdbcPojoStore} as a persistent store.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start H2 database TCP server using {@link DbH2ServerStartup}.</li>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start example using {@link CacheAutoStoreLoadDataExample}.</li>
 * </ul>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheAutoStoreLoadDataExample {
    /** Cache name. */
    public static final String CACHE_NAME = CacheAutoStoreLoadDataExample.class.getSimpleName();

    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 1024 * 1024 * 1024;

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
            System.out.println(">>> Cache auto store load data example started.");

            CacheConfiguration<Long, Person> cacheCfg = CacheConfig.jdbcPojoStoreCache(CACHE_NAME);

            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheCfg)) {
                // Load cache on all data nodes with custom SQL statement.
                cache.loadCache(null, "java.lang.Long", "select * from PERSON where id <= 3");

                System.out.println("Loaded cache entries: " + cache.size());

                cache.clear();

                // Load cache on all data nodes with default SQL statement.
                cache.loadCache(null);

                System.out.println("Loaded cache entries: " + cache.size());
            }
        }
    }
}