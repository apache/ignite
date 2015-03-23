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

package org.apache.ignite.schema;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.configuration.*;

import javax.cache.*;

/**
 * Demo for CacheJdbcPojoStore.
 *
 * This example demonstrates the use of cache with {@link CacheJdbcPojoStore}.
 *
 * Custom SQL will be executed to populate cache with data from database.
 */
public class Demo {
    /** */
    private static final String CACHE_NAME = "Person";

    /**
     * Executes demo.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        System.out.println(">>> Start demo...");

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Configure cache store.
        CacheConfiguration ccfg = CacheConfig.cache(CACHE_NAME,
            org.h2.jdbcx.JdbcConnectionPool.create("jdbc:h2:tcp://localhost/~/schema-import/demo", "sa", ""));

        cfg.setCacheConfiguration(ccfg);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteCache<PersonKey, Person> cache = ignite.jcache(CACHE_NAME);

            // Demo for load cache with custom SQL.
            cache.loadCache(null, "org.apache.ignite.schema.PersonKey",
                "select * from PERSON where ID <= 3");

            for (Cache.Entry<PersonKey, Person> person : cache)
                System.out.println(">>> Loaded Person: " + person);
        }
    }

    /** Demonstrates cache preload from database.  */
    private static void preload() {
        // TODO
    }

    /** Demonstrates cache wright through to database.  */
    private static void writeThrough() {
        // TODO
    }

    /** Demonstrates cache read through from database.  */
    private static void readThrough() {
        // TODO
    }

    /** Demonstrates cache remove from database.  */
    private static void remove() {
        // TODO
    }

    /** Demonstrates cache transaction from database.  */
    private static void transaction() {
        // TODO
    }
}
