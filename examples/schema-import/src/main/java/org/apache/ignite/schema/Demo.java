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
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.h2.jdbcx.*;

import javax.cache.*;
import javax.cache.configuration.*;

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
     * Constructs and returns a fully configured instance of a {@link CacheJdbcPojoStore}.
     */
    private static class H2DemoStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            CacheJdbcPojoStore store = new CacheJdbcPojoStore<>();

            store.setDataSource(JdbcConnectionPool.create("jdbc:h2:tcp://localhost/~/schema-import/demo", "sa", ""));

            return store;
        }
    }

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
        CacheConfiguration ccfg = CacheConfig.cache(CACHE_NAME, new H2DemoStoreFactory());

        cfg.setCacheConfiguration(ccfg);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteCache<PersonKey, Person> cache = ignite.cache(CACHE_NAME);

            // Preload cache from database.
            preload(cache);

            // Read-through from database
            // and store in cache.
            readThrough(cache);

            // Perform transaction and
            // write-through to database.
            transaction(ignite, cache);
        }
    }

    /**
     * Demonstrates cache preload from database.
     */
    private static void preload(IgniteCache<PersonKey, Person> cache) {
        System.out.println();
        System.out.println(">>> Loading entries from database.");

        // Preload all person keys that are less than or equal to 3.
        cache.loadCache(null, PersonKey.class.getName(), "select * from PERSON where ID <= 3");

        for (Cache.Entry<PersonKey, Person> person : cache)
            System.out.println(">>> Loaded Person: " + person);
    }

    /**
     * Demonstrates cache read through from database.
     */
    private static void readThrough(IgniteCache<PersonKey, Person> cache) {
        PersonKey key = new PersonKey(4);

        System.out.println();
        System.out.println(">>> Read-through person from database for ID: " + key.getId());

        // Check that person with ID=4 is not in cache.
        Person p = cache.localPeek(key);

        assert p == null;

        // Read-through form database.
        p = cache.get(new PersonKey(4));

        System.out.println(">>> Loaded person from database: " + p);
    }

    /**
     * Demonstrates cache transaction joining database transaction.
     */
    private static void transaction(Ignite ignite, IgniteCache<PersonKey, Person> cache) {
        PersonKey key = new PersonKey(5);

        System.out.println();
        System.out.println(">>> Update salary and write-through to database for person with ID: " + key.getId());

        try (Transaction tx = ignite.transactions().txStart()) {
            // Read-through from database.
            Person p = cache.get(key);

            System.out.println(">>> Loaded person from database: " + p);

            double salary = p.getSalary();

            // Raise salary by 20%.
            p.setSalary(salary * 1.2);

            // Write-through to database
            // and store in cache.
            cache.put(key, p);

            tx.commit();
        }

        System.out.println(">>> Updated person: " + cache.get(key));
    }
}
