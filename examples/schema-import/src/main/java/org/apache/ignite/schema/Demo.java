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

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * This demo demonstrates the use of cache with {@link CacheJdbcPojoStore}
 * together with automatic Ignite schema-import utility.
 * <p>
 * This Demo can work stand-alone. You can also choose to start
 * several {@link DemoNode} instances as well to form a cluster.
 */
public class Demo {
    /**
     * Constructs and returns a fully configured instance of a {@link CacheJdbcPojoStore}.
     */
    private static class H2DemoStoreFactory<K, V> implements Factory<CacheStore<K, V>> {
        /** {@inheritDoc} */
        @Override public CacheStore<K, V> create() {
            CacheJdbcPojoStore<K, V> store = new CacheJdbcPojoStore<>();

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

        // Start Ignite node.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            // Configure cache store.
            CacheConfiguration<PersonKey, Person> cfg =
                CacheConfig.cache("PersonCache", new H2DemoStoreFactory<PersonKey, Person>());

            try (IgniteCache<PersonKey, Person> cache = ignite.getOrCreateCache(cfg)) {
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