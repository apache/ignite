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

package org.apache.ignite.examples.binary.datagrid.store.auto;

import java.sql.Types;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.util.DbH2ServerStartup;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Demonstrates usage of cache with underlying persistent store configured.
 * <p>
 * This example uses {@link CacheJdbcPojoStore} as a persistent store.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start H2 database TCP server using {@link DbH2ServerStartup}.</li>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start example using {@link CacheBinaryAutoStoreExample}.</li>
 * </ul>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * contains H2 data source bean descriptor: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheBinaryAutoStoreExample {
    /** Global person ID to use across entire example. */
    private static final Long id = Math.abs(UUID.randomUUID().getLeastSignificantBits());

    /** Cache name. */
    public static final String CACHE_NAME = CacheBinaryAutoStoreExample.class.getSimpleName();

    /**
     * Configure cache with store.
     */
    private static CacheConfiguration<Long, Person> cacheConfiguration() {
        CacheJdbcPojoStoreFactory<Long, Person> storeFactory = new CacheJdbcPojoStoreFactory<>();

        storeFactory.setDataSourceBean("h2-example-db");
        storeFactory.setDialect(new H2Dialect());

        JdbcType jdbcType = new JdbcType();

        jdbcType.setCacheName(CACHE_NAME);
        jdbcType.setDatabaseSchema("PUBLIC");
        jdbcType.setDatabaseTable("PERSON");

        jdbcType.setKeyType("java.lang.Long");
        jdbcType.setKeyFields(new JdbcTypeField(Types.BIGINT, "ID", Long.class, "id"));

        jdbcType.setValueType("org.apache.ignite.examples.model.Person");
        jdbcType.setValueFields(
                new JdbcTypeField(Types.BIGINT, "ID", Long.class, "id"),
                new JdbcTypeField(Types.VARCHAR, "FIRST_NAME", String.class, "firstName"),
                new JdbcTypeField(Types.VARCHAR, "LAST_NAME", String.class, "lastName")
        );

        storeFactory.setTypes(jdbcType);

        CacheConfiguration<Long, Person> cfg = new CacheConfiguration<>(CACHE_NAME);

        cfg.setCacheStoreFactory(storeFactory);

        // Set atomicity as transaction, since we are showing transactions in the example.
        cfg.setAtomicityMode(TRANSACTIONAL);

        // This option will allow to start remote nodes without having user classes in classpath.
        cfg.setKeepBinaryInStore(true);

        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);

        return cfg;
    }

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        // To start ignite with desired configuration uncomment the appropriate line.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache auto store example started.");

            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheConfiguration())) {
                try (Transaction tx = ignite.transactions().txStart()) {
                    Person val = cache.get(id);

                    System.out.println("Read value: " + val);

                    val = cache.getAndPut(id, new Person(id, "Isaac", "Newton"));

                    System.out.println("Overwrote old value: " + val);

                    val = cache.get(id);

                    System.out.println("Read value: " + val);

                    tx.commit();
                }

                System.out.println("Read value after commit: " + cache.get(id));

                cache.clear();

                System.out.println(">>> ------------------------------------------");
                System.out.println(">>> Load data to cache from DB with cusom SQL...");

                // Load cache on all data nodes with custom SQL statement.
                cache.loadCache(null, "java.lang.Long", "select * from PERSON where id <= 3");

                System.out.println("Loaded cache entries: " + cache.size());

                cache.clear();

                // Load cache on all data nodes with default SQL statement.
                System.out.println(">>> Load ALL data to cache from DB...");
                cache.loadCache(null);

                System.out.println("Loaded cache entries: " + cache.size());
            }
        }
    }
}
