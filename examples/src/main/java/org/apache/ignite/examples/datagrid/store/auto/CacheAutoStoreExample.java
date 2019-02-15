/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.datagrid.store.auto;

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
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.examples.util.DbH2ServerStartup;
import org.apache.ignite.transactions.Transaction;
import org.h2.jdbcx.JdbcConnectionPool;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Demonstrates usage of cache with underlying persistent store configured.
 * <p>
 * This example uses {@link CacheJdbcPojoStore} as a persistent store.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start H2 database TCP server using {@link DbH2ServerStartup}.</li>
 *     <li>Start a few nodes using {@link ExampleNodeStartup}.</li>
 *     <li>Start example using {@link CacheAutoStoreExample}.</li>
 * </ul>
 * <p>
 * Remote nodes can be started with {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheAutoStoreExample {
    /** Global person ID to use across entire example. */
    private static final Long id = 25121642L;

    /** Cache name. */
    public static final String CACHE_NAME = CacheAutoStoreExample.class.getSimpleName();

    /**
     * Example store factory.
     */
    private static final class CacheJdbcPojoStoreExampleFactory extends CacheJdbcPojoStoreFactory<Long, Person> {
        /** {@inheritDoc} */
        @Override public CacheJdbcPojoStore<Long, Person> create() {
            setDataSource(JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", ""));

            return super.create();
        }
    }

    /**
     * Configure cache with store.
     */
    private static CacheConfiguration<Long, Person> cacheConfiguration() {
        CacheConfiguration<Long, Person> cfg = new CacheConfiguration<>(CACHE_NAME);

        CacheJdbcPojoStoreExampleFactory storeFactory = new CacheJdbcPojoStoreExampleFactory();

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

        cfg.setCacheStoreFactory(storeFactory);

        // Set atomicity as transaction, since we are showing transactions in the example.
        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);

        return cfg;
    }

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // To start ignite with desired configuration uncomment the appropriate line.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Populate database with data...");
            DbH2ServerStartup.populateDatabase();

            System.out.println();
            System.out.println(">>> Cache auto store example started...");

            // Auto-close cache at the end of the example.
            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheConfiguration())) {
                try (Transaction tx = ignite.transactions().txStart()) {
                    Person val = cache.get(id);

                    System.out.println(">>> Read value: " + val);

                    val = cache.getAndPut(id, new Person(id, 1L, "Isaac", "Newton", 100.10, "English physicist and mathematician"));

                    System.out.println(">>> Overwrote old value: " + val);

                    val = cache.get(id);

                    System.out.println(">>> Read value: " + val);

                    System.out.println(">>> Update salary in transaction...");

                    val.salary *= 2;

                    cache.put(id, val);

                    tx.commit();
                }

                System.out.println(">>> Read value after commit: " + cache.get(id));

                cache.clear();

                System.out.println(">>> ------------------------------------------");
                System.out.println(">>> Load data to cache from DB with custom SQL...");

                // Load cache on all data nodes with custom SQL statement.
                cache.loadCache(null, "java.lang.Long", "select * from PERSON where id <= 3");

                System.out.println(">>> Loaded cache entries: " + cache.size());

                cache.clear();

                // Load cache on all data nodes with default SQL statement.
                System.out.println(">>> Load ALL data to cache from DB...");
                cache.loadCache(null);

                System.out.println(">>> Loaded cache entries: " + cache.size());
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }
}
