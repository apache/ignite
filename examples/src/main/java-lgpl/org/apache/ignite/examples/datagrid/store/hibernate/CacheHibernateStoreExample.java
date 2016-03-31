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

package org.apache.ignite.examples.datagrid.store.hibernate;

import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.hibernate.CacheHibernateStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.examples.util.DbH2ServerStartup;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Demonstrates usage of cache with underlying persistent store configured.
 * <p>
 * This example uses {@link CacheHibernatePersonStore} as a persistent store.
 * <p>
 * To start the example, you should:
 * <ul>
 *     <li>Start H2 database TCP server using {@link DbH2ServerStartup}.</li>
 *     <li>Start a few nodes using {@link ExampleNodeStartup}.</li>
 *     <li>Start example using {@link CacheHibernateStoreExample}.</li>
 * </ul>
 * <p>
 * Remote nodes can be started with {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheHibernateStoreExample {
    /** Hibernate configuration resource path. */
    private static final String HIBERNATE_CFG =
        "/org/apache/ignite/examples/datagrid/store/hibernate/hibernate.cfg.xml";

    /** Cache name. */
    private static final String CACHE_NAME = CacheHibernateStoreExample.class.getSimpleName();

    /** Heap size required to run this example. */
    public static final int MIN_MEMORY = 1024 * 1024 * 1024;

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 100_000;

    /** Global person ID to use across entire example. */
    private static final Long id = Math.abs(UUID.randomUUID().getLeastSignificantBits());

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        ExamplesUtils.checkMinMemory(MIN_MEMORY);

        // To start ignite with desired configuration uncomment the appropriate line.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache store example started.");

            CacheConfiguration<Long, Person> cacheCfg = new CacheConfiguration<>(CACHE_NAME);

            // Set atomicity as transaction, since we are showing transactions in example.
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            // Configure Hibernate store.
            cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheHibernatePersonStore.class));

            // Configure Hibernate session listener.
            cacheCfg.setCacheStoreSessionListenerFactories(new Factory<CacheStoreSessionListener>() {
                @Override public CacheStoreSessionListener create() {
                    CacheHibernateStoreSessionListener lsnr = new CacheHibernateStoreSessionListener();

                    lsnr.setHibernateConfigurationPath(HIBERNATE_CFG);

                    return lsnr;
                }
            });

            cacheCfg.setReadThrough(true);
            cacheCfg.setWriteThrough(true);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheCfg)) {
                // Make initial cache loading from persistent store. This is a
                // distributed operation and will call CacheStore.loadCache(...)
                // method on all nodes in topology.
                loadCache(cache);

                // Start transaction and execute several cache operations with
                // read/write-through to persistent store.
                executeTransaction(cache);
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    /**
     * Makes initial cache loading.
     *
     * @param cache Cache to load.
     */
    private static void loadCache(IgniteCache<Long, Person> cache) {
        long start = System.currentTimeMillis();

        // Start loading cache from persistent store on all caching nodes.
        cache.loadCache(null, ENTRY_COUNT);

        long end = System.currentTimeMillis();

        System.out.println(">>> Loaded " + cache.size() + " keys with backups in " + (end - start) + "ms.");
    }

    /**
     * Executes transaction with read/write-through to persistent store.
     *
     * @param cache Cache to execute transaction on.
     */
    private static void executeTransaction(IgniteCache<Long, Person> cache) {
        try (Transaction tx = Ignition.ignite().transactions().txStart()) {
            Person val = cache.get(id);

            System.out.println("Read value: " + val);

            val = cache.getAndPut(id, new Person(id, "Isaac", "Newton"));

            System.out.println("Overwrote old value: " + val);

            val = cache.get(id);

            System.out.println("Read value: " + val);

            tx.commit();
        }

        System.out.println("Read value after commit: " + cache.get(id));
    }
}
