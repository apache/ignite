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

package org.apache.ignite.scalar.examples.datagrid.store.jdbc

import java.lang.{Long => JavaLong}
import java.util.UUID
import javax.cache.configuration.FactoryBuilder

import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.examples.util.DbH2ServerStartup
import org.apache.ignite.scalar.examples.model.Person
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates usage of cache with underlying persistent store configured.
 * <p>
 * This example uses [[CacheJdbcPojoStore]] as a persistent store.
 * <p>
 * To start the example, you should:
 * <ul>
 * <li>Start H2 database TCP server using [[DbH2ServerStartup]].</li>
 * <li>Start a few nodes using [[ExampleNodeStartup]] or by starting remote nodes as specified below.</li>
 * <li>Start example using [[ScalarCacheJdbcStoreExample]].</li>
 * </ul>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheJdbcStoreExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val CACHE_NAME = ScalarCacheJdbcStoreExample.getClass.getSimpleName

    /** Heap size required to run this example. */
    val MIN_MEMORY = 1024 * 1024 * 1024

    /** Number of entries to load. */
    private val ENTRY_COUNT = 100000

    /** Global person ID to use across entire example. */
    private val id = Math.abs(UUID.randomUUID.getLeastSignificantBits)

    scalar(CONFIG) {
        println()
        println(">>> Cache auto store example started.")

        val cache = createCache$[JavaLong, Person](CacheConfig.jdbcPojoStoreCache(CACHE_NAME))

        try {
            // Make initial cache loading from persistent store. This is a
            // distributed operation and will call CacheStore.loadCache(...)
            // method on all nodes in topology.
            loadCache(cache)

            // Start transaction and execute several cache operations with
            // read/write-through to persistent store.
            executeTransaction(cache)
        }
        finally {
            if (cache != null) cache.close()
        }
    }

    /**
     * Makes initial cache loading.
     *
     * @param cache Cache to load.
     */
    private def loadCache(cache: IgniteCache[JavaLong, Person]) {
        val start = System.currentTimeMillis

        cache.loadCache(null, ENTRY_COUNT.asInstanceOf[Object])

        val end = System.currentTimeMillis

        println(">>> Loaded " + cache.size() + " keys with backups in " + (end - start) + "ms.")
    }

    /**
     * Executes transaction with read/write-through to persistent store.
     *
     * @param cache Cache to execute transaction on.
     */
    private def executeTransaction(cache: IgniteCache[JavaLong, Person]) {
        val tx = transaction$()

        try {
            var value = cache.get(id)

            println("Read value: " + value)

            value = cache.getAndPut(id, new Person(id, "Isaac", "Newton"))

            println("Overwrote old value: " + value)

            value = cache.get(id)

            println("Read value: " + value)

            tx.commit()
        }
        finally {
            if (tx != null)
                tx.close()
        }

        println("Read value after commit: " + cache.get(id))
    }

    /**
     * Predefined configuration for examples with [[CacheJdbcPojoStore]].
     */
    private object CacheConfig {
        /**
         * Configure cache with store.
         */
        def jdbcPojoStoreCache(name: String): CacheConfiguration[JavaLong, Person] = {
            val cacheCfg = new CacheConfiguration[JavaLong, Person](CACHE_NAME)

            // Set atomicity as transaction, since we are showing transactions in example.
            cacheCfg.setAtomicityMode(TRANSACTIONAL)

            // Configure JDBC store.
            cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(classOf[ScalarCacheJdbcPersonStore]))

            cacheCfg.setReadThrough(true)
            cacheCfg.setWriteThrough(true)

            cacheCfg
        }
    }
}
