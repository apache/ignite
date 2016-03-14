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

package org.apache.ignite.scalar.examples.datagrid.store.auto

import java.lang.{Long => JavaLong}
import java.sql.Types
import java.util.UUID

import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect
import org.apache.ignite.cache.store.jdbc.{CacheJdbcPojoStore, CacheJdbcPojoStoreFactory, JdbcType, JdbcTypeField}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.examples.util.DbH2ServerStartup
import org.apache.ignite.scalar.examples.model.Person
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.transactions.Transaction
import org.h2.jdbcx.JdbcConnectionPool

/**
 * Demonstrates usage of cache with underlying persistent store configured.
 * <p>
 * This example uses [[CacheJdbcPojoStore]] as a persistent store.
 * <p>
 * To start the example, you should:
 * <ul>
 *  <li>Start H2 database TCP server using [[DbH2ServerStartup]].</li>
 *  <li>Start a few nodes using [[ExampleNodeStartup]] or by starting remote nodes as specified below.</li>
 *  <li>Start example using [[ScalarCacheAutoStoreExample]].</li>
 * </ul>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheAutoStoreExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val CACHE_NAME = ScalarCacheAutoStoreExample.getClass.getSimpleName

    /** Global person ID to use across entire example. */
    private val id = Math.abs(UUID.randomUUID.getLeastSignificantBits)

    scalar(CONFIG) {
        println
        println(">>> Populate database with data...")

        DbH2ServerStartup.populateDatabase()

        println
        println(">>> Cache auto store example started...")

        // Auto-close cache at the end of the example.
        val cache = createCache$(cacheConfiguration())
        try {
            val tx = transaction$()

            try {
                var value = cache.get(id)

                println(">>> Read value: " + value)

                value = cache.getAndPut(id, new Person(id, 1L, "Isaac", "Newton", 100.10, "English physicist and mathematician"))

                println(">>> Overwrote old value: " + value)

                value = cache.get(id)

                println(">>> Read value: " + value)

                println(">>> Update salary in transaction...")

                value.setSalary(value.getSalary * 2)

                cache.put(id, value)

                tx.commit()
            } finally {
                if (tx != null)
                    tx.close()
            }

            println(">>> Read value after commit: " + cache.get(id))

            cache.clear()

            println(">>> ------------------------------------------")
            println(">>> Load data to cache from DB with custom SQL...")

            cache.loadCache(null, "java.lang.Long", "select * from PERSON where id <= 3")

            println(">>> Loaded cache entries: " + cache.size())

            cache.clear()

            println(">>> Load ALL data to cache from DB...")

            cache.loadCache(null)

            println(">>> Loaded cache entries: " + cache.size())
        }
        finally {
            ignite$.destroyCache(CACHE_NAME)

            if (cache != null)
                cache.close()
        }
    }

    /**
      * Example store factory.
      */
    private final class CacheJdbcPojoStoreExampleFactory extends CacheJdbcPojoStoreFactory[JavaLong, Person] {
        /** @inheritdoc */
        override def create: CacheJdbcPojoStore[JavaLong, Person] = {
            setDataSource(JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", ""))

            super.create
        }
    }

    /**
      * Configure cache with store.
      */
    def cacheConfiguration(): CacheConfiguration[JavaLong, Person] = {
        val cfg = new CacheConfiguration[JavaLong, Person](CACHE_NAME)

        val storeFactory = new ScalarCacheAutoStoreExample.CacheJdbcPojoStoreExampleFactory

        storeFactory.setDialect(new H2Dialect)

        val jdbcType = new JdbcType

        jdbcType.setCacheName(CACHE_NAME)
        jdbcType.setDatabaseSchema("PUBLIC")
        jdbcType.setDatabaseTable("PERSON")

        jdbcType.setKeyType("java.lang.Long")
        jdbcType.setKeyFields(new JdbcTypeField(Types.BIGINT, "ID", classOf[JavaLong], "id"))

        jdbcType.setValueType("org.apache.ignite.scalar.examples.model.Person")
        jdbcType.setValueFields(
            new JdbcTypeField(Types.BIGINT, "ID", classOf[JavaLong], "id"),
            new JdbcTypeField(Types.VARCHAR, "FIRST_NAME", classOf[String], "firstName"),
            new JdbcTypeField(Types.VARCHAR, "LAST_NAME", classOf[String], "lastName")
        )

        storeFactory.setTypes(jdbcType)

        cfg.setCacheStoreFactory(storeFactory)

        // Set atomicity as transaction, since we are showing transactions in the example.
        cfg.setAtomicityMode(TRANSACTIONAL)

        cfg.setReadThrough(true)
        cfg.setWriteThrough(true)

        cfg
    }
}
