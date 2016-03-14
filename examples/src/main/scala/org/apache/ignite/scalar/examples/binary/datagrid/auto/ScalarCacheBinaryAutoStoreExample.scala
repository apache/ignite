/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.apache.ignite.scalar.examples.binary.datagrid.auto

import java.sql.Types

import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect
import org.apache.ignite.cache.store.jdbc.{CacheJdbcPojoStore, CacheJdbcPojoStoreFactory, JdbcType, JdbcTypeField}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.examples.model.Person
import org.apache.ignite.examples.util.DbH2ServerStartup
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
  * <li>Start example using [[ScalarCacheBinaryAutoStoreExample]].</li>
  * </ul>
  * <p>
  * Remote nodes should always be started with special configuration file which
  * contains H2 data source bean descriptor: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
  * <p>
  * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
  * start node with `examples/config/example-ignite.xml` configuration.
  */
object ScalarCacheBinaryAutoStoreExample extends App {
    /** Global person ID to use across entire example. */
    private val id = 25121642L

    /** Cache name. */
    private val CACHE_NAME = ScalarCacheBinaryAutoStoreExample.getClass.getSimpleName

    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /**
      * Configure cache with store.
      */
    private def cacheConfiguration(): CacheConfiguration[Long, Person] = {
        val storeFactory = new CacheJdbcPojoStoreFactory[Long, Person]
        
        storeFactory.setDataSourceBean("h2-example-db")
        storeFactory.setDialect(new H2Dialect)
        
        val jdbcType = new JdbcType
        
        jdbcType.setCacheName(CACHE_NAME)
        jdbcType.setDatabaseSchema("PUBLIC")
        jdbcType.setDatabaseTable("PERSON")
        jdbcType.setKeyType("java.lang.Long")
        jdbcType.setKeyFields(new JdbcTypeField(Types.BIGINT, "ID", classOf[Long], "id"))
        jdbcType.setValueType("org.apache.ignite.examples.model.Person")
        jdbcType.setValueFields(
            new JdbcTypeField(Types.BIGINT, "ID", classOf[Long], "id"), 
            new JdbcTypeField(Types.VARCHAR, "FIRST_NAME", classOf[String], "firstName"), 
            new JdbcTypeField(Types.VARCHAR, "LAST_NAME", classOf[String], "lastName")
        )

        storeFactory.setTypes(jdbcType)

        val cfg = new CacheConfiguration[Long, Person](CACHE_NAME)
        
        cfg.setCacheStoreFactory(storeFactory)
        cfg.setAtomicityMode(TRANSACTIONAL)
        cfg.setStoreKeepBinary(true)
        cfg.setReadThrough(true)
        cfg.setWriteThrough(true)
        
        cfg
    }

    scalar(CONFIG) {
        println
        println(">>> Populate database with data...")

        DbH2ServerStartup.populateDatabase()

        println
        println(">>> Cache auto store example started...")

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

                value.salary *= 2

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
        } finally {
            ignite$.destroyCache(CACHE_NAME)

            if (cache != null)
                cache.close()
        }
    }
}
