/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid.hibernate

import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.CacheWriteSynchronizationMode._
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import org.hibernate.cfg.Configuration
import org.hibernate.service.ServiceRegistryBuilder
import org.hibernate.{Session, SessionFactory}

import javax.persistence._
import java.net.URL
import java.util.{Arrays, HashSet => JavaHashSet, List => JavaList, Set => JavaSet}

import scala.collection.JavaConversions._

/**
 * This example demonstrates the use of Ignite In-Memory Data Ignite cluster as a Hibernate
 * Second-Level cache provider.
 * <p>
 * The Hibernate Second-Level cache (or "L2 cache" shortly) lets you significantly
 * reduce the number of requests to the underlying SQL database. Because database
 * access is known to be an expansive operation, using L2 cache may improve
 * performance dramatically.
 * <p>
 * This example defines 2 entity classes: [[User]] and [[Post]], with
 * 1 <-> N relation, and marks them with appropriate annotations for Hibernate
 * object-relational mapping to SQL tables of an underlying H2 in-memory database.
 * The example launches node in the same JVM and registers it in
 * Hibernate configuration as an L2 cache implementation. It then stores and
 * queries instances of the entity classes to and from the database, having
 * Hibernate SQL output, L2 cache statistics output, and Ignite cache metrics
 * output enabled.
 * <p>
 * When running example, it's easy to notice that when an object is first
 * put into a database, the L2 cache is not used and it's contents is empty.
 * However, when an object is first read from the database, it is immediately
 * stored in L2 cache (which is Ignite In-Memory Data Ignite cluster in fact), which can
 * be seen in stats output. Further requests of the same object only read the data
 * from L2 cache and do not hit the database.
 * <p>
 * In this example, the Hibernate query cache is also enabled. Query cache lets you
 * avoid hitting the database in case of repetitive queries with the same parameter
 * values. You may notice that when the example runs the same query repeatedly in
 * loop, only the first query hits the database and the successive requests take the
 * data from L2 cache.
 * <p>
 * Note: this example uses [[AccessType#READ_ONLY]] L2 cache access type, but you
 * can experiment with other access types by modifying the Hibernate configuration file
 * `IGNITE_HOME/examples/config/hibernate/example-hibernate-L2-cache.xml`, used by the example.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarHibernateL2CacheExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** JDBC URL for backing database (an H2 in-memory database is used). */
    private val JDBC_URL = "jdbc:h2:mem:example;DB_CLOSE_DELAY=-1"
    
    /** Path to hibernate configuration file (will be resolved from application `CLASSPATH`). */
    private val HIBERNATE_CFG = "hibernate/example-scalar-hibernate-L2-cache.xml"
    
    /** Entity names for stats output. */
    private val ENTITY_NAMES: JavaList[String] = 
        Arrays.asList(classOf[User].getName, classOf[Post].getName, classOf[User].getName + ".posts")
    // We use a single session factory, but create a dedicated session
    // for each transaction or query. This way we ensure that L1 cache
    // is not used (L1 cache has per-session scope only).
    
    scalar(CONFIG) {
        println()
        println(">>> Hibernate L2 cache example started.")

        val c1 = createCache("org.hibernate.cache.spi.UpdateTimestampsCache", ATOMIC)
        val c2 = createCache("org.hibernate.cache.internal.StandardQueryCache", ATOMIC)
        val c3 = createCache("org.apache.ignite.scalar.examples.datagrid.hibernate.User", TRANSACTIONAL)
        val c4 = createCache("org.apache.ignite.scalar.examples.datagrid.hibernate.User.posts", TRANSACTIONAL)
        val c5 = createCache("org.apache.ignite.scalar.examples.datagrid.hibernate.Post", TRANSACTIONAL)

        try {
            val hibernateCfg: URL = ExamplesUtils.url(HIBERNATE_CFG)

            val sesFactory: SessionFactory = createHibernateSessionFactory(hibernateCfg)

            println()
            println(">>> Creating objects.")

            var userId = 0L

            var ses: Session = sesFactory.openSession

            try {
                val tx = ses.beginTransaction

                val user = new User("jedi", "Luke", "Skywalker")

                user.getPosts.add(new Post(user, "Let the Force be with you."))

                ses.save(user)

                tx.commit()

                userId = user.getId
            }
            finally {
                ses.close
            }

            printStats(sesFactory)

            println()
            println(">>> Querying object by ID.")

            for (i <- 0 until 3) {
                ses = sesFactory.openSession

                try {
                    val tx = ses.beginTransaction

                    val user = ses.get(classOf[User], userId).asInstanceOf[User]

                    println("User: " + user)

                    for (post <- user.getPosts)
                        println("\tPost: " + post)

                    tx.commit()
                }
                finally {
                    ses.close
                }
            }

            printStats(sesFactory)
        }
        finally {
            if (c1 != null)
                c1.close()

            if (c2 != null)
                c2.close()

            if (c3 != null)
                c3.close()

            if (c4 != null)
                c4.close()

            if (c5 != null)
                c5.close()
        }
    }

    /**
     * Creates cache.
     *
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private def createCache(name: String, atomicityMode: CacheAtomicityMode): IgniteCache[_, _] = {
        val ccfg = new CacheConfiguration[AnyRef, AnyRef](name)

        ccfg.setAtomicityMode(atomicityMode)

        ccfg.setWriteSynchronizationMode(FULL_SYNC)

        createCache$(ccfg)
    }

    /**
     * Creates a new Hibernate [[SessionFactory]] using a programmatic
     * configuration.
     *
     * @param hibernateCfg Hibernate configuration file.
     * @return New Hibernate { @link SessionFactory}.
     */
    private def createHibernateSessionFactory(hibernateCfg: URL): SessionFactory = {
        val builder = new ServiceRegistryBuilder

        builder.applySetting("hibernate.connection.url", JDBC_URL)

        builder.applySetting("hibernate.show_sql", true)

        new Configuration().configure(hibernateCfg).buildSessionFactory(builder.buildServiceRegistry)
    }

    /**
     * Prints Hibernate L2 cache statistics to standard output.
     *
     * @param sesFactory Hibernate { @link SessionFactory}, for which to print
     *                                     statistics.
     */
    private def printStats(sesFactory: SessionFactory) {
        println("=== Hibernate L2 cache statistics ===")

        for (entityName <- ENTITY_NAMES) {
            println("\tEntity: " + entityName)

            val stats = sesFactory.getStatistics.getSecondLevelCacheStatistics(entityName)

            println("\t\tL2 cache entries: " + stats.getEntries)
            println("\t\tHits: " + stats.getHitCount)
            println("\t\tMisses: " + stats.getMissCount)
        }

        println("=====================================")
    }
}




