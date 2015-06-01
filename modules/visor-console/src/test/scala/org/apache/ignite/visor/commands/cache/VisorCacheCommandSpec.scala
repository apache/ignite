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

package org.apache.ignite.visor.commands.cache

import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.CacheMode._
import org.apache.ignite.cache.query.SqlQuery
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration._
import org.apache.ignite.spi.discovery.tcp._
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm._

import java.lang.{Integer => JavaInt}
import org.jetbrains.annotations._

import org.apache.ignite.visor._
import org.apache.ignite.visor.commands.cache.VisorCacheCommand._

/**
 * Unit test for 'events' command.
 */
class VisorCacheCommandSpec extends VisorRuntimeBaseSpec(1) {

    /** IP finder. */
    val ipFinder = new TcpDiscoveryVmIpFinder(true)

    /**
     * @param name Cache name.
     * @return Cache Configuration.
     */
    def cacheConfig(@Nullable name: String): CacheConfiguration[Object, Object] = {
        val cfg = new CacheConfiguration[Object, Object]

        cfg.setCacheMode(REPLICATED)
        cfg.setAtomicityMode(TRANSACTIONAL)
        cfg.setName(name)

        val arr = Seq(classOf[JavaInt], classOf[Foo]).toArray

        cfg.setIndexedTypes(arr: _*)

        cfg
    }

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Grid name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration =
    {
        val cfg = new IgniteConfiguration

        cfg.setGridName(name)
        cfg.setLocalHost("127.0.0.1")
        cfg.setCacheConfiguration(cacheConfig("replicated"))

        val discoSpi = new TcpDiscoverySpi()

        discoSpi.setIpFinder(ipFinder)

        cfg.setDiscoverySpi(discoSpi)

        cfg
    }

    describe("A 'cache' visor command") {
        it("should put/get some values to/from cache and display information about caches") {
            val c = Ignition.ignite("node-1").cache[String, String]("replicated")

            for (i <- 0 to 3) {
                val kv = "" + i

                c.put(kv, kv)

                c.get(kv)
            }

            visor.cache()
        }

        it("should run query and display information about caches") {
            val g = Ignition.ignite("node-1")

            val c = g.cache[JavaInt, Foo]("replicated")

            c.put(0, Foo(20))
            c.put(1, Foo(100))
            c.put(2, Foo(101))
            c.put(3, Foo(150))

            // Create and execute query that mast return 2 rows.
            val q1 = c.query(new SqlQuery(classOf[Foo], "_key > ?").setArgs(JavaInt.valueOf(1))).getAll

            assert(q1.size() == 2)

            // Create and execute query that mast return 0 rows.
            val q2 = c.query(new SqlQuery(classOf[Foo], "_key > ?").setArgs(JavaInt.valueOf(100))).getAll

            assert(q2.size() == 0)

            visor cache "-a"
        }

        it("should display correct information for 'replicated' cache only") {
            visor cache "-n=replicated -a"
        }

        it("should display correct information for all caches") {
            visor cache "-a"
        }
    }
}

/**
 * Object for queries.
 */
private case class Foo(
    @QuerySqlField
    value: Int
)
