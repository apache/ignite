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

package org.apache.ignite.spark

import org.apache.ignite.Ignition
import org.apache.ignite.cache.query.annotations.{QuerySqlField, QueryTextField}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._
import IgniteRDDSpec._
import org.apache.ignite.binary.BinaryObject

import scala.annotation.meta.field

@RunWith(classOf[JUnitRunner])
class IgniteRDDSpec extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
    describe("IgniteRDD") {
        it("should successfully store data to ignite using savePairs") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                // Save pairs ("0", "val0"), ("1", "val1"), ... to Ignite cache.
                ic.fromCache[String, String](PARTITIONED_CACHE_NAME).savePairs(sc.parallelize(0 to 10000, 2).map(i ⇒ (String.valueOf(i), "val" + i)))

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                for (i ← 0 to 10000) {
                    val res = ignite.cache[String, String](PARTITIONED_CACHE_NAME).get(String.valueOf(i))

                    assert(res != null, "Value was not put to cache for key: " + i)
                    assert("val" + i == res, "Invalid value stored for key: " + i)
                }
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully store data to ignite using savePairs with inline transformation") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc, () ⇒ configuration("client", client = true))

                // Save pairs ("0", "val0"), ("1", "val1"), ... to Ignite cache.
                ic.fromCache(PARTITIONED_CACHE_NAME).savePairs(
                    sc.parallelize(0 to 10000, 2), (i: Int, ic) ⇒ (String.valueOf(i), "val" + i))

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                for (i ← 0 to 10000) {
                    val res = ignite.cache[String, String](PARTITIONED_CACHE_NAME).get(String.valueOf(i))

                    assert(res != null, "Value was not put to cache for key: " + i)
                    assert("val" + i == res, "Invalid value stored for key: " + i)
                }
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully store data to ignite using saveValues") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc, () ⇒ configuration("client", client = true))

                // Save pairs ("0", "val0"), ("1", "val1"), ... to Ignite cache.
                ic.fromCache(PARTITIONED_CACHE_NAME).saveValues(
                    sc.parallelize(0 to 10000, 2).map(i ⇒ "val" + i))

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                val values = ignite.cache[String, String](PARTITIONED_CACHE_NAME).toList.map(e ⇒ e.getValue)

                for (i ← 0 to 10000)
                    assert(values.contains("val" + i), "Value not found for index: " + i)
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully store data to ignite using saveValues with inline transformation") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc, () ⇒ configuration("client", client = true))

                // Save pairs ("0", "val0"), ("1", "val1"), ... to Ignite cache.
                ic.fromCache(PARTITIONED_CACHE_NAME).saveValues(
                    sc.parallelize(0 to 10000, 2), (i: Int, ic) ⇒ "val" + i)

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                val values = ignite.cache[String, String](PARTITIONED_CACHE_NAME).toList.map(e ⇒ e.getValue)

                for (i ← 0 to 10000)
                    assert(values.contains("val" + i), "Value not found for index: " + i)
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully read data from ignite") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val cache = Ignition.ignite("grid-0").cache[String, Int](PARTITIONED_CACHE_NAME)

                val num = 10000

                for (i ← 0 to num) {
                    cache.put(String.valueOf(i), i)
                }

                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                val res = ic.fromCache[String, Int](PARTITIONED_CACHE_NAME).map(_._2).sum()

                assert(res == (0 to num).sum)
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully query objects from ignite") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                val cache: IgniteRDD[String, Entity] = ic.fromCache[String, Entity](PARTITIONED_CACHE_NAME)

                cache.savePairs(sc.parallelize(0 to 1000, 2).map(i ⇒ (String.valueOf(i), new Entity(i, "name" + i, i * 100))))

                val res: Array[Entity] = cache.objectSql("Entity", "name = ? and salary = ?", "name50", 5000).map(_._2).collect()

                assert(res.length == 1, "Invalid result length")
                assert(50 == res(0).id, "Invalid result")
                assert("name50" == res(0).name, "Invalid result")
                assert(5000 == res(0).salary)

                assert(500 == cache.objectSql("Entity", "id > 500").count(), "Invalid count")
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully query fields from ignite") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                val cache: IgniteRDD[String, Entity] = ic.fromCache(PARTITIONED_CACHE_NAME)

                import ic.sqlContext.implicits._

                cache.savePairs(sc.parallelize(0 to 1000, 2).map(i ⇒ (String.valueOf(i), new Entity(i, "name" + i, i * 100))))

                val df = cache.sql("select id, name, salary from Entity where name = ? and salary = ?", "name50", 5000)

                df.printSchema()

                val res = df.collect()

                assert(res.length == 1, "Invalid result length")
                assert(50 == res(0)(0), "Invalid result")
                assert("name50" == res(0)(1), "Invalid result")
                assert(5000 == res(0)(2), "Invalid result")

                val df0 = cache.sql("select id, name, salary from Entity").where('NAME === "name50" and 'SALARY === 5000)

                val res0 = df0.collect()

                assert(res0.length == 1, "Invalid result length")
                assert(50 == res0(0)(0), "Invalid result")
                assert("name50" == res0(0)(1), "Invalid result")
                assert(5000 == res0(0)(2), "Invalid result")

                assert(500 == cache.sql("select id from Entity where id > 500").count(), "Invalid count")
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully start spark context with XML configuration") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc,
                    "modules/core/src/test/config/spark/spark-config.xml")

                val cache: IgniteRDD[String, String] = ic.fromCache[String, String](PARTITIONED_CACHE_NAME)

                cache.savePairs(sc.parallelize(1 to 1000, 2).map(i ⇒ (String.valueOf(i), "val" + i)))

                assert(1000 == cache.count())
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully query complex object fields") {
            assert(false, "https://issues.apache.org/jira/browse/IGNITE-3077")

            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                val cache: IgniteRDD[Integer, WithObjectField] = ic.fromCache[Integer, WithObjectField](PARTITIONED_CACHE_NAME)

                cache.savePairs(sc.parallelize(0 to 1000, 2).map(i ⇒ (i:java.lang.Integer, new WithObjectField(i, new Entity(i, "", i)))))

                val df = cache.sql("select i, ts from WithLocalDate where i = ?", 50)

                df.printSchema()

                val res = df.collect()

                assert(res.length == 1, "Invalid result length")
                assert(50 == res(0)(0), "Invalid result")
            }
            finally {
                sc.stop()
            }
        }

        it("should properly count RDD size") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc, () ⇒ configuration("client", client = true))

                val cache: IgniteRDD[Integer, WithObjectField] = ic.fromCache(PARTITIONED_CACHE_NAME)

                assert(cache.count() == 0)
                assert(cache.isEmpty())

                cache.savePairs(sc.parallelize(0 until 1000, 2).map(i ⇒ (i:java.lang.Integer, new WithObjectField(i, new Entity(i, "", i)))))

                assert(cache.count() == 1000)
                assert(!cache.isEmpty())

                cache.clear()

                assert(cache.count() == 0)
                assert(cache.isEmpty())
            }
            finally {
                sc.stop()
            }
        }

        it("should properly work with binary objects") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc, () ⇒ configuration("client", client = true))

                val cache = ic.fromCache[String, Entity](PARTITIONED_CACHE_NAME)

                cache.savePairs(sc.parallelize(0 until 10, 2).map(i ⇒ (String.valueOf(i),
                    new Entity(i, "name" + i, i * 100))))

                val res = cache.withKeepBinary[String, BinaryObject]().map(t ⇒ t._2.field[Int]("salary")).collect()

                println(res)
            }
            finally {
                sc.stop()
            }
        }
    }

    override protected def beforeEach() = {
        Ignition.ignite("grid-0").cache(PARTITIONED_CACHE_NAME).removeAll()
    }

    override protected def afterEach() = {
        Ignition.stop("client", false)
    }

    override protected def beforeAll() = {
        for (i ← 0 to 3) {
            Ignition.start(configuration("grid-" + i, client = false))
        }
    }

    override protected def afterAll() = {
        for (i ← 0 to 3) {
            Ignition.stop("grid-" + i, false)
        }
    }
}

case class WithObjectField(
    @(QuerySqlField @field)(index = true) val i : Int,
    @(QuerySqlField @field)(index = false) val ts : Object
) {
}

/**
 * Constants and utility methods.
 */
object IgniteRDDSpec {
    /** IP finder for the test. */
    val IP_FINDER = new TcpDiscoveryVmIpFinder(true)

    /** Partitioned cache name. */
    val PARTITIONED_CACHE_NAME = "partitioned"

    /** Type alias for `QuerySqlField`. */
    type ScalarCacheQuerySqlField = QuerySqlField @field

    /** Type alias for `QueryTextField`. */
    type ScalarCacheQueryTextField = QueryTextField @field

    /**
     * Gets ignite configuration.
     *
     * @param gridName Grid name.
     * @param client Client mode flag.
     * @return Ignite configuration.
     */
    def configuration(gridName: String, client: Boolean): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        val discoSpi = new TcpDiscoverySpi

        discoSpi.setIpFinder(IgniteRDDSpec.IP_FINDER)

        cfg.setDiscoverySpi(discoSpi)

        cfg.setCacheConfiguration(cacheConfiguration(gridName))

        cfg.setClientMode(client)

        cfg.setGridName(gridName)

        cfg
    }

    /**
     * Gets cache configuration for the given grid name.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    def cacheConfiguration(gridName: String): CacheConfiguration[Object, Object] = {
        val ccfg = new CacheConfiguration[Object, Object]()

        ccfg.setBackups(1)

        ccfg.setName(PARTITIONED_CACHE_NAME)

        ccfg.setIndexedTypes(
            classOf[String], classOf[Entity],
            classOf[Integer], classOf[WithObjectField]
        )

        ccfg
    }
}
