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
import org.apache.ignite.lang.IgniteUuid
import org.apache.ignite.spark.IgniteRDDSpec._
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
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class IgniteRDDSpec extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
    describe("IgniteRDD") {
        it("should successfully store data to ignite using savePairs") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                // Save pairs ("0", "val0"), ("1", "val1"), ... to Ignite cache.
                ic.fromCache[String, String](STR_STR_CACHE_NAME).savePairs(sc.parallelize(0 to 10000, 2).map(i ⇒ (String.valueOf(i), "val" + i)))

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                for (i ← 0 to 10000) {
                    val res = ignite.cache[String, String](STR_STR_CACHE_NAME).get(String.valueOf(i))

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
                ic.fromCache(STR_STR_CACHE_NAME).savePairs(
                    sc.parallelize(0 to 10000, 2), (i: Int, ic) ⇒ (String.valueOf(i), "val" + i))

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                for (i ← 0 to 10000) {
                    val res = ignite.cache[String, String](STR_STR_CACHE_NAME).get(String.valueOf(i))

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
                ic.fromCache(UUID_STR_CACHE_NAME).saveValues(
                    sc.parallelize(0 to 10000, 2).map(i ⇒ "val" + i))

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                val values = ignite.cache[IgniteUuid, String](UUID_STR_CACHE_NAME).toList.map(e ⇒ e.getValue)

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
                ic.fromCache(UUID_STR_CACHE_NAME).saveValues(
                    sc.parallelize(0 to 10000, 2), (i: Int, ic) ⇒ "val" + i)

                // Check cache contents.
                val ignite = Ignition.ignite("grid-0")

                val values = ignite.cache[IgniteUuid, String](UUID_STR_CACHE_NAME).toList.map(e ⇒ e.getValue)

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
                val cache = Ignition.ignite("grid-0").cache[String, Int](STR_INT_CACHE_NAME)

                val num = 10000

                for (i ← 0 to num) {
                    cache.put(String.valueOf(i), i)
                }

                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                val res = ic.fromCache[String, Int](STR_INT_CACHE_NAME).map(_._2).sum()

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

                val cache: IgniteRDD[String, Entity] = ic.fromCache[String, Entity](ENTITY_CACHE_NAME)

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

                val cache: IgniteRDD[String, Entity] = ic.fromCache(ENTITY_CACHE_NAME)

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

                val cache: IgniteRDD[String, String] = ic.fromCache[String, String](STR_STR_CACHE_NAME)

                cache.savePairs(sc.parallelize(1 to 1000, 2).map(i ⇒ (String.valueOf(i), "val" + i)))

                assert(1000 == cache.count())
            }
            finally {
                sc.stop()
            }
        }

        it("should successfully query complex object fields") {
            val sc = new SparkContext("local[*]", "test")

            try {
                val ic = new IgniteContext(sc,
                    () ⇒ configuration("client", client = true))

                val cache: IgniteRDD[Integer, WithObjectField] = ic.fromCache[Integer, WithObjectField](WITH_OBJECT_FIELD_CACHE_NAME)

                cache.savePairs(sc.parallelize(0 to 1000, 2).map(i ⇒ (i:java.lang.Integer, new WithObjectField(i, new Entity(i, "", i)))))

                val df = cache.sql(s"select i, ts from $WITH_OBJECT_FIELD_CACHE_NAME where i = ?", 50)

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

                val cache: IgniteRDD[Integer, WithObjectField] = ic.fromCache(WITH_OBJECT_FIELD_CACHE_NAME)

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

                val cache = ic.fromCache[String, Entity](ENTITY_CACHE_NAME)

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
        for (cacheName <- Ignition.ignite("grid-0").cacheNames()) {
            Ignition.ignite("grid-0").cache(cacheName).clear()
        }
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
    /** Cache name for the pairs (String, Entity). */
    val ENTITY_CACHE_NAME = "entity"

    /** Cache name for the pairs (String, WithObjectField). */
    val WITH_OBJECT_FIELD_CACHE_NAME = "withObjectField"

    /** Cache name for the pairs (String, String). */
    val STR_STR_CACHE_NAME = "StrStr"

    /** Cache name for the pairs (String, String). */
    val UUID_STR_CACHE_NAME = "UuidStr"

    /** Cache name for the pairs (String, Int). */
    val STR_INT_CACHE_NAME = "StrInt"

    /** Type alias for `QuerySqlField`. */
    type ScalarCacheQuerySqlField = QuerySqlField @field

    /** Type alias for `QueryTextField`. */
    type ScalarCacheQueryTextField = QueryTextField @field

    /**
     * Gets ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param client Client mode flag.
     * @return Ignite configuration.
     */
    def configuration(igniteInstanceName: String, client: Boolean): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setLocalHost("127.0.0.1")

        val discoSpi = new TcpDiscoverySpi

        val ipFinder = new TcpDiscoveryVmIpFinder()

        ipFinder.setAddresses(List("127.0.0.1:47500..47504"))

        discoSpi.setIpFinder(ipFinder)

        cfg.setDiscoverySpi(discoSpi)

        cfg.setCacheConfiguration(
            cacheConfiguration[String, String](STR_STR_CACHE_NAME),
            cacheConfiguration[IgniteUuid, String](UUID_STR_CACHE_NAME),
            cacheConfiguration[String, Integer](STR_INT_CACHE_NAME),
            cacheConfiguration[String, Entity](ENTITY_CACHE_NAME),
            cacheConfiguration[Integer, WithObjectField](WITH_OBJECT_FIELD_CACHE_NAME))

        cfg.setClientMode(client)

        cfg.setIgniteInstanceName(igniteInstanceName)

        cfg
    }

    /**
      * Gets cache configuration for the given grid name.
      *
      * @tparam K class of cached keys
      * @tparam V class of cached values
      * @param cacheName cache name.
      * @return Cache configuration.
      */
    def cacheConfiguration[K : ClassTag, V : ClassTag](cacheName : String): CacheConfiguration[Object, Object] = {
        val ccfg = new CacheConfiguration[Object, Object]()

        ccfg.setBackups(1)

        ccfg.setName(cacheName)

        ccfg.setIndexedTypes(
            implicitly[reflect.ClassTag[K]].runtimeClass.asInstanceOf[Class[K]],
            implicitly[reflect.ClassTag[V]].runtimeClass.asInstanceOf[Class[V]])

        ccfg
    }
}
