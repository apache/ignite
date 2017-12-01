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

import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, Matchers}
import java.lang.{Integer ⇒ JInteger, Long ⇒ JLong}

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.AbstractDataFrameSpec.configuration

/**
  */
abstract class AbstractDataFrameSpec extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter {
    var spark: SparkSession = _

    var client: Ignite = _

    override protected def beforeAll() = {
        spark = SparkSession.builder()
            .appName("DataFrameSpec")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()

        for (i ← 0 to 3)
            Ignition.start(configuration("grid-" + i, client = false))

        client = Ignition.getOrStart(configuration("client", client = true))
    }

    override protected def afterAll() = {
        Ignition.stop("client", false)

        for (i ← 0 to 3)
            Ignition.stop("grid-" + i, false)

        spark.close()
    }

    def createPersonTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE person (
              |    id LONG,
              |    name VARCHAR,
              |    birth_date DATE,
              |    is_resident BOOLEAN,
              |    salary FLOAT,
              |    pension DOUBLE,
              |    account DECIMAL,
              |    age INT,
              |    city_id LONG,
              |    PRIMARY KEY (id, city_id)) WITH "backups=1, affinityKey=city_id"
            """.stripMargin)).getAll

        val qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "John Doe", 3L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Jane Roe", 2L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "Mary Major", 1L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], "Richard Miles", 2L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(5L.asInstanceOf[JLong], null, 2L.asInstanceOf[JLong])).getAll
    }

    def createCityTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll

        val qry = new SqlFieldsQuery("INSERT INTO city (id, name) VALUES (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "Forest Hill")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "Denver")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "St. Petersburg")).getAll
    }
}

object AbstractDataFrameSpec {
    val INT_STR_CACHE_NAME = "cache1"

    val INT_TEST_OBJ_CACHE_NAME = "cache2"

    val TEST_OBJ_TEST_OBJ_CACHE_NAME = "cache3"

    val IP_FINDER = new TcpDiscoveryVmIpFinder(true)

    def configuration(igniteInstanceName: String, client: Boolean): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        val discoSpi = new TcpDiscoverySpi

        discoSpi.setIpFinder(IP_FINDER)

        cfg.setDiscoverySpi(discoSpi)

        cfg.setCacheConfiguration(
            cacheConfiguration[JInteger, String](INT_STR_CACHE_NAME).setSqlSchema("PUBLIC"),
            cacheConfiguration[JInteger, TestObject](INT_TEST_OBJ_CACHE_NAME).setSqlSchema("PUBLIC")
        )

        cfg.setClientMode(client)

        cfg.setIgniteInstanceName(igniteInstanceName)

        cfg
    }

    def cacheConfiguration[K, V](cacheName : String): CacheConfiguration[Object, Object] = {
        val ccfg = new CacheConfiguration[Object, Object]()

        ccfg.setBackups(1)

        ccfg.setName(cacheName)

        ccfg
    }
}
