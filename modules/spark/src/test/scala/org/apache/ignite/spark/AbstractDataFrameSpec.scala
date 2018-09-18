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
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest._
import java.lang.{Long ⇒ JLong}

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.internal.IgnitionEx.loadConfiguration
import org.apache.ignite.spark.AbstractDataFrameSpec.configuration
import org.apache.ignite.spark.impl.IgniteSQLAccumulatorRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.ignite.spark.AbstractDataFrameSpec._

import scala.annotation.meta.field
import scala.reflect.ClassTag

/**
  */
abstract class AbstractDataFrameSpec extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter
    with Assertions {
    var spark: SparkSession = _

    var client: Ignite = _

    private val NUM_SERVERS = 5

    override protected def beforeAll(): Unit = {
        for (i ← 0 to NUM_SERVERS)
            Ignition.start(configuration("grid-" + i, client = false))

        client = Ignition.getOrStart(configuration("client", client = true))

        createSparkSession()
    }

    override protected def afterAll(): Unit = {
        Ignition.stop("client", false)

        for (i ← 0 to NUM_SERVERS)
            Ignition.stop("grid-" + i, false)

        spark.close()
    }

    protected def createSparkSession(): Unit = {
        spark = SparkSession.builder()
            .appName("DataFrameSpec")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()
    }

    def createPersonTable2(client: Ignite, cacheName: String): Unit =
        createPersonTable0(client, cacheName, PERSON_TBL_NAME_2)

    def createPersonTable(client: Ignite, cacheName: String): Unit =
        createPersonTable0(client, cacheName, PERSON_TBL_NAME)

    private def createPersonTable0(client: Ignite, cacheName: String, tblName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            s"""
              | CREATE TABLE $tblName (
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

        val qry = new SqlFieldsQuery(s"INSERT INTO $tblName (id, name, city_id) values (?, ?, ?)")

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
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], "St. Petersburg")).getAll
    }

    def createEmployeeCache(client: Ignite, cacheName: String): Unit = {
        val ccfg = AbstractDataFrameSpec.cacheConfiguration[String, Employee](cacheName)

        val cache = client.getOrCreateCache(ccfg)

        cache.put("key1", Employee(1, "John Connor", 15, 0))
        cache.put("key2", Employee(2, "Sarah Connor", 32, 10000))
        cache.put("key3", Employee(3, "Arnold Schwarzenegger", 27, 1000))
    }

    def checkQueryData[T](res: DataFrame, expectedRes: Product)
        (implicit ord: T ⇒ Ordered[T]): Unit =
        checkQueryData(res, expectedRes, _.getAs[T](0))

    def checkQueryData[Ordered](res: DataFrame, expectedRes: Product, sorter: Row => Ordered)
        (implicit ord: Ordering[Ordered]): Unit = {
        val data = res.rdd.collect.sortBy(sorter)

        for(i ← 0 until expectedRes.productArity) {
            val row = data(i)

            if (row.size == 1)
                assert(row(0) == expectedRes.productElement(i), s"row[$i, 0] = ${row(0)} should be equal ${expectedRes.productElement(i)}")
            else {
                val expectedRow: Product = expectedRes.productElement(i).asInstanceOf[Product]

                assert(expectedRow.productArity == row.size, s"Rows size should be equal, but expected.size=${expectedRow.productArity} " +
                    s"and row.size=${row.size}")

                for (j ← 0 until expectedRow.productArity)
                    assert(row(j) == expectedRow.productElement(j), s"row[$i, $j] = ${row(j)} should be equal ${expectedRow.productElement(j)}")
            }
        }
    }
}

object AbstractDataFrameSpec {
    val TEST_CONFIG_FILE = "modules/spark/src/test/resources/ignite-spark-config.xml"

    val DEFAULT_CACHE = "cache1"

    val TEST_OBJ_TEST_OBJ_CACHE_NAME = "cache2"

    val EMPLOYEE_CACHE_NAME = "cache3"

    val PERSON_TBL_NAME = "person"

    val PERSON_TBL_NAME_2 = "person2"

    def configuration(igniteInstanceName: String, client: Boolean): IgniteConfiguration = {
        val cfg = loadConfiguration(TEST_CONFIG_FILE).get1()

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

    /**
      * @param df Data frame.
      * @param qry SQL Query.
      */
    def checkOptimizationResult(df: DataFrame, qry: String = ""): Unit = {
        df.explain(true)

        val plan = df.queryExecution.optimizedPlan

        val cnt = plan.collectLeaves.count {
            case LogicalRelation(relation: IgniteSQLAccumulatorRelation[_, _], _, _, _) ⇒
                if (qry != "")
                    assert(qry.toLowerCase == relation.acc.compileQuery().toLowerCase,
                        s"Generated query should be equal to expected.\nexpected  - $qry\ngenerated - ${relation.acc.compileQuery()}")

                true

            case _ ⇒
                false
        }

        assert(cnt != 0, s"Plan should contains IgniteSQLAccumulatorRelation")
    }

    /**
      * Enclose some closure, so it doesn't on outer object(default scala behaviour) while serializing.
      */
    def enclose[E, R](enclosed: E)(func: E => R): R = func(enclosed)
}

case class Employee (
    @(QuerySqlField @field)(index = true) id: Long,

    @(QuerySqlField @field) name: String,

    age: Int,

    @(QuerySqlField @field)(index = true, descending = true) salary: Float
) extends Serializable { }
