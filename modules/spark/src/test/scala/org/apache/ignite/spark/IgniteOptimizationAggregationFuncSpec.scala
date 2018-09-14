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

import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.lang.{Double ⇒ JDouble, Long ⇒ JLong}

import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, checkOptimizationResult, enclose}
import org.apache.spark.sql.ignite.IgniteSparkSession

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationAggregationFuncSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Supported optimized aggregation functions") {
        it("COUNT") {
            val df = igniteSession.sql("SELECT count(*) FROM numbers")

            checkOptimizationResult(df, "SELECT count(1) FROM numbers")

            val data = Tuple1(21)

            checkQueryData(df, data)
        }

        it("AVG - DECIMAL") {
            //TODO: write me
        }

        it("AVG - DOUBLE") {
            val df = igniteSession.sql("SELECT AVG(val) FROM numbers WHERE id <= 3")

            checkOptimizationResult(df, "SELECT AVG(val) FROM numbers WHERE id <= 3")

            val data = Tuple1(.5)

            checkQueryData(df, data)
        }

        it("MIN - DOUBLE") {
            val df = igniteSession.sql("SELECT MIN(val) FROM numbers")

            checkOptimizationResult(df, "SELECT MIN(val) FROM numbers")

            val data = Tuple1(-1.0)

            checkQueryData(df, data)
        }

        it("MAX - DOUBLE") {
            val df = igniteSession.sql("SELECT MAX(val) FROM numbers")

            checkOptimizationResult(df, "SELECT MAX(val) FROM numbers")

            val data = Tuple1(180.0)

            checkQueryData(df, data)
        }

        it("SUM - DOUBLE") {
            val df = igniteSession.sql("SELECT SUM(val) FROM numbers WHERE id <= 3")

            checkOptimizationResult(df, "SELECT SUM(val) FROM numbers WHERE id <= 3")

            val data = Tuple1(1.5)

            checkQueryData(df, data)
        }

        it("SUM - DECIMAL - 1") {
            val df = igniteSession.sql("SELECT SUM(decimal_val) FROM numbers WHERE id IN (18, 19, 20)")

            checkOptimizationResult(df, "SELECT SUM(decimal_val) FROM numbers WHERE id IN (18, 19, 20)")

            df.printSchema()

            val data = Tuple1(new java.math.BigDecimal(10.5).setScale(3))

            checkQueryData(df, data)
        }

        it("SUM - DECIMAL - 2") {
            val df = igniteSession.sql("SELECT SUM(decimal_val) FROM numbers WHERE id IN (18, 19, 20, 21)")

            checkOptimizationResult(df, "SELECT SUM(decimal_val) FROM numbers WHERE id IN (18, 19, 20, 21)")

            val data = Tuple1(new java.math.BigDecimal(15).setScale(3))

            checkQueryData(df, data)
        }

        it("SUM - LONG") {
            val df = igniteSession.sql("SELECT SUM(int_val) FROM numbers WHERE id in (15, 16, 17)")

            checkOptimizationResult(df, "SELECT CAST(SUM(int_val) AS BIGINT) as \"SUM(int_val)\" " +
                "FROM numbers WHERE id in (15, 16, 17)")

            val data = Tuple1(6L)

            checkQueryData(df, data)
        }
    }

    def createNumberTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE numbers (
              |    id LONG,
              |    val DOUBLE,
              |    int_val LONG,
              |    decimal_val DECIMAL(5, 5),
              |    PRIMARY KEY (id)) WITH "backups=1"
            """.stripMargin)).getAll

        var qry = new SqlFieldsQuery("INSERT INTO numbers (id, val) values (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], .0.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], .5.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], 1.0.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], 2.0.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(5L.asInstanceOf[JLong], 4.0.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(6L.asInstanceOf[JLong], -0.5.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(7L.asInstanceOf[JLong], -1.0.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(8L.asInstanceOf[JLong], 42.0.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(9L.asInstanceOf[JLong], .51.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(10L.asInstanceOf[JLong], .49.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(11L.asInstanceOf[JLong], 100.0.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(12L.asInstanceOf[JLong], (Math.E*Math.E).asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(13L.asInstanceOf[JLong], Math.PI.asInstanceOf[JDouble])).getAll
        cache.query(qry.setArgs(14L.asInstanceOf[JLong], 180.0.asInstanceOf[JDouble])).getAll

        qry = new SqlFieldsQuery("INSERT INTO numbers (id, int_val) values (?, ?)")

        cache.query(qry.setArgs(15L.asInstanceOf[JLong], 1L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(16L.asInstanceOf[JLong], 2L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(17L.asInstanceOf[JLong], 3L.asInstanceOf[JLong])).getAll

        qry = new SqlFieldsQuery("INSERT INTO numbers (id, decimal_val) values (?, ?)")

        cache.query(qry.setArgs(18L.asInstanceOf[JLong], new java.math.BigDecimal(2.5))).getAll
        cache.query(qry.setArgs(19L.asInstanceOf[JLong], new java.math.BigDecimal(3.5))).getAll
        cache.query(qry.setArgs(20L.asInstanceOf[JLong], new java.math.BigDecimal(4.5))).getAll
        cache.query(qry.setArgs(21L.asInstanceOf[JLong], new java.math.BigDecimal(4.5))).getAll
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createNumberTable(client, DEFAULT_CACHE)

        val configProvider = enclose(null) (x ⇒ () ⇒ {
            val cfg = IgnitionEx.loadConfiguration(TEST_CONFIG_FILE).get1()

            cfg.setClientMode(true)

            cfg.setIgniteInstanceName("client-2")

            cfg
        })

        igniteSession = IgniteSparkSession.builder()
            .config(spark.sparkContext.getConf)
            .igniteConfigProvider(configProvider)
            .getOrCreate()
    }
}
