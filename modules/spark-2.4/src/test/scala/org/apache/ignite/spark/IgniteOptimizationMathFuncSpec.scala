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
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, checkOptimizationResult, enclose}
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.lang.{Double ⇒ JDouble, Long ⇒ JLong}

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationMathFuncSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Supported optimized string functions") {
        it("ABS") {
            val df = igniteSession.sql("SELECT ABS(val) FROM numbers WHERE id = 6")

            checkOptimizationResult(df, "SELECT ABS(val) FROM numbers WHERE id = 6")

            val data = Tuple1(.5)

            checkQueryData(df, data)
        }

        it("ACOS") {
            val df = igniteSession.sql("SELECT ACOS(val) FROM numbers WHERE id = 7")

            checkOptimizationResult(df, "SELECT ACOS(val) FROM numbers WHERE id = 7")

            val data = Tuple1(Math.PI)

            checkQueryData(df, data)
        }

        it("ASIN") {
            val df = igniteSession.sql("SELECT ASIN(val) FROM numbers WHERE id = 7")

            checkOptimizationResult(df, "SELECT ASIN(val) FROM numbers WHERE id = 7")

            val data = Tuple1(-Math.PI/2)

            checkQueryData(df, data)
        }

        it("ATAN") {
            val df = igniteSession.sql("SELECT ATAN(val) FROM numbers WHERE id = 7")

            checkOptimizationResult(df, "SELECT ATAN(val) FROM numbers WHERE id = 7")

            val data = Tuple1(-Math.PI/4)

            checkQueryData(df, data)
        }

        it("COS") {
            val df = igniteSession.sql("SELECT COS(val) FROM numbers WHERE id = 1")

            checkOptimizationResult(df, "SELECT COS(val) FROM numbers WHERE id = 1")

            val data = Tuple1(1.0)

            checkQueryData(df, data)
        }

        it("SIN") {
            val df = igniteSession.sql("SELECT SIN(val) FROM numbers WHERE id = 1")

            checkOptimizationResult(df, "SELECT SIN(val) FROM numbers WHERE id = 1")

            val data = Tuple1(.0)

            checkQueryData(df, data)
        }

        it("TAN") {
            val df = igniteSession.sql("SELECT TAN(val) FROM numbers WHERE id = 1")

            checkOptimizationResult(df, "SELECT TAN(val) FROM numbers WHERE id = 1")

            val data = Tuple1(.0)

            checkQueryData(df, data)
        }

        it("COSH") {
            val df = igniteSession.sql("SELECT COSH(val) FROM numbers WHERE id = 1")

            checkOptimizationResult(df, "SELECT COSH(val) FROM numbers WHERE id = 1")

            val data = Tuple1(1.0)

            checkQueryData(df, data)
        }

        it("SINH") {
            val df = igniteSession.sql("SELECT SINH(val) FROM numbers WHERE id = 1")

            checkOptimizationResult(df, "SELECT SINH(val) FROM numbers WHERE id = 1")

            val data = Tuple1(.0)

            checkQueryData(df, data)
        }

        it("TANH") {
            val df = igniteSession.sql("SELECT TANH(val) FROM numbers WHERE id = 1")

            checkOptimizationResult(df, "SELECT TANH(val) FROM numbers WHERE id = 1")

            val data = Tuple1(.0)

            checkQueryData(df, data)
        }

        it("ATAN2") {
            val df = igniteSession.sql("SELECT ATAN2(val, 0.0) FROM numbers WHERE id = 1")

            checkOptimizationResult(df, "SELECT ATAN2(val, 0.0) AS \"ATAN2(val, CAST(0.0 AS DOUBLE))\" " +
                "FROM numbers WHERE id = 1")

            val data = Tuple1(.0)

            checkQueryData(df, data)
        }

        it("MOD") {
            val df = igniteSession.sql("SELECT val % 9 FROM numbers WHERE id = 8")

            checkOptimizationResult(df, "SELECT val % 9.0 as \"(val % CAST(9 AS DOUBLE))\" " +
                "FROM numbers WHERE id = 8")

            val data = Tuple1(6.0)

            checkQueryData(df, data)
        }

        it("CEIL") {
            val df = igniteSession.sql("SELECT CEIL(val) FROM numbers WHERE id = 2")

            checkOptimizationResult(df, "SELECT CAST(CEIL(val) AS LONG) as \"CEIL(val)\" " +
                "FROM numbers WHERE id = 2")

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it("ROUND") {
        val df = igniteSession.sql("SELECT id, ROUND(val) FROM numbers WHERE  id IN (2, 9, 10)")

            checkOptimizationResult(df, "SELECT id, ROUND(val, 0) FROM numbers WHERE id IN (2, 9, 10)")

            val data = (
                (2, 1.0),
                (9, 1.0),
                (10, 0.0))

            checkQueryData(df, data)
        }

        it("FLOOR") {
            val df = igniteSession.sql("SELECT FLOOR(val) FROM numbers WHERE id = 2")

            checkOptimizationResult(df, "SELECT CAST(FLOOR(val) AS LONG) as \"FLOOR(val)\" FROM numbers " +
                "WHERE id = 2")

            val data = Tuple1(0)

            checkQueryData(df, data)
        }

        it("POWER") {
            val df = igniteSession.sql("SELECT POWER(val, 3) FROM numbers WHERE id = 4")

            checkOptimizationResult(df, "SELECT POWER(val, 3.0) as \"POWER(val, CAST(3 AS DOUBLE))\" FROM numbers " +
                "WHERE id = 4")

            val data = Tuple1(8.0)

            checkQueryData(df, data)
        }

        it("EXP") {
            val df = igniteSession.sql("SELECT id, EXP(val) FROM numbers WHERE id IN (1, 3)")

            checkOptimizationResult(df, "SELECT id, EXP(val) FROM numbers WHERE id IN (1, 3)")

            val data = (
                (1, 1),
                (3, Math.E))

            checkQueryData(df, data)
        }

        it("LOG") {
            val df = igniteSession.sql("SELECT LOG(val) FROM numbers WHERE id = 12")

            checkOptimizationResult(df, "SELECT LOG(val) as \"LOG(E(), val)\" FROM numbers " +
                "WHERE id = 12")

            val data = Tuple1(2.0)

            checkQueryData(df, data)
        }

        it("LOG10") {
            val df = igniteSession.sql("SELECT LOG10(val) FROM numbers WHERE id = 11")

            checkOptimizationResult(df, "SELECT LOG10(val) FROM numbers WHERE id = 11")

            val data = Tuple1(2.0)

            checkQueryData(df, data)
        }

        it("DEGREES") {
            val df = igniteSession.sql("SELECT DEGREES(val) FROM numbers WHERE id = 13")

            checkOptimizationResult(df, "SELECT DEGREES(val) FROM numbers WHERE id = 13")

            val data = Tuple1(180.0)

            checkQueryData(df, data)
        }

        it("RADIANS") {
            val df = igniteSession.sql("SELECT RADIANS(val) FROM numbers WHERE id = 14")

            checkOptimizationResult(df, "SELECT RADIANS(val) FROM numbers WHERE id = 14")

            val data = Tuple1(Math.PI)

            checkQueryData(df, data)
        }

        it("BITAND") {
            val df = igniteSession.sql("SELECT int_val&1 FROM numbers WHERE id = 15")

            checkOptimizationResult(df, "SELECT BITAND(int_val, 1) as \"(int_val & CAST(1 AS BIGINT))\" FROM numbers " +
                "WHERE id = 15")

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it("BITOR") {
            val df = igniteSession.sql("SELECT int_val|1 FROM numbers WHERE id = 16")

            checkOptimizationResult(df, "SELECT BITOR(int_val, 1) as \"(int_val | CAST(1 AS BIGINt))\" FROM numbers " +
                "WHERE id = 16")

            val data = Tuple1(3)

            checkQueryData(df, data)
        }

        it("BITXOR") {
            val df = igniteSession.sql("SELECT int_val^1 FROM numbers WHERE id = 17")

            checkOptimizationResult(df, "SELECT BITXOR(int_val, 1) AS \"(int_val ^ CAST(1 AS BIGINT))\" FROM numbers " +
                "WHERE id = 17")

            val data = Tuple1(2)

            checkQueryData(df, data)
        }

        it("RAND") {
            val df = igniteSession.sql("SELECT id, RAND(1) FROM numbers WHERE id = 17")

            checkOptimizationResult(df, "SELECT id, RAND(1) FROM numbers WHERE id = 17")

            val data = df.rdd.collect

            assert(data(0).getAs[JLong]("id") == 17L)
            assert(data(0).getAs[JDouble]("rand(1)") != null)
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
