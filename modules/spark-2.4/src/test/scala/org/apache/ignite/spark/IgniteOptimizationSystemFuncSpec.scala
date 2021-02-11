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
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.lang.{Double ⇒ JDouble, Long ⇒ JLong}

import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, checkOptimizationResult, enclose}

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationSystemFuncSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Supported optimized system functions") {
        it("COALESCE") {
            val df = igniteSession.sql("SELECT COALESCE(int_val1, int_val2) FROM numbers WHERE id IN (1, 2, 3)")

            checkOptimizationResult(df, "SELECT COALESCE(int_val1, int_val2) FROM numbers WHERE id IN (1, 2, 3)")

            val data = (1, 2, 3)

            checkQueryData(df, data)
        }

        it("GREATEST") {
            val df = igniteSession.sql("SELECT GREATEST(int_val1, int_val2) FROM numbers WHERE id IN (4, 5)")

            checkOptimizationResult(df, "SELECT GREATEST(int_val1, int_val2) FROM numbers WHERE id IN (4, 5)")

            val data = (4, 6)

            checkQueryData(df, data)
        }

        it("LEAST") {
            val df = igniteSession.sql("SELECT LEAST(int_val1, int_val2) FROM numbers WHERE id IN (4, 5)")

            checkOptimizationResult(df, "SELECT LEAST(int_val1, int_val2) FROM numbers WHERE id IN (4, 5)")

            val data = (3, 5)

            checkQueryData(df, data)
        }

        it("IFNULL") {
            val df = igniteSession.sql("SELECT IFNULL(int_val1, int_val2) FROM numbers WHERE id IN (1, 2, 3)")

            checkOptimizationResult(df, "SELECT COALESCE(int_val1, int_val2) as \"ifnull(default.numbers.`int_val1`, default.numbers.`int_val2`)\" FROM numbers WHERE id IN (1, 2, 3)")

            val data = (1, 2, 3)

            checkQueryData(df, data)
        }

        it("NULLIF") {
            val df = igniteSession.sql("SELECT id, NULLIF(int_val1, int_val2) FROM numbers WHERE id IN (6, 7)")

            checkOptimizationResult(df)

            val data = (
                (6, null),
                (7, 8))

            checkQueryData(df, data)
        }

        it("NVL2") {
            val df = igniteSession.sql("SELECT id, NVL2(int_val1, 'not null', 'null') FROM numbers WHERE id IN (1, 2, 3)")

            checkOptimizationResult(df)

            val data = (
                (1, "not null"),
                (2, "null"),
                (3, "not null"))

            checkQueryData(df, data)
        }
    }

    def createNumberTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE numbers (
              |    id LONG,
              |    int_val1 LONG,
              |    int_val2 LONG,
              |    PRIMARY KEY (id)) WITH "backups=1"
            """.stripMargin)).getAll


        val qry = new SqlFieldsQuery("INSERT INTO numbers (id, int_val1, int_val2) values (?, ?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], 1L.asInstanceOf[JLong], null)).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], null, 2L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], 3L.asInstanceOf[JLong], null)).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], 3L.asInstanceOf[JLong], 4L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(5L.asInstanceOf[JLong], 6L.asInstanceOf[JLong], 5L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(6L.asInstanceOf[JLong], 7L.asInstanceOf[JLong], 7L.asInstanceOf[JLong])).getAll
        cache.query(qry.setArgs(7L.asInstanceOf[JLong], 8L.asInstanceOf[JLong], 9L.asInstanceOf[JLong])).getAll
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
