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

import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, checkOptimizationResult, enclose}
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_TABLE}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.{Dataset, Row}

import scala.annotation.meta.field

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Optimized queries") {
        it("SELECT name as city_name FROM city") {
            val df = igniteSession.sql("SELECT name as city_name FROM city")

            checkOptimizationResult(df, "SELECT name as city_name FROM city")
        }

        it("SELECT count(*) as city_count FROM city") {
            val df = igniteSession.sql("SELECT count(1) as city_count FROM city")

            checkOptimizationResult(df, "SELECT count(1) as city_count FROM city")
        }

        it("SELECT count(*), city_id FROM person p GROUP BY city_id") {
            val df = igniteSession.sql("SELECT city_id, count(*) FROM person GROUP BY city_id")

            checkOptimizationResult(df, "SELECT city_id, count(1) FROM person GROUP BY city_id")

            val data = (
                (1, 1),
                (2, 3),
                (3, 1)
            )

            checkQueryData(df, data)
        }

        it("SELECT id, name FROM person WHERE id > 3 ORDER BY id") {
            val df = igniteSession.sql("SELECT id, name FROM person WHERE id > 3 ORDER BY id")

            checkOptimizationResult(df, "SELECT id, name FROM person WHERE id > 3 ORDER BY id")

            val data = (
                (4, "Richard Miles"),
                (5, null))

            checkQueryData(df, data)
        }

        it("SELECT id, name FROM person WHERE id > 3 ORDER BY id DESC") {
            val df = igniteSession.sql("SELECT id, name FROM person WHERE id > 3 ORDER BY id DESC")

            checkOptimizationResult(df, "SELECT id, name FROM person WHERE id > 3 ORDER BY id DESC")

            val data = (
                (5, null),
                (4, "Richard Miles"))

            checkQueryData(df, data, -_.getAs[Long]("id"))
        }

        it("SELECT id, test_reverse(name) FROM city ORDER BY id") {
            igniteSession.udf.register("test_reverse", (str: String) ⇒ str.reverse)

            val df = igniteSession.sql("SELECT id, test_reverse(name) FROM city ORDER BY id")

            checkOptimizationResult(df, "SELECT name, id FROM city")

            val data = (
                (1, "Forest Hill".reverse),
                (2, "Denver".reverse),
                (3, "St. Petersburg".reverse),
                (4, "St. Petersburg".reverse))

            checkQueryData(df, data)
        }

        it("SELECT count(*), city_id FROM person p GROUP BY city_id HAVING count(*) > 1") {
            val df = igniteSession.sql("SELECT city_id, count(*) FROM person p GROUP BY city_id HAVING count(*) > 1")

            checkOptimizationResult(df, "SELECT city_id, count(1) FROM person GROUP BY city_id HAVING count(1) > 1")

            val data = Tuple1(
                (2, 3))

            checkQueryData(df, data)
        }

        it("SELECT id FROM city HAVING id > 1") {
            val df = igniteSession.sql("SELECT id FROM city HAVING id > 1")

            checkOptimizationResult(df, "SELECT id FROM city WHERE id > 1")

            val data = (2, 3, 4)

            checkQueryData(df, data)
        }

        it("SELECT DISTINCT name FROM city ORDER BY name") {
            val df = igniteSession.sql("SELECT DISTINCT name FROM city ORDER BY name")

            checkOptimizationResult(df, "SELECT name FROM city GROUP BY name ORDER BY name")

            val data = ("Denver", "Forest Hill", "St. Petersburg")

            checkQueryData(df, data)
        }

        it("SELECT id, name FROM city ORDER BY id, name") {
            val df = igniteSession.sql("SELECT id, name FROM city ORDER BY id, name")

            checkOptimizationResult(df, "SELECT id, name FROM city ORDER BY id, name")

            val data = (
                (1, "Forest Hill"),
                (2, "Denver"),
                (3, "St. Petersburg"),
                (4, "St. Petersburg"))

            checkQueryData(df, data)
        }

        it("SELECT id, name FROM city WHERE id > 1 ORDER BY id") {
            val df = igniteSession.sql("SELECT id, name FROM city WHERE id > 1 ORDER BY id")

            checkOptimizationResult(df, "SELECT id, name FROM city WHERE id > 1 ORDER BY id")

            val data = (
                (2, "Denver"),
                (3, "St. Petersburg"),
                (4, "St. Petersburg"))

            checkQueryData(df, data)
        }

        it("SELECT count(*) FROM city") {
            val df = igniteSession.sql("SELECT count(*) FROM city")

            checkOptimizationResult(df, "SELECT count(1) FROM city")

            val data = Tuple1(4)

            checkQueryData(df, data)
        }

        it("SELECT count(DISTINCT name)  FROM city") {
            val df = igniteSession.sql("SELECT count(DISTINCT name) FROM city")

            checkOptimizationResult(df, "SELECT count(DISTINCT name) FROM city")

            val data = Tuple1(3)

            checkQueryData(df, data)
        }

        it("SELECT id FROM city LIMIT 2") {
            val df = igniteSession.sql("SELECT id FROM city LIMIT 2")

            checkOptimizationResult(df, "SELECT id FROM city LIMIT 2")

            val data = (1, 2)

            checkQueryData(df, data)
        }

        it("SELECT CAST(id AS STRING) FROM city") {
            val df = igniteSession.sql("SELECT CAST(id AS STRING) FROM city")

            checkOptimizationResult(df, "SELECT CAST(id AS varchar) as id FROM city")

            val data = ("1", "2", "3", "4")

            checkQueryData(df, data)
        }

        it("SELECT SQRT(id) FROM city WHERE id = 4 OR id = 1") {
            val df = igniteSession.sql("SELECT SQRT(id) FROM city WHERE id = 4 OR id = 1")

            checkOptimizationResult(df,
                "SELECT SQRT(cast(id as double)) FROM city WHERE id = 4 OR id = 1")

            val data = (1, 2)

            checkQueryData(df, data)
        }

        it("SELECT CONCAT(id, \" - this is ID\") FROM city") {
            val df = igniteSession.sql("SELECT CONCAT(id, \" - this is ID\") FROM city")

            checkOptimizationResult(df,
                "SELECT CONCAT(cast(id AS VARCHAR), ' - this is ID') as \"CONCAT(cast(id AS STRING),  - this is ID)\" " +
                    "FROM city")

            val data = (
                "1 - this is ID",
                "2 - this is ID",
                "3 - this is ID",
                "4 - this is ID")

            checkQueryData(df, data)
        }

        it("SELECT id FROM city WHERE CONCAT(id, \" - this is ID\") = \"1 - this is ID\"") {
            val df = igniteSession.sql("SELECT id FROM city WHERE CONCAT(id, \" - this is ID\") = \"1 - this is ID\"")

            checkOptimizationResult(df,
                "SELECT id FROM city WHERE CONCAT(CAST(id AS VARCHAR), ' - this is ID') = '1 - this is ID'")

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it("Should optimize union") {
            val union = readTable("JPerson").union(readTable("JPerson2"))

            val data = (
                (1, "JPerson-1"),
                (2, "JPerson-2"))

            checkQueryData(union, data)
        }

        it("Should optimize null column") {
            val p = readTable("JPerson").withColumn("nullColumn", lit(null).cast(StringType))

            val data = Tuple1(
                (1, "JPerson-1", null))

            checkQueryData(p, data)
        }
    }

    describe("Not Optimized Queries") {
        it("SELECT id, name FROM json_cities") {
            val citiesDataFrame = igniteSession.read.json(
                resolveIgnitePath("modules/spark/src/test/resources/cities.json").getAbsolutePath)

            citiesDataFrame.createOrReplaceTempView("JSON_CITIES")

            val df = igniteSession.sql("SELECT id, name FROM json_cities")

            val data = (
                (1, "Forest Hill"),
                (2, "Denver"),
                (3, "St. Petersburg"))

            checkQueryData(df, data)
        }

        it("SELECT id, test_reverse(name) tr FROM city WHERE test_reverse(name) = 'revneD' ORDER BY id") {
            val df = igniteSession.sql("SELECT id, test_reverse(name) tr " +
                "FROM city WHERE test_reverse(name) = 'revneD' ORDER BY id")

            checkOptimizationResult(df)
        }

        it("SELECT id, test_reverse(name) tr FROM city WHERE test_reverse(name) = 'revneD' and id > 0 ORDER BY id") {
            val df = igniteSession.sql("SELECT id, test_reverse(name) tr " +
                "FROM city WHERE test_reverse(name) = 'revneD' and id > 0 ORDER BY id")

            checkOptimizationResult(df)
        }

        it("SELECT id, test_reverse(name) tr FROM city ORDER BY tr") {
            val df = igniteSession.sql("SELECT id, test_reverse(name) tr FROM city ORDER BY tr")

            checkOptimizationResult(df)
        }

        it("SELECT count(*), test_reverse(name) tr FROM city GROUP BY test_reverse(name)") {
            val df = igniteSession.sql("SELECT count(*), test_reverse(name) tr FROM city GROUP BY test_reverse(name)")

            checkOptimizationResult(df)
        }
    }

    def readTable(tblName: String): Dataset[Row] =
        igniteSession.read
            .format(FORMAT_IGNITE)
            .option(OPTION_TABLE, tblName)
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .load

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createPersonTable(client, DEFAULT_CACHE)

        createCityTable(client, DEFAULT_CACHE)

        val p = client.getOrCreateCache(new CacheConfiguration[Long, JPerson]()
            .setName("P")
            .setSqlSchema("SQL_PUBLIC")
            .setIndexedTypes(classOf[Long], classOf[JPerson]))

        p.put(1L, new JPerson(1L, "JPerson-1"))

        val p2 = client.getOrCreateCache(new CacheConfiguration[Long, JPerson2]()
            .setName("P2")
            .setSqlSchema("SQL_PUBLIC")
            .setIndexedTypes(classOf[Long], classOf[JPerson2]))

        p2.put(1L, new JPerson2(2L, "JPerson-2"))

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

        igniteSession.udf.register("test_reverse", (str: String) ⇒ str.reverse)
    }

    case class JPerson(
        @(QuerySqlField @field) id: Long,
        @(QuerySqlField @field)(index = true) name: String)

    case class JPerson2(
        @(QuerySqlField @field) id: Long,
        @(QuerySqlField @field)(index = true) name: String)
}
