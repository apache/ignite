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

import com.google.common.collect.Iterators
import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Tests to check all kinds of SQL queries from Spark SQL engine to Ignite SQL table.
  */
@RunWith(classOf[JUnitRunner])
class IgniteSQLDataFrameSpec extends AbstractDataFrameSpec {
    var personDataFrame: DataFrame = _

    describe("DataFrame for a Ignite SQL table") {
        it("Should correct filter with EqualTo Clause") {
            val res = spark.sqlContext.sql("SELECT name FROM person WHERE id = 2").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[String]("name") should equal("Jane Roe")
        }

        it("Should correct filter with EqualToNullSafe Clause") {
            val res = spark.sqlContext.sql("SELECT id FROM person WHERE name = 'Jane Roe'").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[Long]("id") should equal(2)
        }

        it("Should correct filter with GreaterThen Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id > 3").rdd

            res.count should equal(2)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[String]("name") should equal("Richard Miles")
            persons(1).getAs[String]("name") should equal(null)
        }

        it("Should correct filter with GreaterThenOrEqual Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id >= 3").rdd

            res.count should equal(3)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[String]("name") should equal("Mary Major")
            persons(1).getAs[String]("name") should equal("Richard Miles")
            persons(2).getAs[String]("name") should equal(null)
        }

        it("Should correct filter with LessThan Clause") {
            val res = spark.sqlContext.sql("SELECT name FROM person WHERE id < 2").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[String]("name") should equal("John Doe")
        }

        it("Should correct filter with LessThanOrEqual Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id <= 2").rdd

            res.count should equal(2)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[String]("name") should equal("John Doe")
            persons(1).getAs[String]("name") should equal("Jane Roe")
        }

        it("Should correct filter with In Clause") {
            val res = spark.sqlContext.sql(
                "SELECT id FROM person WHERE name in ('Jane Roe', 'Richard Miles', 'Unknown Person')").rdd

            res.count should equal(2)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[Long]("id") should equal(2L)
            persons(1).getAs[Long]("id") should equal(4L)
        }

        it("Should correct filter with IsNull Clause") {
            val res = spark.sqlContext.sql(
                "SELECT id FROM person WHERE name IS NULL").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[Long]("id") should equal(5L)
        }

        it("Should correct filter with IsNotNull Clause") {
            val res = spark.sqlContext.sql(
                "SELECT id FROM person WHERE name IS NOT NULL").rdd

            res.count should equal(4)

            res.collect.map(r ⇒ r.getAs[Long]("id")).sorted should equal(Array(1, 2, 3, 4))

        }

        it("Should correct filter with And Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id <= 4 AND name = 'Jane Roe'").rdd

            res.count should equal(1)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[Long]("id") should equal(2)
            persons(0).getAs[String]("name") should equal("Jane Roe")
        }

        it("Should correct filter with Or Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id = 2 OR name = 'John Doe'").rdd

            res.count should equal(2)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[Long]("id") should equal(1)
            persons(0).getAs[String]("name") should equal("John Doe")

            persons(1).getAs[Long]("id") should equal(2)
            persons(1).getAs[String]("name") should equal("Jane Roe")
        }

        it("Should correct filter with Not Clause") {
            val res = spark.sqlContext.sql("SELECT id FROM person WHERE NOT(name is null)").rdd

            res.count should equal(4)

            res.collect.map(r ⇒ r.getAs[Long]("id")).sorted should equal(Array(1, 2, 3, 4))
        }

        it("Should correct filter with StringStartsWith Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE name LIKE 'J%'").rdd

            res.count should equal(2)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[Long]("id") should equal(1)
            persons(0).getAs[String]("name") should equal("John Doe")

            persons(1).getAs[Long]("id") should equal(2)
            persons(1).getAs[String]("name") should equal("Jane Roe")
        }

        it("Should correct filter with StringEndsWith Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE name LIKE '%e'").rdd

            res.count should equal(2)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[Long]("id") should equal(1)
            persons(0).getAs[String]("name") should equal("John Doe")

            persons(1).getAs[Long]("id") should equal(2)
            persons(1).getAs[String]("name") should equal("Jane Roe")
        }

        it("Should correct filter with StringContains Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE name LIKE '%M%'").rdd

            res.count should equal(2)

            val persons = res.collect.sortBy(_.getAs[Long]("id"))

            persons(0).getAs[Long]("id") should equal(3)
            persons(0).getAs[String]("name") should equal("Mary Major")

            persons(1).getAs[Long]("id") should equal(4)
            persons(1).getAs[String]("name") should equal("Richard Miles")
        }

        it("Should correct calculate MAX aggregate function") {
            val res = spark.sqlContext.sql("SELECT max(id) FROM person").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[Long]("max(id)") should equal(5)
        }

        it("Should correct calculate MIN aggregate function") {
            val res = spark.sqlContext.sql("SELECT min(id) FROM person").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[Long]("min(id)") should equal(1)
        }

        it("Should correct calculate AVG aggregate function") {
            val res = spark.sqlContext.sql("SELECT avg(id) FROM person WHERE id = 1 OR id = 2").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[Double]("avg(id)") should equal(1.5D)
        }

        it("Should correct calculate COUNT(*) aggregate function") {
            val res = spark.sqlContext.sql("SELECT count(*) FROM person").rdd

            res.count should equal(1)

            val persons = res.collect

            persons(0).getAs[Long]("count(1)") should equal(5)
        }

        it("Should correct execute GROUP BY query") {
            val res = spark.sqlContext.sql("SELECT count(1), city_id FROM person GROUP BY city_id").rdd

            res.count should equal(3)

            val persons = res.collect.sortBy(_.getAs[Long]("city_id"))

            persons(0).getAs[Long]("city_id") should equal(1)
            persons(0).getAs[Long]("count(1)") should equal(1)

            persons(1).getAs[Long]("city_id") should equal(2)
            persons(1).getAs[Long]("count(1)") should equal(3)

            persons(2).getAs[Long]("city_id") should equal(3)
            persons(2).getAs[Long]("count(1)") should equal(1)
        }

        it("Should correct execute GROUP BY with HAVING query") {
            val res = spark.sqlContext.sql("SELECT count(1), city_id FROM person GROUP BY city_id HAVING count(1) > 1").rdd

            res.count should equal(1)

            val persons = res.collect.sortBy(_.getAs[Long]("city_id"))

            persons(0).getAs[Long]("city_id") should equal(2)
            persons(0).getAs[Long]("count(1)") should equal(3)
        }

        it("should use the schema name where one is specified") {
            // `employeeCache1` is created in the schema matching the name of the cache, ie. `employeeCache1`.
            createEmployeeCache(client, "employeeCache1")

            spark.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "employee")
                .option(OPTION_SCHEMA, "employeeCache1")
                .load()
                .createOrReplaceTempView("employeeWithSchema")

            // `employeeCache2` is created with a custom schema of `employeeSchema`.
            createEmployeeCache(client, "employeeCache2", Some("employeeSchema"))

            Iterators.size(client.cache("employeeCache2").iterator()) should equal(3)

            // Remove a value from `employeeCache2` so that we know whether the select statement picks up the
            // correct cache, ie. it should now have 2 values compared to 3 in `employeeCache1`.
            client.cache("employeeCache2").remove("key1") shouldBe true

            spark.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "employee")
                .option(OPTION_SCHEMA, "employeeSchema")
                .load()
                .createOrReplaceTempView("employeeWithSchema2")

            val res = spark.sqlContext.sql("SELECT id FROM employeeWithSchema").rdd

            res.count should equal(3)

            val res2 = spark.sqlContext.sql("SELECT id FROM employeeWithSchema2").rdd

            res2.count should equal(2)
        }
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createPersonTable(client, "cache1")

        personDataFrame = spark.read
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, "person")
            .load()

        personDataFrame.createOrReplaceTempView("person")
    }
}
