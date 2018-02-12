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
            val res = spark.sqlContext.sql("SELECT name FROM person WHERE id = 2")

            val data = Tuple1("Jane Roe")

            checkQueryData(res, data)
        }

        it("Should correct filter with EqualToNullSafe Clause") {
            val res = spark.sqlContext.sql("SELECT id FROM person WHERE name = 'Jane Roe'")

            val data = Tuple1(2)

            checkQueryData(res, data)
        }

        it("Should correct filter with GreaterThen Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id > 3")

            val data = (
                (4, "Richard Miles"),
                (5, null))

            checkQueryData(res, data)
        }

        it("Should correct filter with GreaterThenOrEqual Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id >= 3")

            val data = (
                (3, "Mary Major"),
                (4, "Richard Miles"),
                (5, null))

            checkQueryData(res, data)
        }

        it("Should correct filter with LessThan Clause") {
            val res = spark.sqlContext.sql("SELECT name FROM person WHERE id < 2")

            val data = Tuple1("John Doe")

            checkQueryData(res, data)
        }

        it("Should correct filter with LessThanOrEqual Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id <= 2")

            val data = (
                (1, "John Doe"),
                (2, "Jane Roe"))

            checkQueryData(res, data)
        }

        it("Should correct filter with In Clause") {
            val res = spark.sqlContext.sql(
                "SELECT id FROM person WHERE name in ('Jane Roe', 'Richard Miles', 'Unknown Person')")

            val data = (2, 4)

            checkQueryData(res, data)
        }

        it("Should correct filter with IsNull Clause") {
            val res = spark.sqlContext.sql("SELECT id FROM person WHERE name IS NULL")

            val data = Tuple1(5)

            checkQueryData(res, data)
        }

        it("Should correct filter with IsNotNull Clause") {
            val res = spark.sqlContext.sql("SELECT id FROM person WHERE name IS NOT NULL")

            val data = (1, 2, 3, 4)

            checkQueryData(res, data)
        }

        it("Should correct filter with And Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id <= 4 AND name = 'Jane Roe'")

            val data = Tuple1(
                (2, "Jane Roe")
            )

            checkQueryData(res, data)
        }

        it("Should correct filter with Or Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE id = 2 OR name = 'John Doe'")

            val data = (
                (1, "John Doe"),
                (2, "Jane Roe"))

            checkQueryData(res, data)
        }

        it("Should correct filter with Not Clause") {
            val res = spark.sqlContext.sql("SELECT id FROM person WHERE NOT(name is null)")

            val data = (1, 2, 3, 4)

            checkQueryData(res, data)
        }

        it("Should correct filter with StringStartsWith Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE name LIKE 'J%'")

            val data = (
                (1, "John Doe"),
                (2, "Jane Roe"))

            checkQueryData(res, data)
        }

        it("Should correct filter with StringEndsWith Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE name LIKE '%e'")

            val data = (
                (1, "John Doe"),
                (2, "Jane Roe"))

            checkQueryData(res, data)
        }

        it("Should correct filter with StringContains Clause") {
            val res = spark.sqlContext.sql("SELECT id, name FROM person WHERE name LIKE '%M%'")

            val data = (
                (3, "Mary Major"),
                (4, "Richard Miles"))

            checkQueryData(res, data)
        }

        it("Should correct calculate MAX aggregate function") {
            val res = spark.sqlContext.sql("SELECT max(id) FROM person")

            val data = Tuple1(5)

            checkQueryData(res, data)
        }

        it("Should correct calculate MIN aggregate function") {
            val res = spark.sqlContext.sql("SELECT min(id) FROM person")

            val data = Tuple1(1)

            checkQueryData(res, data)
        }

        it("Should correct calculate AVG aggregate function") {
            val res = spark.sqlContext.sql("SELECT avg(id) FROM person WHERE id = 1 OR id = 2")

            val data = Tuple1(1.5D)

            checkQueryData(res, data)
        }

        it("Should correct calculate COUNT(*) aggregate function") {
            val res = spark.sqlContext.sql("SELECT count(*) FROM person")

            val data = Tuple1(5)

            checkQueryData(res, data)
        }

        it("Should correct execute GROUP BY query") {
            val res = spark.sqlContext.sql("SELECT city_id, count(1) FROM person GROUP BY city_id")

            val data = (
                (1, 1),
                (2, 3),
                (3, 1)
            )

            checkQueryData(res, data)
        }

        it("Should correct execute GROUP BY with HAVING query") {
            val res = spark.sqlContext.sql("SELECT city_id, count(1) FROM person GROUP BY city_id HAVING count(1) > 1")

            val data = Tuple1((2, 3))

            checkQueryData(res, data)
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
