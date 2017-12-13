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

import java.lang.{Byte ⇒ JByte, Integer ⇒ JInteger, Long ⇒ JLong, Short ⇒ JShort, String ⇒ JString}
import java.sql.Timestamp
import java.util.Date

import org.apache.ignite.spark.AbstractDataFrameSpec.{INT_TEST_OBJ_CACHE_NAME, TEST_CONFIG_FILE}
import org.apache.ignite.spark.IgniteDataFrameOptions._
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Tests to check all kinds of SQL queries from Spark SQL engine to Ignite key-value cache.
  */
@RunWith(classOf[JUnitRunner])
class IgniteCacheDataFrameSpec extends AbstractDataFrameSpec {
    var intTestObjDataFrame: DataFrame = _

    describe("DataFrame for a Ignite key-value cache") {
        it("Should correct filter with EqualTo Clause") {
            val res = spark.sqlContext.sql("SELECT `value.stringF` FROM int_test_obj WHERE key = 10").rdd

            res.count should equal (1)

            val data = res.collect

            data(0).getAs[String]("value.stringF") should equal ("test string")
        }

        it("Should correct filter with EqualToNullSafe Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.stringF` = 'test string'").rdd

            res.count should equal (1)

            val data = res.collect

            data(0).getAs[JInteger]("key") should equal (10)
        }

        it("Should correct filter with GreaterThen Clause") {
            val res = spark.sqlContext.sql("SELECT `value.stringF`, `value.timestampF`, key FROM int_test_obj WHERE key > 9").rdd

            res.count should equal (6)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data(0).getAs[JInteger]("key") should equal (10)
            data(0).getAs[JString]("value.stringF") should equal ("test string")
            data(0).getAs[java.sql.Timestamp]("value.timestampF") should equal (null)

            data(1).getAs[JInteger]("key") should equal (11)
            data(1).getAs[JString]("value.stringF") should equal (null)
            data(1).getAs[java.sql.Timestamp]("value.timestampF") should equal (new java.sql.Timestamp(97, 8, 29, 0, 0, 0, 0))

            data(2).getAs[JInteger]("key") should equal (12)
            data(3).getAs[JInteger]("key") should equal (13)
            data(4).getAs[JInteger]("key") should equal (14)
            data(5).getAs[JInteger]("key") should equal (15)
        }

        it("Should correct filter with GreaterThenOrEqual Clause") {
            val res = spark.sqlContext.sql("SELECT `value.stringF`, `value.timestampF`, key FROM int_test_obj WHERE key >= 10").rdd

            res.count should equal (6)

            val data = res.collect.sortBy(_.getAs[Integer]("key"))

            data(0).getAs[JInteger]("key") should equal (10)
            data(0).getAs[JInteger]("value.stringF") should equal ("test string")
            data(0).getAs[java.sql.Timestamp]("value.timestampF") should equal (null)

            data(1).getAs[JInteger]("key") should equal (11)
            data(1).getAs[JInteger]("value.stringF") should equal (null)
            data(1).getAs[java.sql.Timestamp]("value.timestampF") should equal (new java.sql.Timestamp(97, 8, 29, 0, 0, 0, 0))

            data(2).getAs[JInteger]("key") should equal (12)
            data(3).getAs[JInteger]("key") should equal (13)
            data(4).getAs[JInteger]("key") should equal (14)
            data(5).getAs[JInteger]("key") should equal (15)
        }

        it("Should correct filter with LessThan Clause") {
            val res = spark.sqlContext.sql("SELECT `value.stringF`, `value.byteF`, key FROM int_test_obj WHERE key < 2").rdd

            res.count should equal (1)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data(0).getAs[JInteger]("key") should equal (1)
            data(0).getAs[JInteger]("value.stringF") should equal (null)
            data(0).getAs[JByte]("value.byteF") should equal (1.toByte)
        }

        it("Should correct filter with LessThanOrEqual Clause") {
            val res = spark.sqlContext.sql("SELECT `value.byteF`, `value.shortF`, key FROM int_test_obj WHERE key <= 2").rdd

            res.count should equal (2)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data(0).getAs[JInteger]("key") should equal (1)
            data(0).getAs[JShort]("value.shortF") should equal (null)
            data(0).getAs[JByte]("value.byteF") should equal (1.toByte)

            data(1).getAs[JInteger]("key") should equal (2)
            data(1).getAs[JShort]("value.shortF") should equal (1.toShort)
            data(1).getAs[JByte]("value.byteF") should equal (null)
        }

        it("Should correct filter with In Clause") {
            val res = spark.sqlContext.sql(
                "SELECT * FROM int_test_obj WHERE key in (1, 3, 5)").rdd

            res.count should equal (3)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data(0).getAs[JInteger]("key") should equal (1)
            data(0).getAs[JByte]("value.byteF") should equal (1.toByte)

            data(1).getAs[JInteger]("key") should equal (3)
            data(1).getAs[JShort]("value.integerF") should equal (1.toShort)

            data(2).getAs[JInteger]("key") should equal (5)
            data(2).getAs[JByte]("value.floatF") should equal (1.0F)
        }

        it("Should correct filter with IsNull Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.longF` IS NULL ").rdd

            res.count should equal (14)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
        }

        it("Should correct filter with IsNotNull Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.longF` IS NOT NULL").rdd

            res.count should equal (1)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(4))
        }

        it("Should correct filter with And Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.longF` IS NULL AND key IN (3, 4, 5)").rdd

            res.count should equal (2)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(3, 5))
        }

        it("Should correct filter with Or Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.longF` IS NOT NULL OR key IN (1, 2, 3)").rdd

            res.count should equal (4)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(1, 2, 3, 4))
        }

        it("Should correct filter with Not Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE NOT(key IN (1, 4, 10))").rdd

            res.count should equal (12)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15))
        }

        it("Should correct filter with StringStartsWith Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.stringF` LIKE 'J%'").rdd

            res.count should equal (2)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(12, 13))
        }

        it("Should correct filter with StringEndsWith Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.stringF` LIKE '%e'").rdd

            res.count should equal (2)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(12, 13))
        }

        it("Should correct filter with StringContains Clause") {
            val res = spark.sqlContext.sql("SELECT key FROM int_test_obj WHERE `value.stringF` LIKE '%e%'").rdd

            res.count should equal (4)

            val data = res.collect.sortBy(_.getAs[JInteger]("key"))

            data.map(_.getAs[Int]("key")) should equal (Array(10, 12, 13, 15))
        }

        it("Should correct calculate MAX aggregate function") {
            val res = spark.sqlContext.sql("SELECT max(key) FROM int_test_obj").rdd

            res.count should equal (1)

            val data = res.collect

            data(0).getAs[Long]("max(key)") should equal (15)
        }

        it("Should correct calculate MIN aggregate function") {
            val res = spark.sqlContext.sql("SELECT min(key) FROM int_test_obj").rdd

            res.count should equal (1)

            val data = res.collect

            data(0).getAs[Long]("min(key)") should equal (1)
        }

        it("Should correct calculate AVG aggregate function") {
            val res = spark.sqlContext.sql("SELECT avg(key) FROM int_test_obj WHERE key=1 OR key=2").rdd

            res.count should equal (1)

            val data = res.collect

            data(0).getAs[Double]("avg(key)") should equal (1.5D)
        }

        it("Should correct calculate COUNT(*) aggregate function") {
            val res = spark.sqlContext.sql("SELECT count(*) FROM int_test_obj").rdd

            res.count should equal (1)

            val data = res.collect

            data(0).getAs[Double]("count(1)") should equal (15)
        }

        it("Should correct execute GROUP BY query") {
            val res = spark.sqlContext.sql("SELECT count(1), `value.stringF` FROM int_test_obj GROUP BY `value.stringF` ").rdd

            res.count should equal (6)

            val data = res.collect.sortBy(r ⇒ (r.getAs[Long]("count(1)"), r.getAs[String]("value.stringF")))

            data(0).getAs[JInteger]("count(1)") should equal (1)
            data(0).getAs[JString]("value.stringF") should equal ("Jane Roe")

            data(1).getAs[JInteger]("count(1)") should equal (1)
            data(1).getAs[JString]("value.stringF") should equal ("John Doe")

            data(2).getAs[JInteger]("count(1)") should equal (1)
            data(2).getAs[JString]("value.stringF") should equal ("Mary Major")

            data(3).getAs[JInteger]("count(1)") should equal (1)
            data(3).getAs[JString]("value.stringF") should equal ("Richard Miles")

            data(4).getAs[JInteger]("count(1)") should equal (1)
            data(4).getAs[JString]("value.stringF") should equal ("test string")

            data(5).getAs[JInteger]("count(1)") should equal (10)
            data(5).getAs[JString]("value.stringF") should equal (null)
        }

        it("Should correct execute GROUP BY with HAVING query") {
            val res = spark.sqlContext.sql("SELECT count(1), `value.stringF` FROM int_test_obj GROUP BY `value.stringF` HAVING count(*) = 1 ").rdd

            res.count should equal (5)

            val data = res.collect.sortBy(r ⇒ (r.getAs[Long]("count(1)"), r.getAs[String]("value.stringF")))

            data(0).getAs[JInteger]("count(1)") should equal (1)
            data(0).getAs[JString]("value.stringF") should equal ("Jane Roe")

            data(1).getAs[JInteger]("count(1)") should equal (1)
            data(1).getAs[JString]("value.stringF") should equal ("John Doe")

            data(2).getAs[JInteger]("count(1)") should equal (1)
            data(2).getAs[JString]("value.stringF") should equal ("Mary Major")

            data(3).getAs[JInteger]("count(1)") should equal (1)
            data(3).getAs[JString]("value.stringF") should equal ("Richard Miles")

            data(4).getAs[JInteger]("count(1)") should equal (1)
            data(4).getAs[JString]("value.stringF") should equal ("test string")
        }
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        intTestObjDataFrame = spark.read
            .format(IGNITE)
            .option(CONFIG_FILE, TEST_CONFIG_FILE)
            .option(CACHE, INT_TEST_OBJ_CACHE_NAME)
            .option(KEY_CLASS, classOf[JInteger].getName)
            .option(VALUE_CLASS, classOf[TestObject].getName)
            .load()

        intTestObjDataFrame.createOrReplaceTempView("INT_TEST_OBJ")

        populateIntTestObjectCache()
    }

    def populateIntTestObjectCache(): Unit = {
        val cache = client.cache[Int, TestObject](INT_TEST_OBJ_CACHE_NAME)

        cache.put(1, TestObject(byteF = 1.toByte))
        cache.put(2, TestObject(shortF = 1.toShort))
        cache.put(3, TestObject(integerF = 1))
        cache.put(4, TestObject(longF = 1.toLong))
        cache.put(5, TestObject(floatF = 1.0f))
        cache.put(6, TestObject(doubleF = 1.0))
        cache.put(7, TestObject(bigDecimalF = new java.math.BigDecimal(1)))
        cache.put(8, TestObject(dateF = new Date(97, 8, 29)))
        cache.put(9, TestObject(sqlDateF = new java.sql.Date(97, 8, 29)))
        cache.put(10, TestObject(stringF = "test string"))
        cache.put(11, TestObject(timestampF = new Timestamp(97, 8, 29, 0, 0, 0, 0)))
        cache.put(12, TestObject(stringF = "John Doe"))
        cache.put(13, TestObject(stringF = "Jane Roe"))
        cache.put(14, TestObject(stringF = "Mary Major"))
        cache.put(15, TestObject(stringF = "Richard Miles"))
    }
}

case class TestObject (
    byteF : java.lang.Byte = null,
    shortF : java.lang.Short = null,
    integerF : JInteger = null,
    longF : java.lang.Long = null,
    floatF : java.lang.Float = null,
    doubleF : java.lang.Double = null,
    bigDecimalF : java.math.BigDecimal = null,
    stringF : java.lang.String = null,
    dateF : java.util.Date = null,
    sqlDateF : java.sql.Date = null,
    timestampF : java.sql.Timestamp = null
) { }
