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

import java.lang.{Integer ⇒ JInteger, String ⇒ JString}

import org.apache.ignite.Ignite
import org.apache.ignite.spark.AbstractDataFrameSpec._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.ignite.spark.IgniteDataFrameOptions._

/**
  * Tests to check loading schema for Ignite data sources.
  */
@RunWith(classOf[JUnitRunner])
class IgniteDataFrameSchemaSpec extends AbstractDataFrameSpec {
    var personDataFrame: DataFrame = _
    var employeeDataFrame: DataFrame = _
    var intStrDataFrame: DataFrame = _
    var intTestObjDataFrame: DataFrame = _

    describe("Loading DataFrame schema for Ignite tables") {
        it("should successfully load DataFrame schema for a Ignite SQL Table") {
            personDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("NAME", StringType, true),
                    ("BIRTH_DATE", DateType, true),
                    ("IS_RESIDENT", BooleanType, true),
                    ("SALARY", DoubleType, true),
                    ("PENSION", DoubleType, true),
                    ("ACCOUNT", DecimalType(10, 0), true),
                    ("AGE", IntegerType, true),
                    ("ID", LongType, false),
                    ("CITY_ID", LongType, false))
            )
        }

        it("should successfully load DataFrame schema for a Ignite key-value cache") {
            intStrDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("key", IntegerType, false),
                    ("value", StringType, false))
            )
        }

        it("should successfully load DataFrame data for a Ignite complex key-value cache") {
            intTestObjDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("key", IntegerType, false),
                    ("value.byteF", ByteType, true),
                    ("value.shortF", ShortType, true),
                    ("value.integerF", IntegerType, true),
                    ("value.longF", LongType, true),
                    ("value.floatF", FloatType, true),
                    ("value.doubleF", DoubleType, true),
                    ("value.bigDecimalF", DecimalType(10, 0), true),
                    ("value.stringF", StringType, true),
                    ("value.dateF", DateType, true),
                    ("value.sqlDateF", DateType, true),
                    ("value.timestampF", TimestampType, true))
            )
        }

        it("should successfully load DataFrame data for a Ignite table configured throw java annotation") {
            employeeDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("id", LongType, true),
                    ("name", StringType, true),
                    ("salary", FloatType, true))
            )
        }
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createPersonTable(client, INT_STR_CACHE_NAME)

        createEmployeeCache(client, EMPLOYEE_CACHE_NAME)

        personDataFrame = spark.read
            .format(IGNITE)
            .option(CONFIG_FILE, TEST_CONFIG_FILE)
            .option(TABLE, "person")
            .load()

        personDataFrame.createOrReplaceTempView("person")

        employeeDataFrame = spark.read
            .format(IGNITE)
            .option(CONFIG_FILE, TEST_CONFIG_FILE)
            .option(TABLE, "employee")
            .load()

        employeeDataFrame.createOrReplaceTempView("employee")
    }
}
