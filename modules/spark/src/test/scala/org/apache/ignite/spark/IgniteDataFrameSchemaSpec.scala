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
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.AbstractDataFrameSpec._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.ignite.spark.IgniteDataFrameSettings._

import scala.annotation.meta.field

/**
  * Tests to check loading schema for Ignite data sources.
  */
@RunWith(classOf[JUnitRunner])
class IgniteDataFrameSchemaSpec extends AbstractDataFrameSpec {
    var personDataFrame: DataFrame = _

    var employeeDataFrame: DataFrame = _

    var personWithAliasesDataFrame: DataFrame = _

    var columnMetaDataFrame: DataFrame = _

    var addedColumnDataFrame: DataFrame = _

    var droppedColumnDataFrame: DataFrame = _

    describe("Loading DataFrame schema for Ignite tables") {
        it("should successfully load DataFrame schema for a Ignite SQL Table") {
            personDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("NAME", StringType, true),
                    ("BIRTH_DATE", DateType, true),
                    ("IS_RESIDENT", BooleanType, true),
                    ("SALARY", DoubleType, true),
                    ("PENSION", DoubleType, true),
                    ("ACCOUNT", IgniteRDD.DECIMAL, true),
                    ("AGE", IntegerType, true),
                    ("ID", LongType, false),
                    ("CITY_ID", LongType, false))
            )
        }

        it("should show correct schema for a Ignite SQL Table with modified column") {
            columnMetaDataFrame = spark.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "test")
                .load()

            columnMetaDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("A", IntegerType, true),
                    ("B", StringType, true),
                    ("ID", IntegerType, false))
            )

            addColumnForTable(client, DEFAULT_CACHE)

            addedColumnDataFrame = spark.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "test")
                .load()

            addedColumnDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("A", IntegerType, true),
                    ("B", StringType, true),
                    ("C", IntegerType, true),
                    ("ID", IntegerType, false))
            )

            dropColumnFromTable(client, DEFAULT_CACHE)

            droppedColumnDataFrame = spark.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "test")
                .load()

            droppedColumnDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("B", StringType, true),
                    ("C", IntegerType, true),
                    ("ID", IntegerType, false))
            )
        }

        it("should successfully load DataFrame data for a Ignite table configured throw java annotation") {
            employeeDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("ID", LongType, true),
                    ("NAME", StringType, true),
                    ("SALARY", FloatType, true))
            )
        }

        it("should use GridQueryTypeDescriptor column aliases") {
            personWithAliasesDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("ID", LongType, true),
                    ("PERSON_NAME", StringType, true))
            )
        }
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        client.getOrCreateCache(new CacheConfiguration[Long, JPersonWithAlias]()
            .setName("P3")
            .setIndexedTypes(classOf[Long], classOf[JPersonWithAlias]))

        personWithAliasesDataFrame = spark.read
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, classOf[JPersonWithAlias].getSimpleName)
            .load()

        createPersonTable(client, DEFAULT_CACHE)

        createMetaTestTable(client, DEFAULT_CACHE)

        createEmployeeCache(client, EMPLOYEE_CACHE_NAME)

        personDataFrame = spark.read
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, "person")
            .load()

        personDataFrame.createOrReplaceTempView("person")

        employeeDataFrame = spark.read
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, "employee")
            .load()

        employeeDataFrame.createOrReplaceTempView("employee")
    }

    def createMetaTestTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE test (id INT PRIMARY KEY, a INT, b CHAR)")).getAll
    }

    def addColumnForTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            "ALTER TABLE test ADD COLUMN c int")).getAll
    }

    def dropColumnFromTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            "ALTER TABLE test DROP COLUMN a")).getAll
    }

    case class JPersonWithAlias(
        @(QuerySqlField @field) id: Long,
        @(QuerySqlField @field)(name = "person_name", index = true) name: String)
}
