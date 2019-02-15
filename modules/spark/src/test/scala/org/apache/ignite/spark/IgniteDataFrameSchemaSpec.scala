/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spark

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

        it("should successfully load DataFrame data for a Ignite table configured throw java annotation") {
            employeeDataFrame.schema.fields.map(f ⇒ (f.name, f.dataType, f.nullable)) should equal (
                Array(
                    ("ID", LongType, true),
                    ("NAME", StringType, true),
                    ("SALARY", FloatType, true))
            )
        }

        it("should use QueryEntity column aliases") {
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

    case class JPersonWithAlias(
        @(QuerySqlField @field) id: Long,
        @(QuerySqlField @field)(name = "person_name", index = true) name: String)
}
