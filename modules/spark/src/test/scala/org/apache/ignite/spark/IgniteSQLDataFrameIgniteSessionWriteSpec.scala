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

import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{TEST_CONFIG_FILE, enclose}
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.testframework.GridTestUtils.resolveIgnitePath
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.functions._

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteSQLDataFrameIgniteSessionWriteSpec extends IgniteSQLDataFrameWriteSpec {
    describe("Additional features for IgniteSparkSession") {
        it("Save data frame as a existing table with saveAsTable('table_name') - Overwrite") {
            val citiesDataFrame = spark.read.json(
                resolveIgnitePath("modules/spark/src/test/resources/cities.json").getAbsolutePath)

            citiesDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
                .mode(SaveMode.Overwrite)
                .saveAsTable("city")

            assert(rowsCount("city") == citiesDataFrame.count(),
                s"Table json_city should contain data from json file.")
        }

        it("Save data frame as a existing table with saveAsTable('table_name') - Append") {
            val citiesDataFrame = spark.read.json(
                resolveIgnitePath("modules/spark/src/test/resources/cities.json").getAbsolutePath)

            val rowCnt = citiesDataFrame.count()

            citiesDataFrame
                .withColumn("id", col("id") + rowCnt) //Edit id column to prevent duplication
                .write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
                .mode(SaveMode.Append)
                .partitionBy("id")
                .saveAsTable("city")

            assert(rowsCount("city") == rowCnt*2,
                s"Table json_city should contain data from json file.")
        }

        it("Save data frame as a new table with saveAsTable('table_name')") {
            val citiesDataFrame = spark.read.json(
                resolveIgnitePath("modules/spark/src/test/resources/cities.json").getAbsolutePath)

            citiesDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
                .saveAsTable("new_cities")

            assert(rowsCount("new_cities") == citiesDataFrame.count(),
                s"Table json_city should contain data from json file.")
        }
    }

    override protected def createSparkSession(): Unit = {
        val configProvider = enclose(null) (x ⇒ () ⇒ {
            val cfg = IgnitionEx.loadConfiguration(TEST_CONFIG_FILE).get1()

            cfg.setClientMode(true)

            cfg.setIgniteInstanceName("client-2")

            cfg
        })

        spark = IgniteSparkSession.builder()
            .appName("DataFrameSpec")
            .master("local")
            .config("spark.executor.instances", "2")
            .igniteConfigProvider(configProvider)
            .getOrCreate()
    }
}
