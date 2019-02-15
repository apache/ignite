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

import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.ignite.IgniteOptimization
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationDisableEnableSpec extends AbstractDataFrameSpec {
    var personDataFrame: DataFrame = _

    describe("Ignite Optimization Disabling/Enabling") {
        it("should add Ignite Optimization to a session on a first query") {
            if (spark.sparkContext.isStopped)
                createSparkSession()

            assert(!igniteOptimizationExists(spark), "Session shouldn't contains IgniteOptimization")

            personDataFrame = spark.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "person")
                .load()

            assert(igniteOptimizationExists(spark),
                "Session should contains IgniteOptimization after executing query over Ignite Data Frame")

            spark.stop()
        }

        it("should remove Ignite Optimization if it disabled at runtime") {
            if (!spark.sparkContext.isStopped)
                spark.stop()

            val newSession = SparkSession.builder()
                .appName("Ignite Optimization check")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate()

            assert(!igniteOptimizationExists(newSession), "Session shouldn't contains IgniteOptimization")

            var newPersonDataFrame = newSession.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "person")
                .load()

            assert(igniteOptimizationExists(newSession),
                "Session should contains IgniteOptimization after executing query over Ignite Data Frame")


            newSession.conf.set(OPTION_DISABLE_SPARK_SQL_OPTIMIZATION, "true")

            newPersonDataFrame = newSession.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "person")
                .load()

            assert(!igniteOptimizationExists(newSession),
                "Session shouldn't contains IgniteOptimization")

            newSession.close()
        }

        it("shouldn't add Ignite Optimization to a session when it's disabled") {
            if (!spark.sparkContext.isStopped)
                spark.stop()

            val newSession = SparkSession.builder()
                .appName("Ignite Optimization check")
                .master("local")
                .config("spark.executor.instances", "2")
                .config(OPTION_DISABLE_SPARK_SQL_OPTIMIZATION, "true")
                .getOrCreate()

            assert(!igniteOptimizationExists(newSession), "Session shouldn't contains IgniteOptimization")

            val newPersonDataFrame = newSession.read
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "person")
                .load()

            newPersonDataFrame.createOrReplaceTempView("person")

            val res = newSession.sqlContext.sql("SELECT name FROM person WHERE id = 2").rdd

            res.count should equal(1)

            assert(!igniteOptimizationExists(newSession), "Session shouldn't contains IgniteOptimization")

            newSession.close()
        }
    }

    def igniteOptimizationExists(session: SparkSession): Boolean =
        session.sessionState.experimentalMethods.extraOptimizations.contains(IgniteOptimization)

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createPersonTable(client, "cache1")
    }
}
