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

package org.apache.spark.sql.ignite

import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath
import org.apache.ignite.spark.AbstractDataFrameSpec
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, enclose}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Tests to check Spark Session implementation.
  */
@RunWith(classOf[JUnitRunner])
class IgniteSparkSessionSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Ignite Spark Session Implementation") {
        it("should keep session state after session clone") {
            val dfProvider = (s: IgniteSparkSession) => {
                s.read.json(resolveIgnitePath("modules/spark-2.4/src/test/resources/cities.json").getAbsolutePath)
                    .filter("name = 'Denver'")
            }

            var df = dfProvider(igniteSession).cache()

            val cachedData = igniteSession.sharedState.cacheManager.lookupCachedData(df)

            cachedData shouldBe defined

            val otherSession = igniteSession.cloneSession()

            df = dfProvider(otherSession)

            val otherCachedData = otherSession.sharedState.cacheManager.lookupCachedData(df)

            otherCachedData shouldBe defined

            cachedData shouldEqual otherCachedData
        }
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createCityTable(client, DEFAULT_CACHE)

        val configProvider = enclose(null)(_ ⇒ () ⇒ {
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
