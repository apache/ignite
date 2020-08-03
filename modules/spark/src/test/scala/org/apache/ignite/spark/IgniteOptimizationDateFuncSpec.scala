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
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, checkOptimizationResult, enclose}
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.lang.{Long ⇒ JLong}
import java.util.{Date ⇒ JDate}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.DAYS

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationDateFuncSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    val format = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss")

    describe("Supported optimized date functions") {
        it(" - CURRENT_TIMESTAMP") {
            val df = igniteSession.sql("SELECT id, CURRENT_TIMESTAMP() FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = df.rdd.collect

            assert(data(0).getAs[JLong]("id") == 1L)

            val date: JDate = data(0).getAs[JDate]("current_timestamp()")
            val millisDiff = new JDate().getTime - date.getTime

            assert(millisDiff <= 30000)
        }

        it(" - CURRENT_DATE") {
            val df = igniteSession.sql("SELECT id, CURRENT_DATE() FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = df.rdd.collect

            assert(data(0).getAs[JLong]("id") == 1L)

            val date: JDate = data(0).getAs[JDate]("current_date()")
            val dayDiff = DAYS.convert(new JDate().getTime - date.getTime, TimeUnit.MILLISECONDS)

            assert(dayDiff <= 1)
        }

        it(" - HOUR") {
            val df = igniteSession.sql("SELECT HOUR(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(0)

            checkQueryData(df, data)
        }

        it(" - MINUTE") {
            val df = igniteSession.sql("SELECT MINUTE(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(0)

            checkQueryData(df, data)
        }

        it(" - SECOND") {
            val df = igniteSession.sql("SELECT SECOND(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(0)

            checkQueryData(df, data)
        }

        it(" - MONTH") {
            val df = igniteSession.sql("SELECT MONTH(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(0)

            checkQueryData(df, data)
        }

        it(" - YEAR") {
            val df = igniteSession.sql("SELECT YEAR(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(2017)

            checkQueryData(df, data)
        }

        it(" - QUARTER") {
            val df = igniteSession.sql("SELECT QUARTER(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it(" - WEEK") {
            val df = igniteSession.sql("SELECT WEEKOFYEAR(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it(" - DAY_OF_MONTH") {
            val df = igniteSession.sql("SELECT DAYOFMONTH(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it(" - DAY_OF_YEAR") {
            val df = igniteSession.sql("SELECT DAYOFYEAR(val) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it(" - DATE_ADD") {
            val df = igniteSession.sql("SELECT DATE_ADD(val, 2) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(format.parse("03.01.2017 00:00:00"))

            checkQueryData(df, data)
        }

        it(" - DATEDIFF") {
            val df = igniteSession.sql("SELECT " +
                "DATEDIFF(val, TO_DATE('2017-01-02 00:00:00.000', 'yyyy-MM-dd HH:mm:ss.SSS')) FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it(" - FORMATDATETIME") {
            val df = igniteSession.sql("SELECT DATE_FORMAT(val, 'yyyy-MM-dd HH:mm:ss.SSS') FROM dates WHERE id = 1")

            checkOptimizationResult(df)

            val data = Tuple1("2017-01-01 00:00:00.000")

            checkQueryData(df, data)
        }
    }

    def createDateTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE dates (
              |    id LONG,
              |    val DATE,
              |    PRIMARY KEY (id)) WITH "backups=1"
            """.stripMargin)).getAll

        val qry = new SqlFieldsQuery("INSERT INTO dates(id, val) values (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], format.parse("01.01.2017 00:00:00"))).getAll
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createDateTable(client, DEFAULT_CACHE)

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
    }
}
