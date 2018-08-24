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

import java.lang.{Long => JLong}
import java.sql.Timestamp
import java.util.concurrent.Executors

import javax.cache.CacheException
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.streaming.Trigger
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters

@RunWith(classOf[JUnitRunner])
class StreamingWithTablesSpec extends StreamingSpec {
    private val IN_TBL_NAME = "INPUT"
    private val OUT_TBL_NAME = "OUTPUT"
    private val DFLT_CACHE_NAME = "default"
    private val INJECT_RATE = 500

    private val exeSvc = Executors.newScheduledThreadPool(4)

    private val sampleData = Seq(
        Seq(1L, Timestamp.valueOf("2017-01-01 00:00:00"), "s1"),
        Seq(2L, Timestamp.valueOf("2017-01-02 00:00:00"), "s2"),
        Seq(3L, Timestamp.valueOf("2017-01-03 00:00:00"), "s3")
    )

    describe("Spark Streaming with Ignite Tables") {
        it("reads and writes historical incremental data") {
            sampleData.foreach(i => input(
                i.head.asInstanceOf[JLong],
                i(1).asInstanceOf[Timestamp],
                i(2).asInstanceOf[String]
            ))

            startOutput(
                loadInput(Map(
                    OPTION_TABLE -> IN_TBL_NAME,
                    OPTION_OFFSET_POLICY -> "incremental",
                    OPTION_OFFSET_FIELD -> "id"
                )),
                "append",
                Some(Trigger.Once()),
                Map(OPTION_TABLE -> OUT_TBL_NAME, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS -> "ID")
            ).awaitTermination()

            output should contain theSameElementsAs sampleData
        }

        it("reads and writes live incremental data") {
            val qry = startOutput(
                loadInput(Map(
                    OPTION_TABLE -> IN_TBL_NAME,
                    OPTION_OFFSET_POLICY -> "incremental",
                    OPTION_OFFSET_FIELD -> "id"
                )),
                "append",
                None,
                Map(OPTION_TABLE -> OUT_TBL_NAME, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS -> "ID")
            )

            exeSvc.submit(new Runnable {
                override def run(): Unit = {
                    sampleData.foreach(i => {
                        input(
                            i.head.asInstanceOf[JLong],
                            i(1).asInstanceOf[Timestamp],
                            i(2).asInstanceOf[String]
                        )
                        Thread.sleep(INJECT_RATE)
                    })

                    qry.stop()
                }
            })

            qry.awaitTermination()

            output should contain theSameElementsAs sampleData
        }

        it("reads and writes historical timestamped data") {
            sampleData.foreach(i => input(
                i.head.asInstanceOf[JLong],
                i(1).asInstanceOf[Timestamp],
                i(2).asInstanceOf[String]
            ))

            startOutput(
                loadInput(Map(
                    OPTION_TABLE -> IN_TBL_NAME,
                    OPTION_OFFSET_POLICY -> "timestamp",
                    OPTION_OFFSET_FIELD -> "ts"
                )),
                "append",
                Some(Trigger.Once()),
                Map(OPTION_TABLE -> OUT_TBL_NAME, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS -> "ID")
            ).awaitTermination()

            output should contain theSameElementsAs sampleData
        }

        it("reads and writes live timestamped data") {
            val qry = startOutput(
                loadInput(Map(
                    OPTION_TABLE -> IN_TBL_NAME,
                    OPTION_OFFSET_POLICY -> "timestamp",
                    OPTION_OFFSET_FIELD -> "ts"
                )),
                "append",
                None,
                Map(OPTION_TABLE -> OUT_TBL_NAME, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS -> "ID")
            )

            exeSvc.submit(new Runnable {
                override def run(): Unit = {
                    sampleData.foreach(i => {
                        input(
                            i.head.asInstanceOf[JLong],
                            i(1).asInstanceOf[Timestamp],
                            i(2).asInstanceOf[String]
                        )
                        Thread.sleep(INJECT_RATE)
                    })

                    qry.stop()
                }
            })

            qry.awaitTermination()

            output should contain theSameElementsAs sampleData
        }

        it("does not create output when there is no input") {
            startOutput(
                loadInput(Map(
                    OPTION_TABLE -> IN_TBL_NAME,
                    OPTION_OFFSET_POLICY -> "incremental",
                    OPTION_OFFSET_FIELD -> "id"
                )),
                "append",
                Some(Trigger.Once()),
                Map(OPTION_TABLE -> OUT_TBL_NAME, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS -> "ID")
            ).awaitTermination()

            output should be(empty)
        }
    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTable(IN_TBL_NAME)
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTable(IN_TBL_NAME)
        dropTable(OUT_TBL_NAME)
    }

    private def createTable(name: String): Unit =
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"CREATE TABLE $name(id LONG, ts TIMESTAMP, ch VARCHAR, PRIMARY KEY(id))"
        )).getAll

    private def dropTable(name: String): Unit = scala.util.control.Exception.ignoring(classOf[CacheException]) {
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"DROP TABLE $name"
        )).getAll
    }

    private def input(id: JLong, ts: Timestamp, ch: String): Unit =
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"INSERT INTO $IN_TBL_NAME(id, ts, ch) VALUES(?, ?, ?)"
        ).setArgs(id, ts, ch)).getAll

    private def output: Seq[Seq[Any]] = {
        try {
            val out = ignite
                .cache(DFLT_CACHE_NAME)
                .query(new SqlFieldsQuery(s"SELECT id, ts, ch FROM $OUT_TBL_NAME"))
                .getAll

            JavaConverters.asScalaIteratorConverter(
                out.iterator()).asScala.toSeq.map(
                r => JavaConverters.asScalaIteratorConverter(r.iterator()).asScala.toSeq
            )
        }
        catch {
            case _: CacheException => Seq()
        }
    }
}