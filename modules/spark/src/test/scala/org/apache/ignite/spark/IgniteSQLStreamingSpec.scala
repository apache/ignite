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

import java.io.IOException
import java.lang.{Long => JLong}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.sql.Timestamp
import java.util.concurrent.Executors

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.{Ignite, Ignition}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}

import scala.collection.JavaConverters

@RunWith(classOf[JUnitRunner])
class IgniteSQLStreamingSpec extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
    private val DFLT_CACHE_NAME = "cache1"
    private val IN_TBL_NAME = "INPUT"
    private val OUT_TBL_NAME = "OUTPUT"
    private val INJECT_RATE = 500
    private val CHECKPOINT_DIR = Paths.get(System.getProperty("java.io.tmpdir"), getClass.getName)

    private val exeSvc = Executors.newScheduledThreadPool(4)
    private var ignite: Ignite = _
    private var spark: SparkSession = _

    private val sampleData = Seq(
        Seq(1L, Timestamp.valueOf("2017-01-01 00:00:00"), "s1"),
        Seq(2L, Timestamp.valueOf("2017-01-02 00:00:00"), "s2"),
        Seq(3L, Timestamp.valueOf("2017-01-03 00:00:00"), "s3")
    )

    describe("Spark Streaming with Ignite") {
        it("reads and writes historical incremental data") {
            sampleData.foreach(i => insertIntoTable(
                i.head.asInstanceOf[JLong],
                i(1).asInstanceOf[Timestamp],
                i(2).asInstanceOf[String]
            ))

            startOutput(
                loadInput(Map(OPTION_OFFSET_POLICY -> "incremental", OPTION_OFFSET_FIELD -> "id")),
                "append",
                Some(Trigger.Once())
            ).awaitTermination()

            getOutput should contain theSameElementsAs sampleData
        }

        it("reads and writes live incremental data") {
            val qry = startOutput(
                loadInput(Map(OPTION_OFFSET_POLICY -> "incremental", OPTION_OFFSET_FIELD -> "id")),
                "append",
                None
            )

            exeSvc.submit(new Runnable {
                override def run(): Unit = {
                    sampleData.foreach(i => {
                        insertIntoTable(
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

            getOutput should contain theSameElementsAs sampleData
        }

        it("reads and writes historical timestamped data") {
            sampleData.foreach(i => insertIntoTable(
                i.head.asInstanceOf[JLong],
                i(1).asInstanceOf[Timestamp],
                i(2).asInstanceOf[String]
            ))

            startOutput(
                loadInput(Map(OPTION_OFFSET_POLICY -> "timestamp", OPTION_OFFSET_FIELD -> "ts")),
                "append",
                Some(Trigger.Once())
            ).awaitTermination()

            getOutput should contain theSameElementsAs sampleData
        }

        it("reads and writes live timestamped data") {
            val qry = startOutput(
                loadInput(Map(OPTION_OFFSET_POLICY -> "timestamp", OPTION_OFFSET_FIELD -> "ts")),
                "append",
                None
            )

            exeSvc.submit(new Runnable {
                override def run(): Unit = {
                    sampleData.foreach(i => {
                        insertIntoTable(
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

            getOutput should contain theSameElementsAs sampleData
        }
    }

    override protected def beforeAll(): Unit = {
        ignite = Ignition.start("ignite-spark-config.xml")

        spark = SparkSession.builder()
            .appName("DataFrameSpec")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()
    }

    override protected def afterAll(): Unit = {
        spark.close()
        ignite.close()
    }

    override protected def beforeEach(): Unit = {
        createTable()
        rmdir(CHECKPOINT_DIR)
    }

    override protected def afterEach(): Unit = {
        dropTable()
        rmdir(CHECKPOINT_DIR)
    }

    private def loadInput(opts: Map[String, String]): DataFrame = {
        val stream = spark.readStream
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, IN_TBL_NAME)

        opts.map(kv => stream.option(kv._1, kv._2))

        stream.load()
    }

    private def startOutput(data: DataFrame, mode: String, trigger: Option[Trigger]): StreamingQuery = {
        val stream = data.writeStream
            .outputMode(mode)
            .format(FORMAT_IGNITE)
            .option("checkpointLocation", CHECKPOINT_DIR.toString)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, OUT_TBL_NAME)
            .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "ID")
        trigger.map(t => stream.trigger(t))
        stream.start()
    }

    private def createTable(): Unit =
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"CREATE TABLE $IN_TBL_NAME(id LONG, ts TIMESTAMP, ch VARCHAR, PRIMARY KEY(id))"
        )).getAll

    private def dropTable(): Unit =
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"DROP TABLE $IN_TBL_NAME"
        )).getAll

    private def insertIntoTable(id: JLong, ts: Timestamp, ch: String): Unit = {
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"INSERT INTO $IN_TBL_NAME(id, ts, ch) VALUES(?, ?, ?)"
        ).setArgs(id, ts, ch)).getAll
    }

    private def getOutput: Seq[Seq[Any]] = {
        val out = ignite
            .cache(DFLT_CACHE_NAME)
            .query(new SqlFieldsQuery(s"SELECT id, ts, ch FROM $OUT_TBL_NAME"))
            .getAll

        JavaConverters.asScalaIteratorConverter(
            out.iterator()).asScala.toSeq.map(r => JavaConverters.asScalaIteratorConverter(r.iterator()).asScala.toSeq
        )
    }

    def rmdir(dir: Path): Unit = {
        if (!dir.toFile.exists) return
        Files.walkFileTree(dir, new SimpleFileVisitor[Path]() {
            override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                Files.delete(file)
                FileVisitResult.CONTINUE
            }

            override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
                Files.delete(dir)
                FileVisitResult.CONTINUE
            }
        })
    }
}
