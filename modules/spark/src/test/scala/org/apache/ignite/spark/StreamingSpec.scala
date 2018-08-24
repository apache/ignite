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
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE}
import org.apache.ignite.{Ignite, Ignition}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}

abstract class StreamingSpec extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
    protected var ignite: Ignite = _
    protected var spark: SparkSession = _

    override protected def beforeAll(): Unit = {
        ignite = Ignition.start("ignite-spark-streaming.xml")

        spark = SparkSession.builder()
            .appName("StreamingSpec")
            .master("local")
            .config("spark.executor.instances", "2")
            .getOrCreate()
    }

    override protected def afterAll(): Unit = {
        spark.close()
        ignite.close()
    }

    override protected def beforeEach(): Unit = {
        rmdir(CHECKPOINT_DIR)
    }

    override protected def afterEach(): Unit = {
        rmdir(CHECKPOINT_DIR)
    }

    protected def loadInput(opts: Map[String, String]): DataFrame = {
        val stream = spark.readStream
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)

        opts.map(kv => stream.option(kv._1, kv._2))

        stream.load()
    }

    protected def startOutput(
        data: DataFrame,
        mode: String,
        trigger: Option[Trigger],
        opts: Map[String, String]
    ): StreamingQuery = {
        val stream = data.writeStream
            .outputMode(mode)
            .format(FORMAT_IGNITE)
            .option("checkpointLocation", CHECKPOINT_DIR.toString)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)

        trigger.map(t => stream.trigger(t))
        opts.map(kv => stream.option(kv._1, kv._2))

        stream.start()
    }

    private val CHECKPOINT_DIR = Paths.get(System.getProperty("java.io.tmpdir"), getClass.getName)

    private def rmdir(dir: Path): Unit = {
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