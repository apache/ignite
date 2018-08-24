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
import java.util.Objects
import java.util.concurrent.Executors

import javax.cache.Cache
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.streaming.Trigger
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import scala.annotation.meta.field

@RunWith(classOf[JUnitRunner])
class StreamingWithCaches extends StreamingSpec {
    case class Value(
        @(QuerySqlField@field) id: JLong,
        @(QuerySqlField@field) ts: Timestamp,
        @(QuerySqlField@field) ch: String
    ) {
        override def equals(obj: Any): Boolean = {
            if (!obj.isInstanceOf[Value])
                false
            else {
                val other = obj.asInstanceOf[Value]
                id == other.id && Objects.equals(ts, other.ts) && Objects.equals(ch, other.ch)
            }
        }

        override def hashCode(): Int = {
            var res = JLong.hashCode(id)
            if (ts != null) res += 31 * res + ts.hashCode()
            if (ch != null) res += 31 * res + ch.hashCode
            res
        }

    }

    private val IN_CACHE_NAME = "INPUT"
    private val OUT_CACHE_NAME = "OUTPUT"
    private val INJECT_RATE = 500

    private val exeSvc = Executors.newScheduledThreadPool(4)

    private val sampleData = Seq(
        Value(1L, Timestamp.valueOf("2017-01-01 00:00:00"), "s1"),
        Value(2L, Timestamp.valueOf("2017-01-02 00:00:00"), "s2"),
        Value(3L, Timestamp.valueOf("2017-01-03 00:00:00"), "s3")
    )

    private var inCache: Cache[JLong, Value] = _

    describe("Spark Streaming with Ignite Caches") {
        it("reads and writes historical incremental data") {
            inCache.putAll(sampleData.map(i => i.id -> i).toMap.asJava)

            startOutput(
                loadInput(Map(
                    OPTION_TABLE -> classOf[Value].getSimpleName,
                    OPTION_OFFSET_POLICY -> "incremental",
                    OPTION_OFFSET_FIELD -> "id"
                )),
                "append",
                Some(Trigger.Once()),
                Map(OPTION_TABLE -> OUT_CACHE_NAME, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS -> "ID")
            ).awaitTermination()

            getOutput should contain theSameElementsAs sampleData
        }

        it("reads and writes live incremental data") {
            val qry = startOutput(
                loadInput(Map(
                    OPTION_TABLE -> classOf[Value].getSimpleName,
                    OPTION_OFFSET_POLICY -> "incremental",
                    OPTION_OFFSET_FIELD -> "id"
                )),
                "append",
                None,
                Map(OPTION_TABLE -> OUT_CACHE_NAME, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS -> "ID")
            )

            exeSvc.submit(new Runnable {
                override def run(): Unit = {
                    sampleData.foreach(i => {
                        inCache.put(i.id, i)
                        Thread.sleep(INJECT_RATE)
                    })

                    qry.stop()
                }
            })

            qry.awaitTermination()

            getOutput should contain theSameElementsAs sampleData
        }
    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        inCache = createCache()
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        destroyCache()
    }

    private def createCache(): Cache[JLong, Value] =
        ignite.createCache(
            new CacheConfiguration[JLong, Value](IN_CACHE_NAME).setIndexedTypes(classOf[JLong], classOf[Value])
        )

    private def destroyCache(): Unit = ignite.destroyCache(IN_CACHE_NAME)

    private def getOutput: Seq[Value] =
        inCache.getAll(new java.util.HashSet(sampleData.map(i => i.id).asJava))
            .asScala.map(i => i._2.asInstanceOf[Value]).toSeq
}