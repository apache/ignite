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

package org.apache.ignite.stream.akka

import java.util.Map
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.lang.IgniteBiTuple
import org.apache.ignite.stream.StreamSingleTupleExtractor
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class IgniteAkkaActorStreamerSpec extends FunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
    describe("Ignite Akka stream.") {
        it("should successfully store data to ignite cache via akka-stream") {
            val system = ActorSystem.create("ignite-streamer")
            implicit val materialize = ActorMaterializer.create(system)

            val ignite = Ignition.getOrStart(IgniteAkkaActorStreamerSpec.configuration("grid", client = false))

            ignite.getOrCreateCache(new CacheConfiguration[Integer, Integer]("grid"))
            val dataStreamer = ignite.dataStreamer[Integer, Integer]("grid")

            dataStreamer.allowOverwrite(true)
            dataStreamer.autoFlushFrequency(1)

            val igniteStreamer = new org.apache.ignite.stream.scala.akka.IgniteAkkaStreamer[Integer, Integer, Integer](dataStreamer, IgniteAkkaActorStreamerSpec.singleExtractor)

            val sourceData: Iterator[Integer] = {
                var list = new ListBuffer[Integer]

                for (i <- 1 to 1000) {
                    list += i.toInt
                }

                list.iterator
            }

            val transform = Flow[Integer]
                .map(100 + _)

            // @formatter:off
            val g = RunnableGraph.fromGraph(GraphDSL.create() {
                implicit builder =>
                    import akka.stream.scaladsl.GraphDSL.Implicits._

                    // Source
                    val A: Outlet[Integer] = builder.add(Source.fromIterator(() => sourceData)).out

                    // Flow
//                    val B: FlowShape[Integer, Integer] = builder.add(transform)

                    // Sink
                    val C: Inlet[Integer] = builder.add(igniteStreamer.foreach).in

                    // Graph
                    A ~> C

                    ClosedShape
            })
            // @formatter:on

            g.run()
        }
    }

    override protected def beforeAll() = {
        Ignition.start(IgniteAkkaActorStreamerSpec.configuration("grid", client = false))
    }

    override protected def afterAll() = {
        Ignition.stop("grid", false)
    }
}

/**
 * Constants and utility methods.
 */
object IgniteAkkaActorStreamerSpec {
    /** Cache name. */
    val CACHE_NAME = "scala-akka-stream"

    /** Cache entries count. */
    val CACHE_ENTRY_COUNT = 1000

    /**
     * Gets ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param client Client mode flag.
     * @return Ignite configuration.
     */
    def configuration(igniteInstanceName: String, client: Boolean): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName))
        cfg.setClientMode(client)
        cfg.setIgniteInstanceName(igniteInstanceName)

        cfg
    }

    /**
     * Gets cache configuration for the given Ignite instance name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     */
    def cacheConfiguration(igniteInstanceName: String): CacheConfiguration[Object, Object] = {
        val ccfg = new CacheConfiguration[Object, Object]()

        ccfg.setBackups(0)
        ccfg.setName(CACHE_NAME)

        ccfg
    }

    val singleExtractor = new StreamSingleTupleExtractor[Integer, Integer, Integer] {
        val count: AtomicInteger = new AtomicInteger(0)
        /**
         * Extracts a key-value tuple from a message.
         *
         * @param msg Message.
         * @return Key-value tuple.
         */
        override def extract(msg: Integer): Map.Entry[Integer, Integer] = return new IgniteBiTuple[Integer, Integer](count.getAndIncrement, msg)
    }
}
