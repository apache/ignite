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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ActorSystem, Props}
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Source}
import org.apache.ignite.cache.CachePeekMode
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.events.CacheEvent
import org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT
import org.apache.ignite.lang.{IgniteBiTuple, IgnitePredicate}
import org.apache.ignite.stream.StreamSingleTupleExtractor
import org.apache.ignite.stream.scala.akka.IgniteAkkaActorStreamerJava
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

            val ignite = Ignition.getOrStart(
                IgniteAkkaActorStreamerSpec.configuration(IgniteAkkaActorStreamerSpec.GRID_NAME, false))

            val cache = ignite.cache[Int, Int](IgniteAkkaActorStreamerSpec.CACHE_NAME)

            val listener = IgniteAkkaActorStreamerSpec.subscribeToPutEvents(ignite)

            val latch = listener.getLatch

            assert(cache.size(CachePeekMode.PRIMARY) == 0)

            val dataStreamer = ignite.dataStreamer[Int, Int](IgniteAkkaActorStreamerSpec.CACHE_NAME)

            dataStreamer.allowOverwrite(true)
            dataStreamer.autoFlushFrequency(1)

            val akkaStreamer = new org.apache.ignite.stream.scala.akka.IgniteAkkaStreamer[Int, Int, Int](
                dataStreamer, IgniteAkkaActorStreamerSpec.singleExtractor)

            val sourceData: Iterator[Int] = {
                var list = new ListBuffer[Int]

                for (i <- 1 to IgniteAkkaActorStreamerSpec.CACHE_ENTRY_COUNT) {
                    list += i.toInt
                }

                list.iterator
            }

            val transform = Flow[Int]
                .map(x => 100 * x)

            // @formatter:off
            val g = RunnableGraph.fromGraph(GraphDSL.create() {
                implicit builder =>
                    import akka.stream.scaladsl.GraphDSL.Implicits._

                    // Source
                    val A: Outlet[Int] = builder.add(Source.fromIterator(() => sourceData)).out

                    // Flow
                    val B: FlowShape[Int, Int] = builder.add(transform)

                    // Sink
                    val C: Inlet[Int] = builder.add(akkaStreamer.foreach).in

                    // Graph
                    A ~> B ~> C

                    ClosedShape
            })
            // @formatter:on

            g.run()

            latch.await(10000, TimeUnit.MILLISECONDS)

            IgniteAkkaActorStreamerSpec.unsubscribeToPutEvents(ignite, listener)

            assert(cache.size(CachePeekMode.PRIMARY) == IgniteAkkaActorStreamerSpec.CACHE_ENTRY_COUNT)

            cache.clear()

            materialize.shutdown()
        }

        it("should successfully store data to ignite cache via akka actor") {
            val system = ActorSystem.create("ignite-streamer")

            implicit val materialize = ActorMaterializer.create(system)

            val ignite = Ignition.getOrStart(
                IgniteAkkaActorStreamerSpec.configuration(IgniteAkkaActorStreamerSpec.GRID_NAME, false))

            val cache = ignite.cache[Int, Int](IgniteAkkaActorStreamerSpec.CACHE_NAME)

            val listener = IgniteAkkaActorStreamerSpec.subscribeToPutEvents(ignite)

            val latch = listener.getLatch

            assert(cache.size(CachePeekMode.PRIMARY) == 0)

            val dataStreamer = ignite.dataStreamer[Int, Int](IgniteAkkaActorStreamerSpec.CACHE_NAME)

            dataStreamer.allowOverwrite(true)
            dataStreamer.autoFlushFrequency(1)

            val actorStreamer = system.actorOf(Props(new IgniteAkkaActorStreamerJava(
                dataStreamer, IgniteAkkaActorStreamerSpec.singleExtractor)), "streamer")

            for (i <- 1 to IgniteAkkaActorStreamerSpec.CACHE_ENTRY_COUNT) {
                actorStreamer ! i
            }

            latch.await(10000, TimeUnit.MILLISECONDS)

            IgniteAkkaActorStreamerSpec.unsubscribeToPutEvents(ignite, listener)

            assert(cache.size(CachePeekMode.PRIMARY) == IgniteAkkaActorStreamerSpec.CACHE_ENTRY_COUNT)

            cache.clear()

            materialize.shutdown()
        }
    }

    override protected def beforeAll() = {
        Ignition.start(IgniteAkkaActorStreamerSpec.configuration(
            IgniteAkkaActorStreamerSpec.GRID_NAME, client = false))
    }

    override protected def afterAll() = {
        Ignition.stop(IgniteAkkaActorStreamerSpec.GRID_NAME, false)
    }
}

/**
 * Constants and utility methods.
 */
object IgniteAkkaActorStreamerSpec {
    /** Grid name. */
    val GRID_NAME = "grid"

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

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME))

        cfg.setClientMode(client)

        cfg.setIgniteInstanceName(igniteInstanceName)

        cfg.setIncludeEventTypes(EVT_CACHE_OBJECT_PUT)

        cfg
    }

    /**
     * Gets cache configuration for the given Ignite instance name.
     *
     * @param name Cache name.
     * @return Cache configuration.
     */
    def cacheConfiguration(name: String): CacheConfiguration[Int, Int] = {
        val ccfg = new CacheConfiguration[Int, Int]()

        ccfg.setBackups(0)
        ccfg.setName(name)

        ccfg
    }

    val singleExtractor = new StreamSingleTupleExtractor[Int, Int, Int] {
        val count: AtomicInteger = new AtomicInteger(0)

        /**
         * Extracts a key-value tuple from a message.
         *
         * @param msg Message.
         * @return Key-value tuple.
         */
        override def extract(msg: Int): Map.Entry[Int, Int] =
            return new IgniteBiTuple[Int, Int](count.getAndIncrement, msg)
    }

    /**
     * @param ignite Ignite instance.
     * @return Cache listener.
     */
    private def subscribeToPutEvents(ignite: Ignite): CacheListener = {
        // Listen to cache PUT events and expect as many as messages as test data items.
        val listener = new CacheListener

        ignite.events(ignite.cluster.forCacheNodes(CACHE_NAME)).localListen(listener, EVT_CACHE_OBJECT_PUT)

        listener
    }

    /**
     * @param listener Cache listener.
     */
    private def unsubscribeToPutEvents(ignite: Ignite, listener: CacheListener) {
        ignite.events(ignite.cluster.forCacheNodes(CACHE_NAME)).stopLocalListen(listener, EVT_CACHE_OBJECT_PUT)
    }

    /**
     * Listener.
     */
    private class CacheListener extends IgnitePredicate[CacheEvent] {
        /** */
        private val latch = new CountDownLatch(CACHE_ENTRY_COUNT)

        /**
         * @return Latch.
         */
        def getLatch: CountDownLatch = latch

        /**
         * @param evt Cache Event.
         * @return true.
         */
        override def apply(evt: CacheEvent): Boolean = {
            latch.countDown()

            true
        }
    }
}
