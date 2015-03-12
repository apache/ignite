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

package org.apache.ignite.scalar.examples

import org.apache.ignite.examples.datagrid.CacheNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{IgniteCache, IgniteDataLoader, IgniteException}

import javax.cache.processor.{EntryProcessor, MutableEntry}
import java.util
import java.util.Map.Entry
import java.util.Timer

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Real time popular number counter.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `ignite.sh examples/config/example-cache.xml`
 * <p>
 * Alternatively you can run [[CacheNodeStartup]] in another JVM which will
 * start node with `examples/config/example-cache.xml` configuration.
 * <p>
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the ignite.
 */
object ScalarCachePopularNumbersExample extends App {
    /** Cache name. */
    private final val CACHE_NAME = "partitioned"

    /** Count of most popular numbers to retrieve from cluster. */
    private final val POPULAR_NUMBERS_CNT = 10

    /** Random number generator. */
    private final val RAND = new Random()

    /** Range within which to generate numbers. */
    private final val RANGE = 1000

    /** Count of total numbers to generate. */
    private final val CNT = 1000000

    scalar("examples/config/example-cache.xml") {
        // Clean up caches on all nodes before run.
        cache$(CACHE_NAME).get.clear()

        println()
        println(">>> Cache popular numbers example started.")

        val prj = ignite$.cluster().forCacheNodes(CACHE_NAME)

        if (prj.nodes().isEmpty)
            println("Ignite does not have cache configured: " + CACHE_NAME)
        else {
            val popularNumbersQryTimer = new Timer("numbers-query-worker")

            try {
                // Schedule queries to run every 3 seconds during populates cache phase.
                popularNumbersQryTimer.schedule(timerTask(query(POPULAR_NUMBERS_CNT)), 3000, 3000)

                streamData()

                // Force one more run to get final counts.
                query(POPULAR_NUMBERS_CNT)

                // Clean up caches on all nodes after run.
                ignite$.cluster().forCacheNodes(CACHE_NAME).bcastRun(() => ignite$.jcache(CACHE_NAME).clear(), null)
            }
            finally {
                popularNumbersQryTimer.cancel()
            }
        }
    }

    /**
     * Populates cache in real time with numbers and keeps count for every number.
     * @throws IgniteException If failed.
     */
    @throws[IgniteException]
    def streamData() {
        // Set larger per-node buffer size since our state is relatively small.
        // Reduce parallel operations since we running the whole ignite cluster locally under heavy load.
        val ldr = dataLoader$[Int, Long](CACHE_NAME, 2048)

        ldr.updater(new IncrementingUpdater())

        (0 until CNT) foreach (_ => ldr.addData(RAND.nextInt(RANGE), 1L))

        ldr.close(false)
    }

    /**
     * Queries a subset of most popular numbers from in-memory data ignite cluster.
     *
     * @param cnt Number of most popular numbers to return.
     */
    def query(cnt: Int) {
        val results = cache$[Int, Long](CACHE_NAME).get
            .sqlFields(clause = "select _key, _val from Long order by _val desc, _key limit " + cnt)
            .getAll

        results.foreach(res => println(res.get(0) + "=" + res.get(1)))

        println("------------------")
    }

    /**
     * Increments value for key.
     */
    private class IncrementingUpdater extends IgniteDataLoader.Updater[Int, Long] {
        private[this] final val INC = new EntryProcessor[Int, Long, Object]() {
            /** Process entries to increase value by entry key. */
            override def process(e: MutableEntry[Int, Long], args: AnyRef*): Object = {
                e.setValue(Option(e.getValue).map(_ + 1).getOrElse(1L))

                null
            }
        }

        @impl def update(cache: IgniteCache[Int, Long], entries: util.Collection[Entry[Int, Long]]) {
            entries.foreach(entry => cache.invoke(entry.getKey, INC))
        }
    }
}
