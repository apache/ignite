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

package org.gridgain.scalar.examples

import javax.cache.processor.{MutableEntry, EntryProcessor}

import org.apache.ignite.dataload.IgniteDataLoadCacheUpdater
import org.apache.ignite.{IgniteCache, IgniteCheckedException}

import java.util
import java.util.Timer
import java.util.Map.Entry

import org.gridgain.scalar.scalar
import org.gridgain.scalar.scalar._

import scala.util.Random

/**
 * Real time popular number counter.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `ggstart.sh examples/config/example-cache.xml`
 * <p>
 * Alternatively you can run [[org.gridgain.examples.datagrid.CacheNodeStartup]] in another JVM which will
 * start GridGain node with `examples/config/example-cache.xml` configuration.
 * <p>
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the grid.
 */
object ScalarCachePopularNumbersExample extends App {
    /** Cache name. */
    private final val CACHE_NAME = "partitioned"

    /** Count of most popular numbers to retrieve from grid. */
    private final val POPULAR_NUMBERS_CNT = 10

    /** Range within which to generate numbers. */
    private final val RANGE = 1000

    /** Count of total numbers to generate. */
    private final val CNT = 100000

    scalar("examples/config/example-cache.xml") {
        // Clean up caches on all nodes before run.
        cache$(CACHE_NAME).get.globalClearAll(0)

        println()
        println(">>> Cache popular numbers example started.")

        val prj = grid$.cluster().forCache(CACHE_NAME)

        if (prj.nodes().isEmpty)
            println("Grid does not have cache configured: " + CACHE_NAME);
        else {
            val popularNumbersQryTimer = new Timer("numbers-query-worker")

            try {
                // Schedule queries to run every 3 seconds during populates cache phase.
                popularNumbersQryTimer.schedule(timerTask(query(POPULAR_NUMBERS_CNT)), 3000, 3000)

                streamData()

                // Force one more run to get final counts.
                query(POPULAR_NUMBERS_CNT)

                // Clean up caches on all nodes after run.
                grid$.cluster().forCache(CACHE_NAME).bcastRun(() => grid$.cache(CACHE_NAME).clearAll(), null)
            }
            finally {
                popularNumbersQryTimer.cancel()
            }
        }
    }

    /**
     * Populates cache in real time with numbers and keeps count for every number.
     * @throws IgniteCheckedException If failed.
     */
    @throws[IgniteCheckedException]
    def streamData() {
        // Set larger per-node buffer size since our state is relatively small.
        // Reduce parallel operations since we running the whole grid locally under heavy load.
        val ldr = dataLoader$[Int, Long](CACHE_NAME, 2048)

        // TODO IGNITE-44: restore invoke.

        (0 until CNT) foreach (_ => ldr.addData(Random.nextInt(RANGE), 1L))

        ldr.close(false)
    }

    /**
     * Queries a subset of most popular numbers from in-memory data grid.
     *
     * @param cnt Number of most popular numbers to return.
     */
    def query(cnt: Int) {
        cache$[Int, Long](CACHE_NAME).get.
            sqlFields(clause = "select _key, _val from Long order by _val desc limit " + cnt).
            sortBy(_(1).asInstanceOf[Long]).reverse.take(cnt).foreach(println)

        println("------------------")
    }
}
