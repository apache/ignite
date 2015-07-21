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

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.util
import java.util.Map.Entry
import java.util.Timer
import javax.cache.processor.{EntryProcessor, MutableEntry}

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.stream.StreamReceiver
import org.apache.ignite.{IgniteCache, IgniteException}

import scala.collection.JavaConversions._
import scala.util.Random

/**
 * Real time popular number counter.
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 * <p/>
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the ignite.
 */
object ScalarCachePopularNumbersExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private final val NAME = ScalarCachePopularNumbersExample.getClass.getSimpleName

    /** Count of most popular numbers to retrieve from cluster. */
    private final val POPULAR_NUMBERS_CNT = 10

    /** Random number generator. */
    private final val RAND = new Random()

    /** Range within which to generate numbers. */
    private final val RANGE = 1000

    /** Count of total numbers to generate. */
    private final val CNT = 1000000

    scalar(CONFIG) {
        val cache = createCache$[JavaInt, JavaLong](NAME, indexedTypes = Seq(classOf[JavaInt], classOf[JavaLong]))

        println()
        println(">>> Cache popular numbers example started.")

        try {
            val prj = ignite$.cluster().forCacheNodes(NAME)

            if (prj.nodes().isEmpty)
                println("Ignite does not have cache configured: " + NAME)
            else {
                val popularNumbersQryTimer = new Timer("numbers-query-worker")

                try {
                    // Schedule queries to run every 3 seconds during populates cache phase.
                    popularNumbersQryTimer.schedule(timerTask(query(POPULAR_NUMBERS_CNT)), 3000, 3000)

                    streamData()

                    // Force one more run to get final counts.
                    query(POPULAR_NUMBERS_CNT)
                }
                finally {
                    popularNumbersQryTimer.cancel()
                }
            }
        }
        finally {
            cache.destroy()
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
        val smtr = dataStreamer$[JavaInt, JavaLong](NAME, 2048)

        smtr.receiver(new IncrementingUpdater())

        (0 until CNT) foreach (_ => smtr.addData(RAND.nextInt(RANGE), 1L))

        smtr.close(false)
    }

    /**
     * Queries a subset of most popular numbers from in-memory data ignite cluster.
     *
     * @param cnt Number of most popular numbers to return.
     */
    def query(cnt: Int) {
        val results = cache$[JavaInt, JavaLong](NAME).get
            .query(new SqlFieldsQuery("select _key, _val from Long order by _val desc, _key limit " + cnt))
            .getAll

        results.foreach(res => println(res.get(0) + "=" + res.get(1)))

        println("------------------")
    }

    /**
     * Increments value for key.
     */
    private class IncrementingUpdater extends StreamReceiver[JavaInt, JavaLong] {
        private[this] final val INC = new EntryProcessor[JavaInt, JavaLong, Object]() {
            /** Process entries to increase value by entry key. */
            override def process(e: MutableEntry[JavaInt, JavaLong], args: AnyRef*): Object = {
                e.setValue(Option(e.getValue)
                    .map(l => JavaLong.valueOf(l + 1))
                    .getOrElse(JavaLong.valueOf(1L)))

                null
            }
        }

        @impl def receive(cache: IgniteCache[JavaInt, JavaLong], entries: util.Collection[Entry[JavaInt, JavaLong]]) {
            entries.foreach(entry => cache.invoke(entry.getKey, INC))
        }
    }
}
