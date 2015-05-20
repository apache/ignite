/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid

import org.apache.ignite.IgniteDataStreamer
import org.apache.ignite.cache.CacheEntryEventSerializableFilter
import org.apache.ignite.cache.query.{ContinuousQuery, ScanQuery}
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgniteBiPredicate
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import javax.cache.event.{CacheEntryEvent, CacheEntryUpdatedListener}
import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConversions._

/**
 * Demonstrates how cache can be populated with data utilizing [[IgniteDataStreamer]] API.
 * [[IgniteDataStreamer]] is a lot more efficient to use than standard
 * `put(...)` operation as it properly buffers cache requests
 * together and properly manages load on remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheContinuousQueryExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"
    
    /** Cache name. */
    private val CACHE_NAME = ScalarCacheContinuousQueryExample.getClass.getSimpleName

    scalar(CONFIG) {
        println()
        println(">>> Cache continuous query example started.")

        val cache = createCache$[Integer, String](CACHE_NAME)

        try {
            val keyCnt = 20

            for (i <- 0 until keyCnt)
                cache.put(i, Integer.toString(i))

            val qry = new ContinuousQuery[Integer, String]

            qry.setInitialQuery(new ScanQuery[Integer, String](new IgniteBiPredicate[Integer, String] {
                @impl def apply(key: Integer, value: String): Boolean = {
                    key > 10
                }
            }))

            qry.setLocalListener(new CacheEntryUpdatedListener[Integer, String] {
                @impl def onUpdated(events: JavaIterable[CacheEntryEvent[_ <: Integer, _ <: String]]) {
                    for (e <- events)
                        println("Updated entry [key=" + e.getKey + ", val=" + e.getValue + ']')
                }
            })

            qry.setRemoteFilter(new CacheEntryEventSerializableFilter[Integer, String] {
                @impl def evaluate(e: CacheEntryEvent[_ <: Integer, _ <: String]): Boolean = {
                    e.getKey > 10
                }
            })

            val cur = cache.query(qry)

            try {
                for (e <- cur)
                    println("Queried existing entry [key=" + e.getKey + ", val=" + e.getValue + ']')

                for (i <- keyCnt until (keyCnt + 10))
                    cache.put(i, Integer.toString(i))

                Thread.sleep(2000)
            }
            finally {
                if (cur != null)
                    cur.close()
            }
        }
        finally {
            cache.close()
        }
    }
}
