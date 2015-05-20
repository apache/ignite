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

import org.apache.ignite.events.CacheEvent
import org.apache.ignite.events.EventType._
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.{IgniteBiPredicate, IgnitePredicate}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import java.util.UUID

/**
 * This examples demonstrates events API. Note that ignite events are disabled by default and
 * must be specifically enabled, just like in `examples/config/example-ignite.xml` file.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheEventsExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"
    
    /** Cache name. */
    private val CACHE_NAME = ScalarCacheEventsExample.getClass.getSimpleName

    scalar(CONFIG) {
        println()
        println(">>> Cache events example started.")

        val cache = createCache$[Integer, String](CACHE_NAME)

        val cluster = cluster$

        try {
            val locLsnr = new IgniteBiPredicate[UUID, CacheEvent] {
                @impl def apply(uuid: UUID, evt: CacheEvent): Boolean = {
                    println("Received event [evt=" + evt.name + ", key=" + evt.key +
                        ", oldVal=" + evt.oldValue + ", newVal=" + evt.newValue)

                    true
                }
            }

            val rmtLsnr = new IgnitePredicate[CacheEvent] {
                @impl def apply(evt: CacheEvent): Boolean = {
                    println("Cache event [name=" + evt.name + ", key=" + evt.key + ']')

                    val key = evt.key.asInstanceOf[Int]

                    key >= 10 && ignite$.affinity(CACHE_NAME).isPrimary(cluster.localNode, key)
                }
            }

            ignite$.events(cluster.forCacheNodes(CACHE_NAME)).remoteListen(locLsnr, rmtLsnr,
                EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_REMOVED)

            for (i <- 0 until 20)
                cache.put(i, Integer.toString(i))

            Thread.sleep(2000)
        }
        finally {
            if (cache != null) cache.close()
        }
    }
}
