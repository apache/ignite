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

import org.apache.ignite.cache.CachePeekMode

import org.apache.ignite.events.Event
import org.apache.ignite.events.EventType._
import org.apache.ignite.lang.IgnitePredicate
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.collection.JavaConversions._

/**
 * Demonstrates basic In-Memory Data Ignite Cluster operations with Scalar.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: `'ignite.sh examples/config/example-cache.xml'`. Local node can
 * be started with or without cache.
 */
object ScalarCacheExample extends App {
    /** Name of cache specified in spring configuration. */
    private val NAME = "partitioned"

    private val peekModes = Array.empty[CachePeekMode]

    scalar("examples/config/example-cache.xml") {
        // Clean up caches on all nodes before run.
        cache$(NAME).get.clear()

        registerListener()

        basicOperations()
    }

    /**
     * Demos basic cache operations.
     */
    def basicOperations() {
        val c = cache$[String, Int](NAME).get

        // Add few values.
        c += (1.toString -> 1)
        c += (2.toString -> 2)

        // Update values.
        c += (1.toString -> 11)
        c += (2.toString -> 22)

        c += (1.toString -> 31)
        c += (2.toString -> 32)
        c += ((2.toString, 32))

        // Remove couple of keys (if any).
        c -= (11.toString, 22.toString)

        // Put one more value.
        c += (3.toString -> 11)

        // Get with option...
        c.opt(44.toString) match {
            case Some(v) => sys.error("Should never happen.")
            case None => println("Correct")
        }

        // Print all values.
        c.iterator() foreach println
    }

    /**
     * This method will register listener for cache events on all nodes,
     * so we can actually see what happens underneath locally and remotely.
     */
    def registerListener() {
        val g = ignite$

        g *< (() => {
            val lsnr = new IgnitePredicate[Event] {
                override def apply(e: Event): Boolean = {
                    println(e.shortDisplay)

                    true
                }
            }

            if (g.cluster().nodeLocalMap[String, AnyRef].putIfAbsent("lsnr", lsnr) == null) {
                g.events().localListen(lsnr,
                    EVT_CACHE_OBJECT_PUT,
                    EVT_CACHE_OBJECT_READ,
                    EVT_CACHE_OBJECT_REMOVED)

                println("Listener is registered.")
            }
        }, null)
    }
}
