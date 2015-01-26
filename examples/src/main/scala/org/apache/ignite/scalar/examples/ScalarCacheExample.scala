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

import org.apache.ignite.cache.CacheEntry
import org.apache.ignite.events.{IgniteEventType, IgniteEvent}
import org.apache.ignite.lang.IgnitePredicate
import org.gridgain.scalar.scalar
import scalar._
import IgniteEventType._
import collection.JavaConversions._

/**
 * Demonstrates basic In-Memory Data Grid operations with Scalar.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: `'ggstart.sh examples/config/example-cache.xml'`. Local node can
 * be started with or without cache.
 */
object ScalarCacheExample extends App {
    /** Name of cache specified in spring configuration. */
    private val NAME = "partitioned"

    scalar("examples/config/example-cache.xml") {
        // Clean up caches on all nodes before run.
        cache$(NAME).get.globalClearAll(0)

        registerListener()

        basicOperations()
        twoViewsOneCache()
    }

    /**
     * Demos basic cache operations.
     */
    def basicOperations() {
        // Create cache predicate-based projection (all values > 30).
        val c = cache$(NAME).get.viewByType(classOf[String], classOf[Int]).
            viewByKv((k: String, v: Int) => v < 30)

        // Add few values.
        c += (1.toString -> 1)
        c += (2.toString -> 2)

        // Update values.
        c += (1.toString -> 11)
        c += (2.toString -> 22)

        // These should be filtered out by projection.
        c += (1.toString -> 31)
        c += (2.toString -> 32)
        c += ((2.toString, 32))

        // Remove couple of keys (if any).
        c -= (11.toString, 22.toString)

        // Put one more value.
        c += (3.toString -> 11)

        val gt10 = (e: CacheEntry[String, Int]) => e.peek() > 10

        // These should pass the predicate.
        // Note that the predicate checks current state of entry, not the new value.
        c += (3.toString -> 9, gt10)

        // These should not pass the predicate
        // because value less then 10 was put on previous step.
        c += (3.toString -> 8, gt10)
        c += (3.toString -> 12, gt10)

        // Get with option...
        c.opt(44.toString) match {
            case Some(v) => sys.error("Should never happen.")
            case None => println("Correct")
        }

        // Print all projection values.
        c.values foreach println
    }

    /**
     * Demos basic type projections.
     */
    def twoViewsOneCache() {
        // Create two typed views on the same cache.
        val view1 = cache$(NAME).get.viewByType(classOf[String], classOf[Int])
        val view2 = cache$(NAME).get.viewByType(classOf[Int], classOf[String])

        view1 += ("key1" -> 1)
        view1 += ("key2" -> 2)

        // Attempt to update with predicate (will not update due to predicate failing).
        view1 += ("key2" -> 3, (k: String, v: Int) => v != 2)

        view2 += (1 -> "val1")
        view2 += (2 -> "val2")

        println("Values in view1:")
        view1.values foreach println
        println("view1 size is: " + view1.size)

        println("Values in view2:")
        view2.values foreach println
        println("view2 size is: " + view2.size)
    }

    /**
     * This method will register listener for cache events on all nodes,
     * so we can actually see what happens underneath locally and remotely.
     */
    def registerListener() {
        val g = grid$

        g *< (() => {
            val lsnr = new IgnitePredicate[IgniteEvent] {
                override def apply(e: IgniteEvent): Boolean = {
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
