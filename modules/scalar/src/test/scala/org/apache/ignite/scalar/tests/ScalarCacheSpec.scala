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

package org.apache.ignite.scalar.tests

import org.apache.ignite.events.Event
import org.apache.ignite.events.EventType._
import org.apache.ignite.lang.IgnitePredicate
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._

/**
 * Scalar cache test.
 */
@RunWith(classOf[JUnitRunner])
class ScalarCacheSpec extends FunSpec with ShouldMatchers {
    private val CFG = "modules/scalar/src/test/resources/spring-cache.xml"

    describe("Scalar cache") {

        it("should work properly via Java APIs") {
            scalar(CFG) {
                registerListener()

                val c = cache$[Int, Int]("partitioned").get

                c.put(1, 1)
                c.put(2, 2)

                c.iterator() foreach println

                println("Size is: " + c.size())
            }
        }
    }

    /**
     * This method will register listener for cache events on all nodes,
     * so we can actually see what happens underneath locally and remotely.
     */
    def registerListener() {
        val g = ignite$

        g *< (() => {
            val lsnr = new IgnitePredicate[Event]() {
                override def apply(e: Event): Boolean = {
                    println(e.shortDisplay)

                    true
                }
            }

            if (g.cluster().nodeLocalMap[String, AnyRef].putIfAbsent("lsnr", lsnr) == null) {
                g.events.localListen(lsnr,
                    EVT_CACHE_OBJECT_PUT,
                    EVT_CACHE_OBJECT_READ,
                    EVT_CACHE_OBJECT_REMOVED)

                println("Listener is registered.")
            }
        }, null)
    }
}
