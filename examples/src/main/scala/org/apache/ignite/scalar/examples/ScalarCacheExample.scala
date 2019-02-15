/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.scalar.examples

import org.apache.ignite.events.Event
import org.apache.ignite.events.EventType._
import org.apache.ignite.lang.IgnitePredicate
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.collection.JavaConversions._

/**
 * Demonstrates basic In-Memory Data Ignite Cluster operations with Scalar.
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Name of cache specified in spring configuration. */
    private val NAME = ScalarCacheExample.getClass.getSimpleName

    scalar(CONFIG) {
        val cache = createCache$[String, Int](NAME)

        try {
            registerListener()

            basicOperations()
        }
        catch {
            case e: Throwable =>
                e.printStackTrace();
        }
        finally {
            cache.destroy()
        }
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

        try {
            c.opt(44.toString) match {
                case Some(v) => sys.error("Should never happen.")
                case _ => println("Correct")
            }
        }
        catch {
            case e: Throwable =>
                e.printStackTrace()
        }


        // Print all values.
        println("Print all values.")
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
