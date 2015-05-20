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

package org.apache.ignite.scalar.examples.computegrid

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgniteCallable
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteException}

import java.util.{Collection => JavaCollection}

import scala.collection.JavaConversions._

/**
 * Demonstrates broadcasting computations within cluster.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarComputeBroadcastExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println(">>> Compute broadcast example started.")

        // Print hello message on all nodes.
        hello(ignite$)

        // Gather system info from all nodes.
        gatherSystemInfo(ignite$)
    }

    /**
     * Print 'Hello' message on all nodes.
     *
     * @param ignite Ignite instance.
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def hello(ignite: Ignite) {
        ignite.compute.broadcast(toRunnable(() => {
            println()
            println(">>> Hello Node! :)")
        }))

        println()
        println(">>> Check all nodes for hello message output.")
    }

    /**
     * Gather system info from all nodes and print it out.
     *
     * @param ignite Ignite instance.
     * @throws IgniteException if failed.
     */
    @throws(classOf[IgniteException])
    private def gatherSystemInfo(ignite: Ignite) {
        val res: JavaCollection[String] = ignite.compute.broadcast(new IgniteCallable[String] {
            @IgniteInstanceResource private var ignite: Ignite = null

            @impl def call: String = {
                println()
                println("Executing task on node: " + ignite.cluster.localNode.id)

                "Node ID: " + ignite.cluster.localNode.id + "\n" +
                    "OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " +
                    System.getProperty("os.arch") + "\n" +
                    "User: " + System.getProperty("user.name") + "\n" +
                    "JRE: " + System.getProperty("java.runtime.name") + " " + System.getProperty("java.runtime.version")
            }
        })

        println()
        println("Nodes system information:")
        println()

        for (r <- res) {
            println(r)
            println()
        }
    }
}
