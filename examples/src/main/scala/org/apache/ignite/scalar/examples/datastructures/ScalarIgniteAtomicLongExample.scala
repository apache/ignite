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

package org.apache.ignite.scalar.examples.datastructures

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import java.util.UUID

/**
 * Demonstrates a simple usage of distributed atomic long.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
class ScalarIgniteAtomicLongExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Number of retries */
    private val RETRIES = 20

    scalar(CONFIG) {
        println()
        println(">>> Atomic long example started.")

        // Make name for atomic long (by which it will be known in the cluster).
        val atomicName = UUID.randomUUID.toString

        // Initialize atomic long.
        val atomicLong = long$(atomicName, true)

        println()
        println("Atomic long initial value : " + atomicLong.get + '.')

        val ignite = ignite$

        // Try increment atomic long from all nodes.
        // Note that this node is also part of the ignite cluster.
        ignite.compute.broadcast(toRunnable(() => {
            for (i <- 0 until RETRIES)
                println("AtomicLong value has been incremented: " + atomicLong.incrementAndGet)
        }))

        println()
        println("Atomic long value after successful CAS: " + atomicLong.get)
    }
}

/**
 * Object to run ScalarIgniteAtomicLongExample.
 */
object ScalarIgniteAtomicLongExampleStartup extends ScalarIgniteAtomicLongExample
