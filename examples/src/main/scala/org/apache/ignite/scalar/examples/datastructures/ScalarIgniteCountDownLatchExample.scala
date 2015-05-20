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
 * Demonstrates a simple usage of distributed count down latch.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with '@code examples/config/example-ignite.xml' configuration.
 */
object ScalarIgniteCountDownLatchExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Number of latch initial count */
    private val INITIAL_COUNT = 10

    scalar(CONFIG) {
        println()
        println(">>> Cache atomic countdown latch example started.")

        // Make name of count down latch.
        val latchName = UUID.randomUUID.toString

        // Initialize count down latch.
        val latch = countDownLatch$(latchName, INITIAL_COUNT, false, true)

        println("Latch initial value: " + latch.count)

        // Start waiting on the latch on all cluster nodes.
        for (i <- 0 until INITIAL_COUNT)
            ignite$.compute.run(toRunnable(() => {
                val latch = countDownLatch$(latchName, 1, false, true)

                val newCnt = latch.countDown

                println("Counted down [newCnt=" + newCnt + ", nodeId=" + cluster$.localNode.id + ']')
            }))

        // Wait for latch to go down which essentially means that all remote closures completed.
        latch.await()

        println("All latch closures have completed.")

        println()
        println("Finished count down latch example...")
        println("Check all nodes for output (this node is also part of the cluster).")
    }
}
