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

package org.apache.ignite.scalar.examples.datastructures

import java.util.UUID

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates a simple usage of distributed atomic stamped.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarIgniteAtomicStampedExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println(">>> Atomic stamped example started.")

        // Make name of atomic stamped.
        val stampedName = UUID.randomUUID.toString

        // Make value of atomic stamped.
        val value = UUID.randomUUID.toString

        // Make stamp of atomic stamped.
        val stamp = UUID.randomUUID.toString

        // Initialize atomic stamped.
        val stamped = atomicStamped$[String, String](stampedName, value, stamp, true)

        println("Atomic stamped initial [value=" + stamped.value + ", stamp=" + stamped.stamp + ']')

        // Make closure for checking atomic stamped.
        val c: IgniteRunnable = () => {
            val stamped = atomicStamped$[String, String](stampedName, null, null, true)

            println("Atomic stamped [value=" + stamped.value + ", stamp=" + stamped.stamp + ']')
        }

        // Check atomic stamped on all cluster nodes.
        ignite$.compute.broadcast(c)

        // Make new value of atomic stamped.
        val newVal = UUID.randomUUID.toString

        // Make new stamp of atomic stamped.
        val newStamp = UUID.randomUUID.toString

        println("Try to change value and stamp of atomic stamped with wrong expected value and stamp.")

        stamped.compareAndSet("WRONG EXPECTED VALUE", newVal, "WRONG EXPECTED STAMP", newStamp)

        // Check atomic stamped on all cluster nodes.
        // Atomic stamped value and stamp shouldn't be changed.
        ignite$.compute.run(c)

        println("Try to change value and stamp of atomic stamped with correct value and stamp.")

        stamped.compareAndSet(value, newVal, stamp, newStamp)

        // Check atomic stamped on all cluster nodes.
        // Atomic stamped value and stamp should be changed.
        ignite$.compute.run(c)

        println()
        println("Finished atomic stamped example...")
        println("Check all nodes for output (this node is also part of the cluster).")
    }
}
