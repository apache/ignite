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

import org.apache.ignite.IgniteAtomicReference
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates a simple usage of distributed atomic long.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarIgniteAtomicReferenceExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println
        println(">>> Atomic reference example started.")

        // Make name of atomic reference.
        val refName = UUID.randomUUID.toString

        // Make value of atomic reference.
        val value = UUID.randomUUID.toString

        // Initialize atomic reference.
        val ref: IgniteAtomicReference[String] = reference$(refName, value, true)

        println("Atomic reference initial value : " + ref.get + '.')

        // Make closure for checking atomic reference value on cluster.
        val c = new ReferenceClosure(refName)

        // Check atomic reference on all cluster nodes.
        ignite$.compute.run(c)

        // Make new value of atomic reference.
        val newVal = UUID.randomUUID.toString

        println("Try to change value of atomic reference with wrong expected value.")

        ref.compareAndSet("WRONG EXPECTED VALUE", newVal)

        // Check atomic reference on all cluster nodes.
        // Atomic reference value shouldn't be changed.
        ignite$.compute.run(c)

        println("Try to change value of atomic reference with correct expected value.")

        ref.compareAndSet(value, newVal)

        // Check atomic reference on all cluster nodes.
        // Atomic reference value should be changed.
        ignite$.compute.run(c)
    
        println()
        println("Finished atomic reference example...")
        println("Check all nodes for output (this node is also part of the cluster).")
    }
}

/**
  * Obtains atomic reference.
  *
  * @param refName Reference name.
  */
private class ReferenceClosure(refName: String) extends IgniteRunnable {
    /** @inheritdoc */
    def run() {
        val ref = reference$(refName, null, true)

        println("Atomic reference value is " + ref.get + '.')
    }
}
