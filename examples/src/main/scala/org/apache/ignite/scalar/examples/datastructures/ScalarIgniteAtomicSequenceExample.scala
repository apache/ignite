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

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates a simple usage of distributed atomic sequence.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
class ScalarIgniteAtomicSequenceExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Number of retries */
    private val RETRIES = 20

    scalar(CONFIG) {
        println()
        println(">>> Cache atomic sequence example started.")

        val seqName = "example-sequence"

        // Try increment atomic sequence on all cluster nodes. Note that this node is also part of the cluster.
        ignite$.compute.broadcast(toRunnable(() => {
            val seq = atomicSequence$(seqName, 0, true)

            val firstVal = seq.get

            println("Sequence initial value on local node: " + firstVal)

            for (i <- 0 until RETRIES)
                println("Sequence [currentValue=" + seq.get + ", afterIncrement=" + seq.incrementAndGet + ']')

            println("Sequence after incrementing [expected=" + (firstVal + RETRIES) + ", actual=" + seq.get + ']')
        }))

        println()
        println("Finished atomic sequence example...")
        println("Check all nodes for output (this node is also part of the cluster).")
        println()
    }
}

object ScalarIgniteAtomicSequenceExampleStartup extends ScalarIgniteAtomicSequenceExample
