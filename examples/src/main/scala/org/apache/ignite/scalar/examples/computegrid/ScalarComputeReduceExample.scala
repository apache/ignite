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

package org.apache.ignite.scalar.examples.computegrid

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Arrays => JavaArrays}

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgniteReducer
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates a simple use of Ignite with reduce closure.
 * <p>
 * Phrase is split into words and distributed across nodes where length of each word is
 * calculated. Then total phrase length is calculated using reducer.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start Ignite node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarComputeReduceExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"
   
    scalar(CONFIG) {
        println()
        println("Compute reducer example started.")

        val sum = ignite$.compute.apply(toClosure((word: String) => {
            println()
            println(">>> Printing '" + word + "' on this node from ignite job.")

            // Return number of letters in the word.
            word.length
        }),
            // Job parameters. Ignite will create as many jobs as there are parameters.
            JavaArrays.asList("Count characters using reducer".split(" "):_*),

            // Reducer to process results as they come.
            new IgniteReducer[Int, Int] {
                private val sum = new AtomicInteger

                // Callback for every job result.
                @impl def collect(len: Int): Boolean = {
                    sum.addAndGet(len)

                    // Return true to continue waiting until all results are received.
                    true
                }

                // Reduce all results into one.
                @impl def reduce(): Int = {
                    sum.get
                }
            })

        println()
        println(">>> Total number of characters in the phrase is '" + sum + "'.")
        println(">>> Check all nodes for output (this node is also part of the cluster).")
    }
}
