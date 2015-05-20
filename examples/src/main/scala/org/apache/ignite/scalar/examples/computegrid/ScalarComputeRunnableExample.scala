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
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates a simple use of [[IgniteRunnable]].
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarComputeRunnableExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println("Compute runnable example started.")

        val compute = ignite$.compute

        // Iterate through all words in the sentence and create runnable jobs.
        "Print words using runnable".split(" ").foreach(word => {
            compute.run(() => {
                println()
                println(">>> Printing '" + word + "' on this node from ignite job.")
            })
        })

        println()
        println(">>> Finished printing words using runnable execution.")
        println(">>> Check all nodes for output (this node is also part of the cluster).")
    }
}
