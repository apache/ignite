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

package org.apache.ignite.scalar.examples

import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates various closure executions on the cloud using Scalar.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarClosureExample extends App {
    scalar("examples/config/example-compute.xml") {
        topology()
        helloWorld()
        helloWorld2()
        broadcast()
        greetRemotes()
        greetRemotesAgain()
    }

    /**
     * Prints grid topology.
     */
    def topology() {
        grid$ foreach (n => println("Node: " + nid8$(n)))
    }

    /**
     * Obligatory example (2) - cloud enabled Hello World!
     */
    def helloWorld2() {
        // Notice the example usage of Java-side closure 'F.println(...)' and method 'scala'
        // that explicitly converts Java side object to a proper Scala counterpart.
        // This method is required since implicit conversion won't be applied here.
        grid$.run$(for (w <- "Hello World!".split(" ")) yield () => println(w), null)
    }

    /**
     * Obligatory example - cloud enabled Hello World!
     */
    def helloWorld() {
        grid$.run$("HELLO WORLD!".split(" ") map (w => () => println(w)), null)
    }

    /**
     * One way to execute closures on the grid.
     */
    def broadcast() {
        grid$.bcastRun(() => println("Broadcasting!!!"), null)
    }

    /**
     *  Greats all remote nodes only.
     */
    def greetRemotes() {
        val me = grid$.cluster().localNode.id

        // Note that usage Java-based closure.
        grid$.cluster().forRemotes() match {
            case p if p.isEmpty => println("No remote nodes!")
            case p => p.bcastRun(() => println("Greetings from: " + me), null)
        }
    }

    /**
     * Same as previous greetings for all remote nodes but remote projection is created manually.
     */
    def greetRemotesAgain() {
        val me = grid$.cluster().localNode.id

        // Just show that we can create any projections we like...
        // Note that usage of Java-based closure via 'F' typedef.
        grid$.cluster().forPredicate((n: ClusterNode) => n.id != me) match {
            case p if p.isEmpty => println("No remote nodes!")
            case p => p.bcastRun(() => println("Greetings again from: " + me), null)
        }
    }
}
