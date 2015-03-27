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
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarClosureExample extends App {
    scalar("examples/config/example-ignite.xml") {
        topology()
        helloWorld()
        helloWorld2()
        broadcast()
        greetRemotes()
        greetRemotesAgain()
    }

    /**
     * Prints ignite topology.
     */
    def topology() {
        ignite$ foreach (n => println("Node: " + nid8$(n)))
    }

    /**
     * Obligatory example (2) - cloud enabled Hello World!
     */
    def helloWorld2() {
        // Notice the example usage of Java-side closure 'F.println(...)' and method 'scala'
        // that explicitly converts Java side object to a proper Scala counterpart.
        // This method is required since implicit conversion won't be applied here.
        ignite$.run$(for (w <- "Hello World!".split(" ")) yield () => println(w), null)
    }

    /**
     * Obligatory example - cloud enabled Hello World!
     */
    def helloWorld() {
        ignite$.run$("HELLO WORLD!".split(" ") map (w => () => println(w)), null)
    }

    /**
     * One way to execute closures on the ignite cluster.
     */
    def broadcast() {
        ignite$.bcastRun(() => println("Broadcasting!!!"), null)
    }

    /**
     *  Greats all remote nodes only.
     */
    def greetRemotes() {
        val me = ignite$.cluster().localNode.id

        // Note that usage Java-based closure.
        ignite$.cluster().forRemotes() match {
            case p if p.isEmpty => println("No remote nodes!")
            case p => p.bcastRun(() => println("Greetings from: " + me), null)
        }
    }

    /**
     * Same as previous greetings for all remote nodes but remote cluster group is filtered manually.
     */
    def greetRemotesAgain() {
        val me = ignite$.cluster().localNode.id

        // Just show that we can create any groups we like...
        // Note that usage of Java-based closure via 'F' typedef.
        ignite$.cluster().forPredicate((n: ClusterNode) => n.id != me) match {
            case p if p.isEmpty => println("No remote nodes!")
            case p => p.bcastRun(() => println("Greetings again from: " + me), null)
        }
    }
}
