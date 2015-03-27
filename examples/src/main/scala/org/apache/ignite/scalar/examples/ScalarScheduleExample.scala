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

import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates a cron-based `Runnable` execution scheduling.
 * Test runnable object broadcasts a phrase to all cluster nodes every minute
 * three times with initial scheduling delay equal to five seconds.
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarScheduleExample extends App {
    scalar("examples/config/example-ignite.xml") {
        println()
        println("Compute schedule example started.")

        val g = ignite$

        var invocations = 0

        // Schedule callable that returns incremented value each time.
        val fut = ignite$.scheduleLocalCall(
            () => {
                invocations += 1

                g.bcastRun(() => {
                    println()
                    println("Howdy! :)")
                }, null)

                invocations
            },
            "{5, 3} * * * * *" // Cron expression.
        )

        while (!fut.isDone)
            println(">>> Invocation #: " + fut.get)

        // Prints.
        println()
        println(">>> Schedule future is done and has been unscheduled.")
        println(">>> Check all nodes for hello message output.")
    }
}
