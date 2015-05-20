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

package org.apache.ignite.scalar.examples.misc.schedule

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import java.lang.{Integer => JavaInt}
import java.util.concurrent.Callable

/**
 * Demonstrates a cron-based [[Runnable]] execution scheduling.
 * Test runnable object broadcasts a phrase to all cluster nodes every minute
 * three times with initial scheduling delay equal to five seconds.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarComputeScheduleExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println("Compute schedule example started.")

        // Schedule output message every minute.
        val fut = ignite$.scheduler.scheduleLocal(new Callable[JavaInt] {
            private var invocations = 0

            @impl def call(): JavaInt = {
                invocations += 1

                ignite$.compute.broadcast(new IgniteRunnable {
                    @impl def run() {
                        println()
                        println("Howdy! :)")
                    }
                })

                invocations
            }
        }, "{5, 3} * * * * *")

        while (!fut.isDone)
            println(">>> Invocation #: " + fut.get)

        println()
        println(">>> Schedule future is done and has been unscheduled.")
        println(">>> Check all nodes for hello message output.")
    }
}
