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

package org.apache.ignite.scalar.examples.events

import java.util.UUID

import org.apache.ignite.IgniteException
import org.apache.ignite.compute.ComputeTaskSession
import org.apache.ignite.events.EventType._
import org.apache.ignite.events.TaskEvent
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.lang.{IgniteBiPredicate, IgnitePredicate, IgniteRunnable}
import org.apache.ignite.resources.TaskSessionResource
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * Note that ignite events are disabled by default and must be specifically enabled,
 * just like in `examples/config/example-ignite.xml` file.
 * <p>
 * Remote nodes should always be started with configuration: `'ignite.sh examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start
 * node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarEventsExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println(">>> Events API example started.")

        localListen()

        remoteListen()

        Thread.sleep(1000)
    }

    /**
     * Listen to events that happen only on local node.
     *
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def localListen() {
        println()
        println(">>> Local event listener example.")

        val lsnr = new IgnitePredicate[TaskEvent] {
            def apply(evt: TaskEvent): Boolean = {
                println("Received task event [evt=" + evt.name + ", taskName=" + evt.taskName + ']')

                true
            }
        }

        ignite$.events.localListen(lsnr, EVTS_TASK_EXECUTION: _*)

        ignite$.compute().withName("example-event-task").run(() => { println("Executing sample job.") })
        ignite$.events.stopLocalListen(lsnr)
    }

    /**
     * Listen to events coming from all cluster nodes.
     *
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def remoteListen() {
        println()
        println(">>> Remote event listener example.")

        val locLsnr = new IgniteBiPredicate[UUID, TaskEvent] {
            def apply(nodeId: UUID, evt: TaskEvent): Boolean = {
                assert(evt.taskName.startsWith("good-task"))

                println("Received task event [evt=" + evt.name + ", taskName=" + evt.taskName)

                true
            }
        }

        val rmtLsnr = new IgnitePredicate[TaskEvent] {
            def apply(evt: TaskEvent): Boolean = {
                evt.taskName.startsWith("good-task")
            }
        }

        val ignite = ignite$

        ignite.events.remoteListen(locLsnr, rmtLsnr, EVTS_TASK_EXECUTION: _*)

        for (i <- 0 until 10) {
            ignite.compute.withName(if (i < 5) "good-task-" + i else "bad-task-" + i).run(new IgniteRunnable {
                @TaskSessionResource private val ses: ComputeTaskSession = null

                override def run() {
                    println("Executing sample job for task: " + ses.getTaskName)
                }
            })
        }
    }
}
