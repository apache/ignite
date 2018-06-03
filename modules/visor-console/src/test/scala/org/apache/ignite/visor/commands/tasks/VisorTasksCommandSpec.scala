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

package org.apache.ignite.visor.commands.tasks

import org.apache.ignite.Ignition
import org.apache.ignite.compute.{ComputeJob, ComputeJobAdapter, ComputeJobResult, ComputeTaskSplitAdapter}
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.events.EventType._
import org.apache.ignite.visor.visor
import org.scalatest._

import java.util

import org.apache.ignite.visor.commands.open.VisorOpenCommand._
import org.apache.ignite.visor.commands.tasks.VisorTasksCommand._

import scala.collection.JavaConversions._
import scala.collection._

/**
 * Unit test for 'tasks' command.
 */
class VisorTasksCommandSpec extends FunSpec with Matchers with BeforeAndAfterAll {
    /**
     * Open visor and execute several tasks before all tests.
     */
    override def beforeAll() {
        val ignite = Ignition.start(config("grid-1"))

        Ignition.start(config("grid-2"))

        visor.open(config("visor-demo-node"), "n/a")

        try {
            val compute = ignite.compute()

            val fut1 = compute.withName("TestTask1").executeAsync(new TestTask1(), null)

            val fut2 = compute.withName("TestTask1").executeAsync(new TestTask1(), null)

            val fut3 = compute.withName("TestTask1").executeAsync(new TestTask1(), null)

            val fut4 = compute.withName("TestTask2").executeAsync(new TestTask2(), null)

            val fut5 = compute.withName("Test3").executeAsync(new Test3(), null)

            fut1.get
            fut2.get
            fut3.get
            fut4.get
            fut5.get
        }
        catch {
            case _: Exception =>
        }
    }

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Ignite instance name.
     * @return Grid configuration.
     */
    private def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setIgniteInstanceName(name)
        cfg.setIncludeEventTypes(EVTS_ALL: _*)

        cfg
    }

    /**
     * Close visor after all tests.
     */
    override def afterAll() {
        visor.close()

        Ignition.stopAll(false)
    }

    describe("A 'tasks' visor command") {
        it("should print tasks when called w/o arguments") {
            visor.tasks()
        }

        it("should print error message with incorrect argument") {
            visor.tasks("-xx")
        }

        it("should print task summary when called for specific task") {
            visor.tasks("-n=@t1")
        }

        it("should print execution when called for specific execution") {
            visor.tasks("-e=@e1")
        }

        it("should print all tasks") {
            visor.tasks("-l")
        }

        it("should print all tasks and executions") {
            visor.tasks("-l -a")
        }

        it("should print tasks that started during last 5 seconds") {
            visor.tasks("-l -t=5s")
        }

        it("should print error message about invalid time period") {
            visor.tasks("-l -t=x2s")
        }

        it("should print error message about negative time period") {
            visor.tasks("-l -t=-10s")
        }

        it("should print error message about invalid time period specification") {
            visor.tasks("-l -t=10x")
        }

        it("should print task summary for the first task") {
            visor.tasks("-n=TestTask1")
        }

        it("should print task summary and executions for the first task") {
            visor.tasks("-n=TestTask1 -a")
        }

        it("should print list of tasks grouped by nodes") {
            visor.tasks("-g")
        }

        it("should print list of tasks that started during last 5 minutes grouped by nodes") {
            visor.tasks("-g -t=5m")
        }

        it("should print list of tasks grouped by hosts") {
            visor.tasks("-h")
        }

        it("should print list of tasks that started during last 5 minutes grouped by hosts") {
            visor.tasks("-h -t=5m")
        }

        it("should print list of tasks filtered by substring") {
            visor.tasks("-s=TestTask")
        }

        it("should print list of tasks and executions filtered by substring") {
            visor.tasks("-s=TestTask -a")
        }
    }
}

/**
 * Test task 1.
 */
private class TestTask1 extends ComputeTaskSplitAdapter[String, Void] {
    def split(gridSize: Int, arg: String): java.util.Collection[_ <: ComputeJob] = {
        Iterable.fill(gridSize)(new ComputeJobAdapter() {
            def execute() = {
                println("Task 1")

                null
            }
        })
    }

    def reduce(results: util.List[ComputeJobResult]) = null
}

/**
 * Test task 2.
 */
private class TestTask2 extends ComputeTaskSplitAdapter[String, Void] {
    def split(gridSize: Int, arg: String): java.util.Collection[_ <: ComputeJob] = {
        Iterable.fill(gridSize)(new ComputeJobAdapter() {
            def execute() = {
                println("Task 2")

                null
            }
        })
    }

    def reduce(results: util.List[ComputeJobResult]) = null
}

/**
 * Test task 3 (w/o 'Task' in host for testing '-s' option).
 */
private class Test3 extends ComputeTaskSplitAdapter[String, Void] {
    def split(gridSize: Int, arg: String): java.util.Collection[_ <: ComputeJob] = {
        Iterable.fill(gridSize)(new ComputeJobAdapter() {
            def execute() = {
                println("Task 3")

                null
            }
        })
    }

    def reduce(results: util.List[ComputeJobResult]) = null
}
