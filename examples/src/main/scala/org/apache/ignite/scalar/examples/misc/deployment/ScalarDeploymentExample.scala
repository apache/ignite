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

package org.apache.ignite.scalar.examples.misc.deployment

import org.apache.ignite.IgniteCompute
import org.apache.ignite.compute._
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import org.jetbrains.annotations.Nullable

import java.util.{ArrayList => JavaArrayList, Collection => JavaCollection, List => JavaList}

import scala.collection.JavaConversions._

/**
 * Demonstrates how to explicitly deploy a task. Note that
 * it is very rare when you would need such functionality as tasks are
 * auto-deployed on demand first time you execute them. So in most cases
 * you would just apply any of the `Ignite.execute(...)` methods directly.
 * However, sometimes a task is not in local class path, so you may not even
 * know the code it will execute, but you still need to execute it. For example,
 * you have two independent components in the system, and one loads the task
 * classes from some external source and deploys it; then another component
 * can execute it just knowing the name of the task.
 * <p>
 * Also note that for simplicity of the example, the task we execute is
 * in system classpath, so even in this case the deployment step is unnecessary.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarDeploymentExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Name of the deployed task. */
    private[deployment] final val TASK_NAME = "ExampleTask"

    scalar(CONFIG) {
        println()
        println(">>> Deployment example started.")

        // This task will be deployed on local node and then peer-loaded
        // onto remote nodes on demand. For this example this task is
        // available on the classpath, however in real life that may not
        // always be the case. In those cases you should use explicit
        // 'IgniteCompute.localDeployTask(Class, ClassLoader) apply and
        // then use 'IgniteCompute.execute(String, Object)' method
        // passing your task name as first parameter.
        ignite$.compute.localDeployTask(classOf[ExampleTask], classOf[ExampleTask].getClassLoader)

        for (e <- ignite$.compute.localTasks.entrySet)
            println(">>> Found locally deployed task [alias=" + e.getKey + ", taskCls=" + e.getValue)

        // Execute the task passing its name as a parameter. The system will find
        // the deployed task by its name and execute it.
        ignite$.compute.execute(TASK_NAME, null)

        // Execute the task passing class name as a parameter. The system will find
        // the deployed task by its class name and execute it.
        // g.compute().execute(ExampleTask.class.getName(), null).get();
        // Undeploy task
        ignite$.compute.undeployTask(TASK_NAME)

        println()
        println(">>> Finished executing Ignite Direct Deployment Example.")
        println(">>> Check participating nodes output.")
    }

    /**
     * Example task used to demonstrate direct task deployment through API.
     * For this example this task as available on the classpath, however
     * in real life that may not always be the case. In those cases
     * you should use explicit [[IgniteCompute#localDeployTask(Class, ClassLoader)]] apply and
     * then use [[IgniteCompute#execute(String, Object)]]
     * method passing your task name as first parameter.
     * <p>
     * Note that this task specifies explicit task name. Task name is optional
     * and is added here for demonstration purpose. If not provided, it will
     * default to the task class name.
     */
    @ComputeTaskName(TASK_NAME) class ExampleTask extends ComputeTaskSplitAdapter[String, AnyRef] {
        @impl protected def split(clusterSize: Int, arg: String): JavaCollection[_ <: ComputeJob] = {
            val jobs = new JavaArrayList[ComputeJob](clusterSize)

            for (i <- 0 until clusterSize) {
                jobs.add(new ComputeJobAdapter {
                    @Nullable @impl override def execute(): AnyRef = {
                        println(">>> Executing deployment example job on this node.")

                        null
                    }
                })
            }

            jobs
        }

        @impl def reduce(results: JavaList[ComputeJobResult]): AnyRef = {
            null
        }
    }
}
