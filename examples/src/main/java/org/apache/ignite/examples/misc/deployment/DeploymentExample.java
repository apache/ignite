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

package org.apache.ignite.examples.misc.deployment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.jetbrains.annotations.Nullable;

/**
 * Demonstrates how to explicitly deploy a task. Note that
 * it is very rare when you would need such functionality as tasks are
 * auto-deployed on demand first time you execute them. So in most cases
 * you would just apply any of the {@code Ignite.execute(...)} methods directly.
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
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public final class DeploymentExample {
    /** Name of the deployed task. */
    static final String TASK_NAME = "ExampleTask";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Deployment example started.");

            // This task will be deployed on local node and then peer-loaded
            // onto remote nodes on demand. For this example this task is
            // available on the classpath, however in real life that may not
            // always be the case. In those cases you should use explicit
            // 'IgniteCompute.localDeployTask(Class, ClassLoader) apply and
            // then use 'IgniteCompute.execute(String, Object)' method
            // passing your task name as first parameter.
            ignite.compute().localDeployTask(ExampleTask.class, ExampleTask.class.getClassLoader());

            for (Map.Entry<String, Class<? extends ComputeTask<?, ?>>> e : ignite.compute().localTasks().entrySet())
                System.out.println(">>> Found locally deployed task [alias=" + e.getKey() + ", taskCls=" + e.getValue());

            // Execute the task passing its name as a parameter. The system will find
            // the deployed task by its name and execute it.
            ignite.compute().execute(TASK_NAME, null);

            // Execute the task passing class name as a parameter. The system will find
            // the deployed task by its class name and execute it.
            // g.compute().execute(ExampleTask.class.getName(), null).get();

            // Undeploy task
            ignite.compute().undeployTask(TASK_NAME);

            System.out.println();
            System.out.println(">>> Finished executing Ignite Direct Deployment Example.");
            System.out.println(">>> Check participating nodes output.");
        }
    }

    /**
     * Example task used to demonstrate direct task deployment through API.
     * For this example this task as available on the classpath, however
     * in real life that may not always be the case. In those cases
     * you should use explicit {@link IgniteCompute#localDeployTask(Class, ClassLoader)} apply and
     * then use {@link IgniteCompute#execute(String, Object)}
     * method passing your task name as first parameter.
     * <p>
     * Note that this task specifies explicit task name. Task name is optional
     * and is added here for demonstration purpose. If not provided, it will
     * default to the task class name.
     */
    @ComputeTaskName(TASK_NAME)
    public static class ExampleTask extends ComputeTaskSplitAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int clusterSize, String arg) {
            Collection<ComputeJob> jobs = new ArrayList<>(clusterSize);

            for (int i = 0; i < clusterSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    @Nullable @Override public Serializable execute() {
                        System.out.println(">>> Executing deployment example job on this node.");

                        // This job does not return any result.
                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}