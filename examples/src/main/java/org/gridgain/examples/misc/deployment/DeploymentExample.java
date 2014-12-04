/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.deployment;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Demonstrates how to explicitly deploy a task. Note that
 * it is very rare when you would need such functionality as tasks are
 * auto-deployed on demand first time you execute them. So in most cases
 * you would just apply any of the {@code Grid.execute(...)} methods directly.
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
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-compute.xml} configuration.
 */
public final class DeploymentExample {
    /** Name of the deployed task. */
    static final String TASK_NAME = "ExampleTask";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Deployment example started.");

            // This task will be deployed on local node and then peer-loaded
            // onto remote nodes on demand. For this example this task is
            // available on the classpath, however in real life that may not
            // always be the case. In those cases you should use explicit
            // 'GridCompute.localDeployTask(Class, ClassLoader) apply and
            // then use 'GridCompute.execute(String, Object)' method
            // passing your task name as first parameter.
            g.compute().localDeployTask(ExampleTask.class, ExampleTask.class.getClassLoader());

            for (Map.Entry<String, Class<? extends GridComputeTask<?, ?>>> e : g.compute().localTasks().entrySet())
                System.out.println(">>> Found locally deployed task [alias=" + e.getKey() + ", taskCls=" + e.getValue());

            // Execute the task passing its name as a parameter. The system will find
            // the deployed task by its name and execute it.
            g.compute().execute(TASK_NAME, null);

            // Execute the task passing class name as a parameter. The system will find
            // the deployed task by its class name and execute it.
            // g.compute().execute(ExampleTask.class.getName(), null).get();

            // Undeploy task
            g.compute().undeployTask(TASK_NAME);

            System.out.println();
            System.out.println(">>> Finished executing Grid Direct Deployment Example.");
            System.out.println(">>> Check participating nodes output.");
        }
    }

    /**
     * Example task used to demonstrate direct task deployment through API.
     * For this example this task as available on the classpath, however
     * in real life that may not always be the case. In those cases
     * you should use explicit {@link org.apache.ignite.IgniteCompute#localDeployTask(Class, ClassLoader)} apply and
     * then use {@link org.apache.ignite.IgniteCompute#execute(String, Object)}
     * method passing your task name as first parameter.
     * <p>
     * Note that this task specifies explicit task name. Task name is optional
     * and is added here for demonstration purpose. If not provided, it will
     * default to the task class name.
     */
    @GridComputeTaskName(TASK_NAME)
    public static class ExampleTask extends GridComputeTaskSplitAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, String arg) throws GridException {
            Collection<GridComputeJob> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new GridComputeJobAdapter() {
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
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
