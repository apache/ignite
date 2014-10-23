/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.design.compute.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.loadbalancing.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Defines compute grid functionality for executing tasks and closures over nodes
 * in the {@link GridProjection}. Instance of {@code GridCompute} is obtained from grid projection
 * as follows:
 * <pre name="code" class="java">
 * GridCompute c = GridGain.grid().compute();
 * </pre>
 * The methods are grouped as follows:
 * <ul>
 * <li>{@code apply(...)} methods execute {@link GridClosure} jobs over nodes in the projection.</li>
 * <li>
 *     {@code call(...)} methods execute {@link Callable} jobs over nodes in the projection.
 *     Use {@link GridCallable} for better performance as it implements {@link Serializable}.
 * </li>
 * <li>
 *     {@code run(...)} methods execute {@link Runnable} jobs over nodes in the projection.
 *     Use {@link GridRunnable} for better performance as it implements {@link Serializable}.
 * </li>
 * <li>{@code broadcast(...)} methods broadcast jobs to all nodes in the projection.</li>
 * <li>{@code affinity(...)} methods colocate jobs with nodes on which a specified key is cached.</li>
 * </ul>
 * Note that if attempt is made to execute a computation over an empty projection (i.e. projection that does
 * not have any alive nodes), then {@link GridEmptyProjectionException} will be thrown out of result future.
 * <h1 class="header">Serializable</h1>
 * Also note that {@link Runnable} and {@link Callable} implementations must support serialization as required
 * by the configured marshaller. For example, {@link GridOptimizedMarshaller} requires {@link Serializable}
 * objects by default, but can be configured not to. Generally speaking objects that implement {@link Serializable}
 * or {@link Externalizable} will perform better. For {@link Runnable} and {@link Callable} interfaces
 * GridGain provides analogous {@link GridRunnable} and {@link GridCallable} classes which are
 * {@link Serializable} and should be used to run computations on the grid.
 * <h1 class="header">Load Balancing</h1>
 * In all cases other than {@code broadcast(...)}, GridGain must select a node for a computation
 * to be executed. The node will be selected based on the underlying {@link GridLoadBalancingSpi},
 * which by default sequentially picks next available node from grid projection. Other load balancing
 * policies, such as {@code random} or {@code adaptive}, can be configured as well by selecting
 * a different load balancing SPI in grid configuration. If your logic requires some custom
 * load balancing behavior, consider implementing {@link GridComputeTask} directly.
 * <h1 class="header">Fault Tolerance</h1>
 * GridGain guarantees that as long as there is at least one grid node standing, every job will be
 * executed. Jobs will automatically failover to another node if a remote node crashed
 * or has rejected execution due to lack of resources. By default, in case of failover, next
 * load balanced node will be picked for job execution. Also jobs will never be re-routed to the
 * nodes they have failed on. This behavior can be changed by configuring any of the existing or a custom
 * {@link GridFailoverSpi} in grid configuration.
 * <h1 class="header">Resource Injection</h1>
 * All compute jobs, including closures, runnables, callables, and tasks can be injected with
 * grid resources. Both, field and method based injections are supported. The following grid
 * resources can be injected:
 * <ul>
 * <li>{@link GridTaskSessionResource}</li>
 * <li>{@link GridInstanceResource}</li>
 * <li>{@link GridLoggerResource}</li>
 * <li>{@link GridHomeResource}</li>
 * <li>{@link GridExecutorServiceResource}</li>
 * <li>{@link GridLocalNodeIdResource}</li>
 * <li>{@link GridMBeanServerResource}</li>
 * <li>{@link GridMarshallerResource}</li>
 * <li>{@link GridSpringApplicationContextResource}</li>
 * <li>{@link GridSpringResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * Here is an example of how to inject instance of {@link Grid} into a computation:
 * <pre name="code" class="java">
 * public class MyGridJob extends GridRunnable {
 *      ...
 *      &#64;GridInstanceResource
 *      private Grid grid;
 *      ...
 *  }
 * </pre>
 * <h1 class="header">Computation SPIs</h1>
 * Note that regardless of which method is used for executing computations, all relevant SPI implementations
 * configured for this grid instance will be used (i.e. failover, load balancing, collision resolution,
 * checkpoints, etc.). If you need to override configured defaults, you should use compute task together with
 * {@link GridComputeTaskSpis} annotation. Refer to {@link GridComputeTask} documentation for more information.
 */
public interface IgniteCompute extends ComputeTopology {
    /** {@inheritDoc} */
    @Override IgniteCompute enableAsync();

    /**
     * Executes given job on the node where data for provided affinity key is located
     * (a.k.a. affinity co-location).
     *
     * @param cacheName Name of the cache to use for affinity co-location.
     * @param affKey Affinity key.
     * @param job Job which will be co-located on the node with given affinity key.
     * @return Future for this execution.
     * @see GridComputeJobContext#cacheName()
     * @see GridComputeJobContext#affinityKey()
     */
    public GridFuture<?> affinityRun(@Nullable String cacheName, Object affKey, Runnable job);

    /**
     * Executes given job on the node where data for provided affinity key is located
     * (a.k.a. affinity co-location).
     *
     * @param cacheName Name of the cache to use for affinity co-location.
     * @param affKey Affinity key.
     * @param job Job which will be co-located on the node with given affinity key.
     * @return Future with job result.
     * @see GridComputeJobContext#cacheName()
     * @see GridComputeJobContext#affinityKey()
     */
    public <R> GridFuture<R> affinityCall(@Nullable String cacheName, Object affKey, Callable<R> job);

    /**
     * Gets task future based on execution session ID. If task execution was started on local node and this
     * projection includes local node then the future for this task will be returned.
     *
     * @param sesId Session ID for task execution.
     * @param <R> Task result type.
     * @return Task future if task was started on this node and this node belongs to this projection,
     *      or {@code null} otherwise.
     */
    @Nullable public <R> GridComputeTaskFuture<R> taskFuture(GridUuid sesId);

    /**
     * Cancels task with the given execution session ID, if it is currently running inside this projection
     * or on local node. Note that if local node is master for the task, task gets alway cancelled, even
     * if local node is not the part of the projection.
     *
     * @param sesId Execution session ID.
     * @throws GridException If task cancellation failed.
     */
    public void cancelTask(GridUuid sesId) throws GridException;

    /**
     * Cancels job with the given job ID, if it is currently running inside this projection.
     *
     * @param jobId Job ID.
     * @throws GridException If task cancellation failed.
     */
    public void cancelJob(GridUuid jobId) throws GridException;

    /**
     * Explicitly deploys a task with given class loader on the local node. Upon completion of this method,
     * a task can immediately be executed on the grid, considering that all participating
     * remote nodes also have this task deployed.
     * <p>
     * Note that tasks are automatically deployed upon first execution (if peer-class-loading is enabled),
     * so use this method only when the provided class loader is different from the
     * {@code taskClass.getClassLoader()}.
     * <p>
     * Another way of class deployment is deployment from local class path.
     * Classes from local class path always have a priority over P2P deployed ones.
     * <p>
     * Note that class can be deployed multiple times on remote nodes, i.e. re-deployed. GridGain
     * maintains internal version of deployment for each instance of deployment (analogous to
     * class and class loader in Java). Execution happens always on the latest deployed instance.
     * <p>
     * This method has no effect if the class passed in was already deployed.
     *
     * @param taskCls Task class to deploy. If task class has {@link GridComputeTaskName} annotation,
     *      then task will be deployed under the name specified within annotation. Otherwise, full
     *      class name will be used as task's name.
     * @param clsLdr Task class loader. This class loader is in charge
     *      of loading all necessary resources for task execution.
     * @throws GridException If task is invalid and cannot be deployed.
     */
    public void localDeployTask(Class<? extends GridComputeTask> taskCls, ClassLoader clsLdr) throws GridException;

    /**
     * Gets map of all locally deployed tasks keyed by their task name .
     *
     * @return Map of locally deployed tasks keyed by their task name.
     */
    public Map<String, Class<? extends GridComputeTask<?, ?>>> localTasks();

    /**
     * Makes the best attempt to undeploy a task with given name from this grid projection. Note that this
     * method returns immediately and does not wait until the task will actually be
     * undeployed on every node.
     *
     * @param taskName Name of the task to undeploy.
     * @throws GridException Thrown if undeploy failed.
     */
    public void undeployTask(String taskName) throws GridException;
}
