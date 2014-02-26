// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.deployment.*;
import org.gridgain.grid.spi.loadbalancing.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Defines compute grid functionality for executing tasks and closures over nodes
 * in the projection. The methods are grouped as follows:
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
 * GridGain has analogous {@link GridRunnable} and {@link GridCallable} classes which are
 * {@link Serializable}.
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
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCompute {
    /**
     * Gets grid projection to which this {@code GridCompute} instance belongs.
     *
     * @return Grid projection to which this {@code GridCompute} instance belongs.
     */
    public GridProjection projection();

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
     * Executes given task on the grid projection. For step-by-step explanation of task execution process
     * refer to {@link GridComputeTask} documentation.
     *
     * @param taskCls Class of the task to execute. If class has {@link GridComputeTaskName} annotation,
     *      then task is deployed under a name specified within annotation. Otherwise, full
     *      class name is used as task name.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task future.
     */
    public <T, R> GridComputeTaskFuture<R> execute(Class<? extends GridComputeTask<T, R>> taskCls, @Nullable T arg);

    /**
     * Executes given task on the grid projection. For step-by-step explanation of task execution process
     * refer to {@link GridComputeTask} documentation.
     *
     * @param task Instance of task to execute. If task class has {@link GridComputeTaskName} annotation,
     *      then task is deployed under a name specified within annotation. Otherwise, full
     *      class name is used as task name.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task future.
     */
    public <T, R> GridComputeTaskFuture<R> execute(GridComputeTask<T, R> task, @Nullable T arg);

    /**
     * Executes given task on the grid projection. For step-by-step explanation of task execution process
     * refer to {@link GridComputeTask} documentation.
     * <p>
     * If task for given name has not been deployed yet, then {@code taskName} will be
     * used as task class name to auto-deploy the task (see {@link #localDeployTask(Class, ClassLoader)} method).
     *
     * @param taskName Name of the task to execute.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task future.
     * @see GridComputeTask for information about task execution.
     */
    public <T, R> GridComputeTaskFuture<R> execute(String taskName, @Nullable T arg);

    /**
     * Broadcasts given job to all nodes in grid projection.
     *
     * @param job Job to broadcast to all projection nodes.
     * @return Future for this execution.
     */
    public GridFuture<?> broadcast(Runnable job);

    /**
     * Broadcasts given job to all nodes in grid projection. Every participating node will return
     * job result. Collection of all returned job results is returned from the result future.
     *
     * @param job Job to broadcast to all projection nodes.
     * @return Future with collection of results for this execution.
     */
    public <R> GridFuture<Collection<R>> broadcast(Callable<R> job);

    /**
     * Broadcasts given closure job with passed in argument to all nodes in grid projection.
     * Every participating node will return job result. Collection of all returned job results
     * is returned from the result future.
     *
     * @param job Job to broadcast to all projection nodes.
     * @param arg Job closure argument.
     * @return Future with collection of results for this execution.
     */
    public <R, T> GridFuture<Collection<R>> broadcast(GridClosure<T, R> job, @Nullable T arg);

    /**
     * Executes job on a node in the grid projection. Node for execution is selected
     * using underlying load balancing SPI.
     *
     * @param job Job closure to execute.
     * @return Future of this execution.
     * @see GridLoadBalancingSpi
     */
    public GridFuture<?> run(Runnable job);

    /**
     * Executes collection of jobs on nodes within grid projection. For each job a next
     * load balanced node will be selected for execution. Nodes for execution are selected
     * using underlying load balancing SPI.
     *
     * @param jobs Collection of jobs to execute.
     * @return Future for this execution.
     * @see GridLoadBalancingSpi
     */
    public GridFuture<?> run(Collection<? extends Runnable> jobs);

    /**
     * Executes job on a node in the grid projection. Node for execution is selected
     * using underlying load balancing SPI.
     *
     * @param job Job closure to execute.
     * @return Future of this execution.
     * @see GridLoadBalancingSpi
     */
    public <R> GridFuture<R> call(Callable<R> job);

    /**
     * Executes collection of jobs on nodes within grid projection. For each job a next
     * load balanced node will be selected for execution. Nodes for execution are selected
     * using underlying load balancing SPI. Collection of all returned job results is
     * returned from the result future.
     *
     * @param jobs Collection of jobs to execute.
     * @return Future with collection of results for this execution.
     * @see GridLoadBalancingSpi
     */
    public <R> GridFuture<Collection<R>> call(Collection<? extends Callable<R>> jobs);

    /**
     * Executes given jobs on this projection.
     * <p>
     * This method will block until the execution is complete. All default SPI implementations
     * configured for this grid instance will be used (i.e. failover, load balancing, collision
     * resolution, etc.).
     * Note that if you need greater control on any aspects of Java code execution on the grid
     * you should implement {@link GridComputeTask} which will provide you with full control over the execution.
     * <p>
     * Here's a general example of the Java method that takes a text message and calculates its length
     * by splitting it by spaces, calculating the length of each word on individual (remote) grid node
     * and then summing (reducing) results from all nodes to produce the final length of the input string
     * using function APIs, typedefs, and execution closures on the grid:
     * <pre name="code" class="java">
     * public static int length(final String msg) throws GridException {
     *     return GridGain.grid().call(SPREAD, F.yield(msg.split(" "), F.cInvoke("length")), F.sumIntReducer());
     * }
     * </pre>
     * <p>
     * Note that class {@link GridAbsClosure} implements {@link Runnable} and class {@link GridOutClosure}
     * implements {@link Callable} interface. Note also that class {@link GridFunc} and typedefs provide rich
     * APIs and functionality for closures and predicates based processing in GridGain. While Java interfaces
     * {@link Runnable} and {@link Callable} allow for lowest common denominator for APIs - it is advisable
     * to use richer Functional Programming support provided by GridGain available in {@link org.gridgain.grid.lang}
     * package.
     * <p>
     * Notice that {@link Runnable} and {@link Callable} implementations must support serialization as required
     * by the configured marshaller. For example, JDK marshaller will require that implementations would
     * be serializable. Other marshallers, e.g. JBoss marshaller, may not have this limitation. Please consult
     * with specific marshaller implementation for the details. Note that all closures and predicates in
     * {@link org.gridgain.grid.lang} package are serializable and can be freely used in the distributed
     * context with all marshallers currently shipped with GridGain.
     *
     * @param jobs Closures to executes.
     * @param rdc Result reducing closure.
     * @return Value produced by reducing closure.
     * @see #withName(String)
     */
    public <R1, R2> GridFuture<R2> call(Collection<? extends Callable<R1>> jobs, GridReducer<R1, R2> rdc);

    /**
     * Runs job producing result with given argument on this projection.
     * <p>
     * This method doesn't block and immediately returns with future of execution.
     *
     * @param job Job to run.
     * @param arg Job argument.
     * @return Closure result future.
     * @see #call(Callable)
     * @see #withName(String)
     */
    public <R, T> GridFuture<R> apply(GridClosure<T, R> job, @Nullable T arg);

    /**
     * Runs job, taking argument and producing result on this projection with given
     * collection of arguments. The job is sequentially executed on every single
     * argument from the collection so that number of actual executions will be
     * equal to size of collection of arguments.
     * <p>
     * This method doesn't block and immediately returns with future of execution.
     *
     * @param job Job to run.
     * @param args Job arguments (closure free variables).
     * @return Future of job results collection.
     * @see #call(Callable)
     * @see #withName(String)
     */
    public <T, R> GridFuture<Collection<R>> apply(GridClosure<T, R> job, @Nullable Collection<? extends T> args);

    /**
     * Runs closure job with given collection of arguments. The job is sequentially
     * executed on every single argument from the collection so that number of actual
     * executions will be equal to size of collection of arguments. Then method reduces
     * these job results to a single execution result using provided reducer.
     *
     * @param job Job to run.
     * @param args Job arguments.
     * @param rdc Job result reducer.
     * @return Result reduced from job results with given reducer.
     * @see #withName(String)
     */
    public <R1, R2, T> GridFuture<R2> apply(GridClosure<T, R1> job, @Nullable Collection<? extends T> args,
        GridReducer<R1, R2> rdc);

    /**
     * Creates new {@link ExecutorService} which will execute all submitted
     * {@link Callable} and {@link Runnable} tasks on this projection. This essentially
     * creates a <b><i>Distributed Thread Pool</i</b> that can be used as a
     * replacement for local thread pools.
     * <p>
     * User may run {@link Callable} and {@link Runnable} tasks
     * just like normally with {@link ExecutorService java.util.ExecutorService}.
     * <p>
     * The typical Java example could be:
     * <pre name="code" class="java">
     * ...
     * ExecutorService exec = grid.compute().executorService();
     *
     * Future&lt;String&gt; fut = exec.submit(new MyCallable());
     * ...
     * String res = fut.get();
     * ...
     * </pre>
     *
     * @return {@code ExecutorService} which delegates all calls to grid.
     */
    public ExecutorService executorService();

    /**
     * Gets task future based on session ID. If task execution was started on local node and this
     * projection includes local node then the future for this task will be returned.
     *
     * @param sesId Session ID for task execution.
     * @param <R> Task result type.
     * @return Task future if task was started on this node and this node belongs to this projection,
     *      or {@code null} otherwise.
     */
    @Nullable public <R> GridComputeTaskFuture<R> taskFuture(GridUuid sesId);

    /**
     * Cancels task with the given ID, if it currently running inside this projection.
     *
     * @param sesId Task session ID.
     * @throws GridException If task cancellation failed.
     */
    public void cancelTask(GridUuid sesId) throws GridException;

    /**
     * Cancels job with the given ID, if it currently running inside this projection.
     *
     * @param jobId Job ID.
     * @throws GridException If task cancellation failed.
     */
    public void cancelJob(GridUuid jobId) throws GridException;

    /**
     * Sets task name for the next executed task on this projection in the <b>current thread</b>.
     * When task starts execution name is reset, so one name is used only once.
     * <p>
     * You may use this method to set task name when you cannot use
     * {@link GridComputeTaskName} annotation.
     * <p>
     * Here is an example.
     * <pre name="code" class="java">
     * GridGain.grid().withName("MyTask").run(
     *     BROADCAST,
     *     new GridRunnable() {
     *         &#64;Override public void run() {
     *             System.out.println("Hello!");
     *         }
     *     }
     * );
     * </pre>
     *
     * @param taskName Task name.
     * @return Grid projection ({@code this}).
     */
    public GridCompute withName(String taskName);

    /**
     * Sets task timeout for the next executed task on this projection in the <b>current thread</b>.
     * When task starts timeout is reset, so one timeout is used only once.
     * <p>
     * Here is an example.
     * <pre name="code" class="java">
     * GridGain.grid().withTimeout(10000).run(
     *     BROADCAST,
     *     new GridRunnable() {
     *         &#64;Override public void run() {
     *             System.out.println("Hello!");
     *         }
     *     }
     * );
     * </pre>
     *
     * @param timeout Task timeout in milliseconds.
     * @return Grid projection ({@code this}).
     */
    public GridCompute withTimeout(long timeout);

    /**
     * Sets no failover flag for the next executed task on this projection in the <b>current thread</b>.
     * If flag is set, job will be never failed over even if it fails with exception.
     * When task starts flag is reset, so all other task will use default failover policy
     * (implemented in {@link GridComputeTask#result(GridComputeJobResult, List)} method).
     * <p>
     * Here is an example.
     * <pre name="code" class="java">
     * GridGain.grid().compute().withNoFailover().run(
     *     BROADCAST,
     *     new GridRunnable() {
     *         &#64;Override public void run() {
     *             System.out.println("Hello!");
     *         }
     *     }
     * );
     * </pre>
     *
     * @return Grid projection ({@code this}).
     */
    public GridCompute withNoFailover();

    /**
     * Explicitly deploys given grid task on the local node. Upon completion of this method,
     * a task can immediately be executed on the grid, considering that all participating
     * remote nodes also have this task deployed. If peer-class-loading is enabled
     * (see {@link GridConfiguration#isPeerClassLoadingEnabled()}), then other nodes
     * will automatically deploy task upon execution request from the originating node without
     * having to manually deploy it.
     * <p>
     * Another way of class deployment which is supported is deployment from local class path.
     * Class from local class path has a priority over P2P deployed.
     * <p>
     * Note that class can be deployed multiple times on remote nodes, i.e. re-deployed. GridGain
     * maintains internal version of deployment for each instance of deployment (analogous to
     * class and class loader in Java). Execution happens always on the latest deployed instance.
     * <p>
     * This method has no effect if the class passed in was already deployed.
     *
     * @param taskCls Task class to deploy. If task class has {@link GridComputeTaskName} annotation,
     *      then task will be deployed under a name specified within annotation. Otherwise, full
     *      class name will be used as task's name.
     * @param clsLdr Task resources/classes class loader. This class loader is in charge
     *      of loading all necessary resources.
     * @throws GridException If task is invalid and cannot be deployed.
     * @see GridDeploymentSpi
     */
    public void localDeployTask(Class<? extends GridComputeTask> taskCls, ClassLoader clsLdr) throws GridException;

    /**
     * Gets map of all locally deployed tasks keyed by their task name satisfying all given predicates.
     * If no tasks were locally deployed, then empty map is returned. If no predicates provided - all
     * locally deployed tasks, if any, will be returned.
     *
     * @return Map of locally deployed tasks keyed by their task name.
     */
    public Map<String, Class<? extends GridComputeTask<?, ?>>> localTasks();

    /**
     * Makes the best attempt to undeploy a task with given name from the projection. Note that this
     * method returns immediately and does not wait until the task will actually be
     * undeployed on every node.
     *
     * @param taskName Name of the task to undeploy. If task class has {@link GridComputeTaskName} annotation,
     *      then task was deployed under a name specified within annotation. Otherwise, full
     *      class name should be used as task's name.
     * @throws GridException Thrown if undeploy failed.
     */
    public void undeployTask(String taskName) throws GridException;
}
