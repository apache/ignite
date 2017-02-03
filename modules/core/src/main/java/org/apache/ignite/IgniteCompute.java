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

package org.apache.ignite;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.compute.ComputeTaskSpis;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteAsyncSupported;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.failover.FailoverSpi;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines compute grid functionality for executing tasks and closures over nodes
 * in the {@link ClusterGroup}. Instance of {@code IgniteCompute} is obtained from {@link Ignite}
 * as follows:
 * <pre name="code" class="java">
 * Ignite ignite = Ignition.ignite();
 *
 * // Compute over all nodes in the cluster.
 * IgniteCompute c = ignite.compute();
 * </pre>
 * You can also get an instance of {@link IgniteCompute} over a subset of cluster nodes, i.e. over
 * a {@link ClusterGroup}:
 * <pre name="code" class="java">
 * // Cluster group composed of all remote nodes.
 * ClusterGroup rmtGrp = ignite.cluster().forRemotes();
 *
 * // Compute over remote nodes only.
 * IgniteCompute c = ignite.compute(rmtGrp);
 * </pre>
 * The methods are grouped as follows:
 * <ul>
 * <li>{@code apply(...)} methods execute {@link IgniteClosure} jobs over nodes in the cluster group.</li>
 * <li>{@code call(...)} methods execute {@link IgniteCallable} jobs over nodes in the cluster group.</li>
 * <li>{@code run(...)} methods execute {@link IgniteRunnable} jobs over nodes in the cluster group.</li>
 * <li>{@code broadcast(...)} methods broadcast jobs to all nodes in the cluster group.</li>
 * <li>{@code affinityCall(...)} and {@code affinityRun(...)} methods collocate jobs with nodes
 *  on which a specified key is cached.</li>
 * </ul>
 * Note that if attempt is made to execute a computation over an empty cluster group (i.e. cluster group
 * that does not have any alive nodes), then {@link org.apache.ignite.cluster.ClusterGroupEmptyException}
 * will be thrown out of result future.
 * <h1 class="header">Load Balancing</h1>
 * In all cases other than {@code broadcast(...)}, Ignite must select a node for a computation
 * to be executed. The node will be selected based on the underlying {@link LoadBalancingSpi},
 * which by default sequentially picks next available node from the underlying cluster group. Other
 * load balancing policies, such as {@code random} or {@code adaptive}, can be configured as well by
 * selecting a different load balancing SPI in Ignite configuration. If your logic requires some custom
 * load balancing behavior, consider implementing {@link ComputeTask} directly.
 * <h1 class="header">Fault Tolerance</h1>
 * Ignite guarantees that as long as there is at least one grid node standing, every job will be
 * executed. Jobs will automatically failover to another node if a remote node crashed
 * or has rejected execution due to lack of resources. By default, in case of failover, next
 * load balanced node will be picked for job execution. Also jobs will never be re-routed to the
 * nodes they have failed on. This behavior can be changed by configuring any of the existing or a custom
 * {@link FailoverSpi} in grid configuration.
 * <h1 class="header">Resource Injection</h1>
 * All compute jobs, including closures, runnables, callables, and tasks can be injected with
 * ignite resources. Both, field and method based injections are supported. The following grid
 * resources can be injected:
 * <ul>
 * <li>{@link TaskSessionResource}</li>
 * <li>{@link IgniteInstanceResource}</li>
 * <li>{@link LoggerResource}</li>
 * <li>{@link SpringApplicationContextResource}</li>
 * <li>{@link SpringResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * Here is an example of how to inject instance of {@link Ignite} into a computation:
 * <pre name="code" class="java">
 * public class MyIgniteJob extends IgniteRunnable {
 *      ...
 *      &#64;IgniteInstanceResource
 *      private Ignite ignite;
 *      ...
 *  }
 * </pre>
 * <h1 class="header">Computation SPIs</h1>
 * Note that regardless of which method is used for executing computations, all relevant SPI implementations
 * configured for this compute instance will be used (i.e. failover, load balancing, collision resolution,
 * checkpoints, etc.). If you need to override configured defaults, you should use compute task together with
 * {@link ComputeTaskSpis} annotation. Refer to {@link ComputeTask} documentation for more information.
 */
public interface IgniteCompute extends IgniteAsyncSupport {
    /**
     * Gets cluster group to which this {@code IgniteCompute} instance belongs.
     *
     * @return Cluster group to which this {@code IgniteCompute} instance belongs.
     */
    public ClusterGroup clusterGroup();

    /**
     * Executes given job on the node where data for provided affinity key is located
     * (a.k.a. affinity co-location). The data of the partition where affKey is stored
     * will not be migrated from the target node while the job is executed.
     *
     * @param cacheName Name of the cache to use for affinity co-location.
     * @param affKey Affinity key.
     * @param job Job which will be co-located on the node with given affinity key.
     * @throws IgniteException If job failed.
     */
    @IgniteAsyncSupported
    public void affinityRun(@Nullable String cacheName, Object affKey, IgniteRunnable job) throws IgniteException;

    /**
     * Executes given job on the node where data for provided affinity key is located
     * (a.k.a. affinity co-location). The data of the partition where affKey is stored
     * will not be migrated from the target node while the job is executed. The data
     * of the extra caches' partitions with the same partition number also will not be migrated.
     *
     * @param cacheNames Names of the caches to to reserve the partition. The first cache uses for affinity co-location.
     * @param affKey Affinity key.
     * @param job Job which will be co-located on the node with given affinity key.
     * @throws IgniteException If job failed.
     */
    @IgniteAsyncSupported
    public void affinityRun(@NotNull Collection<String> cacheNames, Object affKey, IgniteRunnable job)
        throws IgniteException;

    /**
     * Executes given job on the node where partition is located (the partition is primary on the node)
     * The data of the partition will not be migrated from the target node
     * while the job is executed. The data of the extra caches' partitions with the same partition number
     * also will not be migrated.
     *
     * @param cacheNames Names of the caches to to reserve the partition. The first cache uses for affinity co-location.
     * @param partId Partition number.
     * @param job Job which will be co-located on the node with given affinity key.
     * @throws IgniteException If job failed.
     */
    @IgniteAsyncSupported
    public void affinityRun(@NotNull Collection<String> cacheNames, int partId, IgniteRunnable job)
        throws IgniteException;

    /**
     * Executes given job on the node where data for provided affinity key is located
     * (a.k.a. affinity co-location). The data of the partition where affKey is stored
     * will not be migrated from the target node while the job is executed.
     *
     * @param cacheName Name of the cache to use for affinity co-location.
     * @param affKey Affinity key.
     * @param job Job which will be co-located on the node with given affinity key.
     * @return Job result.
     * @throws IgniteException If job failed.
     */
    @IgniteAsyncSupported
    public <R> R affinityCall(@Nullable String cacheName, Object affKey, IgniteCallable<R> job) throws IgniteException;

    /**
     * Executes given job on the node where data for provided affinity key is located
     * (a.k.a. affinity co-location). The data of the partition where affKey is stored
     * will not be migrated from the target node while the job is executed. The data
     * of the extra caches' partitions with the same partition number also will not be migrated.
     *
     * @param cacheNames Names of the caches to to reserve the partition. The first cache uses for affinity co-location.
     * @param affKey Affinity key.
     * @param job Job which will be co-located on the node with given affinity key.
     * @return Job result.
     * @throws IgniteException If job failed.
     */
    @IgniteAsyncSupported
    public <R> R affinityCall(@NotNull Collection<String> cacheNames, Object affKey, IgniteCallable<R> job)
        throws IgniteException;

    /**
     * Executes given job on the node where partition is located (the partition is primary on the node)
     * The data of the partition will not be migrated from the target node
     * while the job is executed. The data of the extra caches' partitions with the same partition number
     * also will not be migrated.
     *
     * @param cacheNames Names of the caches to to reserve the partition. The first cache uses for affinity co-location.
     * @param partId Partition to reserve.
     * @param job Job which will be co-located on the node with given affinity key.
     * @return Job result.
     * @throws IgniteException If job failed.
     */
    @IgniteAsyncSupported
    public <R> R affinityCall(@NotNull Collection<String> cacheNames, int partId, IgniteCallable<R> job)
        throws IgniteException;

    /**
     * Executes given task on within the cluster group. For step-by-step explanation of task execution process
     * refer to {@link ComputeTask} documentation.
     *
     * @param taskCls Class of the task to execute. If class has {@link ComputeTaskName} annotation,
     *      then task is deployed under a name specified within annotation. Otherwise, full
     *      class name is used as task name.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task result.
     * @throws IgniteException If task failed.
     */
    @IgniteAsyncSupported
    public <T, R> R execute(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) throws IgniteException;

    /**
     * Executes given task within the cluster group. For step-by-step explanation of task execution process
     * refer to {@link ComputeTask} documentation.
     *
     * @param task Instance of task to execute. If task class has {@link ComputeTaskName} annotation,
     *      then task is deployed under a name specified within annotation. Otherwise, full
     *      class name is used as task name.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task result.
     * @throws IgniteException If task failed.
     */
    @IgniteAsyncSupported
    public <T, R> R execute(ComputeTask<T, R> task, @Nullable T arg) throws IgniteException;

    /**
     * Executes given task within the cluster group. For step-by-step explanation of task execution process
     * refer to {@link ComputeTask} documentation.
     * <p>
     * If task for given name has not been deployed yet, then {@code taskName} will be
     * used as task class name to auto-deploy the task (see {@link #localDeployTask(Class, ClassLoader)} method).
     *
     * @param taskName Name of the task to execute.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task result.
     * @throws IgniteException If task failed.
     * @see ComputeTask for information about task execution.
     */
    @IgniteAsyncSupported
    public <T, R> R execute(String taskName, @Nullable T arg) throws IgniteException;

    /**
     * Broadcasts given job to all nodes in the cluster group.
     *
     * @param job Job to broadcast to all cluster group nodes.
     * @throws IgniteException If job failed.
     */
    @IgniteAsyncSupported
    public void broadcast(IgniteRunnable job) throws IgniteException;

    /**
     * Broadcasts given job to all nodes in cluster group. Every participating node will return a
     * job result. Collection of all returned job results is returned from the result future.
     *
     * @param job Job to broadcast to all cluster group nodes.
     * @return Collection of results for this execution.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <R> Collection<R> broadcast(IgniteCallable<R> job) throws IgniteException;

    /**
     * Broadcasts given closure job with passed in argument to all nodes in the cluster group.
     * Every participating node will return a job result. Collection of all returned job results
     * is returned from the result future.
     *
     * @param job Job to broadcast to all cluster group nodes.
     * @param arg Job closure argument.
     * @return Collection of results for this execution.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <R, T> Collection<R> broadcast(IgniteClosure<T, R> job, @Nullable T arg) throws IgniteException;

    /**
     * Executes provided job on a node within the underlying cluster group.
     *
     * @param job Job closure to execute.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public void run(IgniteRunnable job) throws IgniteException;

    /**
     * Executes collection of jobs on grid nodes within the underlying cluster group.
     *
     * @param jobs Collection of jobs to execute.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public void run(Collection<? extends IgniteRunnable> jobs) throws IgniteException;

    /**
     * Executes provided job on a node within the underlying cluster group. The result of the
     * job execution is returned from the result closure.
     *
     * @param job Job to execute.
     * @return Job result.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <R> R call(IgniteCallable<R> job) throws IgniteException;

    /**
     * Executes collection of jobs on nodes within the underlying cluster group.
     * Collection of all returned job results is returned from the result future.
     *
     * @param jobs Collection of jobs to execute.
     * @return Collection of job results for this execution.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <R> Collection<R> call(Collection<? extends IgniteCallable<R>> jobs) throws IgniteException;

    /**
     * Executes collection of jobs on nodes within the underlying cluster group. The returned
     * job results will be reduced into an individual result by provided reducer.
     *
     * @param jobs Collection of jobs to execute.
     * @param rdc Reducer to reduce all job results into one individual return value.
     * @return Future with reduced job result for this execution.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <R1, R2> R2 call(Collection<? extends IgniteCallable<R1>> jobs, IgniteReducer<R1, R2> rdc)
        throws IgniteException;

    /**
     * Executes provided closure job on a node within the underlying cluster group. This method is different
     * from {@code run(...)} and {@code call(...)} methods in a way that it receives job argument
     * which is then passed into the closure at execution time.
     *
     * @param job Job to run.
     * @param arg Job argument.
     * @return Job result.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <R, T> R apply(IgniteClosure<T, R> job, @Nullable T arg) throws IgniteException;

    /**
     * Executes provided closure job on nodes within the underlying cluster group. A new job is executed for
     * every argument in the passed in collection. The number of actual job executions will be
     * equal to size of the job arguments collection.
     *
     * @param job Job to run.
     * @param args Job arguments.
     * @return Collection of job results.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <T, R> Collection<R> apply(IgniteClosure<T, R> job, Collection<? extends T> args) throws IgniteException;

    /**
     * Executes provided closure job on nodes within the underlying cluster group. A new job is executed for
     * every argument in the passed in collection. The number of actual job executions will be
     * equal to size of the job arguments collection. The returned job results will be reduced
     * into an individual result by provided reducer.
     *
     * @param job Job to run.
     * @param args Job arguments.
     * @param rdc Reducer to reduce all job results into one individual return value.
     * @return Future with reduced job result for this execution.
     * @throws IgniteException If execution failed.
     */
    @IgniteAsyncSupported
    public <R1, R2, T> R2 apply(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) throws IgniteException;

    /**
     * Gets tasks future for active tasks started on local node.
     *
     * @return Map of active tasks keyed by their task task session ID.
     */
    public <R> Map<IgniteUuid, ComputeTaskFuture<R>> activeTaskFutures();

    /**
     * Sets task name for the next executed task in the <b>current thread</b>.
     * When task starts execution, the name is reset, so one name is used only once. You may use
     * this method to set task name when executing jobs directly, without explicitly
     * defining {@link ComputeTask}.
     * <p>
     * Here is an example.
     * <pre name="code" class="java">
     * ignite.withName("MyTask").run(new IgniteRunnable() {...});
     * </pre>
     *
     * @param taskName Task name.
     * @return This {@code IgniteCompute} instance for chaining calls.
     */
    public IgniteCompute withName(String taskName);

    /**
     * Sets task timeout for the next executed task in the <b>current thread</b>.
     * When task starts execution, the timeout is reset, so one timeout is used only once. You may use
     * this method to set task name when executing jobs directly, without explicitly
     * defining {@link ComputeTask}.
     * <p>
     * Here is an example.
     * <pre class="brush:java">
     * ignite.withTimeout(10000).run(new IgniteRunnable() {...});
     * </pre>
     *
     * @param timeout Computation timeout in milliseconds.
     * @return This {@code IgniteCompute} instance for chaining calls.
     */
    public IgniteCompute withTimeout(long timeout);

    /**
     * Sets no-failover flag for the next task executed in the <b>current thread</b>.
     * If flag is set, job will be never failed over even if remote node crashes or rejects execution.
     * When task starts execution, the no-failover flag is reset, so all other task will use default
     * failover policy, unless this flag is set again.
     * <p>
     * Here is an example.
     * <pre name="code" class="java">
     * ignite.compute().withNoFailover().run(new IgniteRunnable() {...});
     * </pre>
     *
     * @return This {@code IgniteCompute} instance for chaining calls.
     */
    public IgniteCompute withNoFailover();

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
     * Note that class can be deployed multiple times on remote nodes, i.e. re-deployed. Ignition
     * maintains internal version of deployment for each instance of deployment (analogous to
     * class and class loader in Java). Execution happens always on the latest deployed instance.
     * <p>
     * This method has no effect if the class passed in was already deployed.
     *
     * @param taskCls Task class to deploy. If task class has {@link ComputeTaskName} annotation,
     *      then task will be deployed under the name specified within annotation. Otherwise, full
     *      class name will be used as task's name.
     * @param clsLdr Task class loader. This class loader is in charge
     *      of loading all necessary resources for task execution.
     * @throws IgniteException If task is invalid and cannot be deployed.
     */
    public void localDeployTask(Class<? extends ComputeTask> taskCls, ClassLoader clsLdr) throws IgniteException;

    /**
     * Gets map of all locally deployed tasks keyed by their task name .
     *
     * @return Map of locally deployed tasks keyed by their task name.
     */
    public Map<String, Class<? extends ComputeTask<?, ?>>> localTasks();

    /**
     * Makes the best attempt to undeploy a task with given name within the underlying cluster group.
     * Note that this method returns immediately and does not wait until the task will actually be
     * undeployed on every node.
     *
     * @param taskName Name of the task to undeploy.
     * @throws IgniteException Thrown if undeploy failed.
     */
    public void undeployTask(String taskName) throws IgniteException;

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<R> future();

    /** {@inheritDoc} */
    @Override public IgniteCompute withAsync();
}