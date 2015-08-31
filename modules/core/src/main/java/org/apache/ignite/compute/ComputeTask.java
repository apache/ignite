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

package org.apache.ignite.compute;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Grid task interface defines a task that can be executed on the grid. Grid task
 * is responsible for splitting business logic into multiple grid jobs, receiving
 * results from individual grid jobs executing on remote nodes, and reducing
 * (aggregating) received jobs' results into final grid task result.
 * <p>
 * <h1 class="header">Grid Task Execution Sequence</h1>
 * <ol>
 * <li>
 *      Upon request to execute a grid task with given task name system will find
 *      deployed task with given name. Task needs to be deployed prior to execution
 *      (see {@link org.apache.ignite.IgniteCompute#localDeployTask(Class, ClassLoader)} method), however if task does not specify
 *      its name explicitly via {@link ComputeTaskName @GridComputeTaskName} annotation, it
 *      will be auto-deployed first time it gets executed.
 * </li>
 * <li>
 *      System will create new distributed task session (see {@link ComputeTaskSession}).
 * </li>
 * <li>
 *      System will inject all annotated resources (including task session) into grid task instance.
 *      See {@link org.apache.ignite.resources} package for the list of injectable resources.
 * </li>
 * <li>
 *      System will apply {@link #map(List, Object) map(List, Object)}. This
 *      method is responsible for splitting business logic of grid task into
 *      multiple grid jobs (units of execution) and mapping them to
 *      grid nodes. Method {@link #map(List, Object) map(List, Object)} returns
 *      a map of with grid jobs as keys and grid node as values.
 * </li>
 * <li>
 *      System will send mapped grid jobs to their respective nodes.
 * </li>
 * <li>
 *      Upon arrival on the remote node a grid job will be handled by collision SPI
 *      (see {@link org.apache.ignite.spi.collision.CollisionSpi}) which will determine how a job will be executed
 *      on the remote node (immediately, buffered or canceled).
 * </li>
 * <li>
 *      Once job execution results become available method {@link #result(ComputeJobResult, List) result(GridComputeJobResult, List)}
 *      will be called for each received job result. The policy returned by this method will
 *      determine the way task reacts to every job result:
 *      <ul>
 *      <li>
 *          If {@link ComputeJobResultPolicy#WAIT} policy is returned, task will continue to wait
 *          for other job results. If this result is the last job result, then
 *          {@link #reduce(List) reduce(List)} method will be called.
 *      </li>
 *      <li>
 *          If {@link ComputeJobResultPolicy#REDUCE} policy is returned, then method
 *          {@link #reduce(List) reduce(List)} will be called right away without waiting for
 *          other jobs' completion (all remaining jobs will receive a cancel request).
 *      </li>
 *      <li>
 *          If {@link ComputeJobResultPolicy#FAILOVER} policy is returned, then job will
 *          be failed over to another node for execution. The node to which job will get
 *          failed over is decided by {@link org.apache.ignite.spi.failover.FailoverSpi} SPI implementation.
 *          Note that if you use {@link ComputeTaskAdapter} adapter for {@code GridComputeTask}
 *          implementation, then it will automatically fail jobs to another node for 2
 *          known failure cases:
 *          <ul>
 *          <li>
 *              Job has failed due to node crash. In this case {@link ComputeJobResult#getException()}
 *              method will return an instance of {@link org.apache.ignite.cluster.ClusterTopologyException} exception.
 *          </li>
 *          <li>
 *              Job execution was rejected, i.e. remote node has cancelled job before it got
 *              a chance to execute, while it still was on the waiting list. In this case
 *              {@link ComputeJobResult#getException()} method will return an instance of
 *              {@link ComputeExecutionRejectedException} exception.
 *          </li>
 *          </ul>
 *      </li>
 *      </ul>
 * </li>
 * <li>
 *      Once all results are received or {@link #result(ComputeJobResult, List) result(GridComputeJobResult, List)}
 *      method returned {@link ComputeJobResultPolicy#REDUCE} policy, method {@link #reduce(List) reduce(List)}
 *      is called to aggregate received results into one final result. Once this method is finished the
 *      execution of the grid task is complete. This result will be returned to the user through
 *      {@link ComputeTaskFuture#get()} method.
 * </li>
 * </ol>
 * <p>
 * <h1 class="header">Continuous Job Mapper</h1>
 * For cases when jobs within split are too large to fit in memory at once or when
 * simply not all jobs in task are known during {@link #map(List, Object)} step,
 * use {@link ComputeTaskContinuousMapper} to continuously stream jobs from task even after {@code map(...)}
 * step is complete. Usually with continuous mapper the number of jobs within task
 * may grow too large - in this case it may make sense to use it in combination with
 * {@link ComputeTaskNoResultCache @GridComputeTaskNoResultCache} annotation.
 * <p>
 * <h1 class="header">Task Result Caching</h1>
 * Sometimes job results are too large or task simply has too many jobs to keep track
 * of which may hinder performance. In such cases it may make sense to disable task
 * result caching by attaching {@link ComputeTaskNoResultCache @GridComputeTaskNoResultCache} annotation to task class, and
 * processing all results as they come in {@link #result(ComputeJobResult, List)} method.
 * When Ignite sees this annotation it will disable tracking of job results and
 * list of all job results passed into {@link #result(ComputeJobResult, List)} or
 * {@link #reduce(List)} methods will always be empty. Note that list of
 * job siblings on {@link ComputeTaskSession} will also be empty to prevent number
 * of job siblings from growing as well.
 * <p>
 * <h1 class="header">Resource Injection</h1>
 * Grid task implementation can be injected using IoC (dependency injection) with
 * ignite resources. Both, field and method based injection are supported.
 * The following ignite resources can be injected:
 * <ul>
 * <li>{@link org.apache.ignite.resources.TaskSessionResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteInstanceResource}</li>
 * <li>{@link org.apache.ignite.resources.LoggerResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringApplicationContextResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <p>
 * <h1 class="header">Grid Task Adapters</h1>
 * {@code GridComputeTask} comes with several convenience adapters to make the usage easier:
 * <ul>
 * <li>
 * {@link ComputeTaskAdapter} provides default implementation for {@link ComputeTask#result(ComputeJobResult, List)}
 * method which provides automatic fail-over to another node if remote job has failed
 * due to node crash (detected by {@link org.apache.ignite.cluster.ClusterTopologyException} exception) or due to job
 * execution rejection (detected by {@link ComputeExecutionRejectedException} exception).
 * Here is an example of how a you would implement your task using {@link ComputeTaskAdapter}:
 * <pre name="code" class="java">
 * public class MyFooBarTask extends GridComputeTaskAdapter&lt;String, String&gt; {
 *     // Inject load balancer.
 *     &#64;LoadBalancerResource
 *     ComputeLoadBalancer balancer;
 *
 *     // Map jobs to grid nodes.
 *     public Map&lt;? extends ComputeJob, GridNode&gt; map(List&lt;GridNode&gt; subgrid, String arg) throws IgniteCheckedException {
 *         Map&lt;MyFooBarJob, GridNode&gt; jobs = new HashMap&lt;MyFooBarJob, GridNode&gt;(subgrid.size());
 *
 *         // In more complex cases, you can actually do
 *         // more complicated assignments of jobs to nodes.
 *         for (int i = 0; i &lt; subgrid.size(); i++) {
 *             // Pick the next best balanced node for the job.
 *             jobs.put(new MyFooBarJob(arg), balancer.getBalancedNode())
 *         }
 *
 *         return jobs;
 *     }
 *
 *     // Aggregate results into one compound result.
 *     public String reduce(List&lt;ComputeJobResult&gt; results) throws IgniteCheckedException {
 *         // For the purpose of this example we simply
 *         // concatenate string representation of every
 *         // job result
 *         StringBuilder buf = new StringBuilder();
 *
 *         for (ComputeJobResult res : results) {
 *             // Append string representation of result
 *             // returned by every job.
 *             buf.append(res.getData().string());
 *         }
 *
 *         return buf.string();
 *     }
 * }
 * </pre>
 * </li>
 * <li>
 * {@link ComputeTaskSplitAdapter} hides the job-to-node mapping logic from
 * user and provides convenient {@link ComputeTaskSplitAdapter#split(int, Object)}
 * method for splitting task into sub-jobs in homogeneous environments.
 * Here is an example of how you would implement your task using {@link ComputeTaskSplitAdapter}:
 * <pre name="code" class="java">
 * public class MyFooBarTask extends GridComputeTaskSplitAdapter&lt;Object, String&gt; {
 *     &#64;Override
 *     protected Collection&lt;? extends ComputeJob&gt; split(int gridSize, Object arg) throws IgniteCheckedException {
 *         List&lt;MyFooBarJob&gt; jobs = new ArrayList&lt;MyFooBarJob&gt;(gridSize);
 *
 *         for (int i = 0; i &lt; gridSize; i++) {
 *             jobs.add(new MyFooBarJob(arg));
 *         }
 *
 *         // Node assignment via load balancer
 *         // happens automatically.
 *         return jobs;
 *     }
 *
 *     // Aggregate results into one compound result.
 *     public String reduce(List&lt;GridComputeJobResult&gt; results) throws IgniteCheckedException {
 *         // For the purpose of this example we simply
 *         // concatenate string representation of every
 *         // job result
 *         StringBuilder buf = new StringBuilder();
 *
 *         for (GridComputeJobResult res : results) {
 *             // Append string representation of result
 *             // returned by every job.
 *             buf.append(res.getData().string());
 *         }
 *
 *         return buf.string();
 *     }
 * }
 * </pre>
 * </li>
 * </ul>
 * @param <T> Type of the task argument that is passed into {@link ComputeTask#map(List, Object)} method.
 * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
 */
public interface ComputeTask<T, R> extends Serializable {
    /**
     * This method is called to map or split grid task into multiple grid jobs. This is the
     * first method that gets called when task execution starts.
     *
     * @param arg Task execution argument. Can be {@code null}. This is the same argument
     *      as the one passed into {@code Grid#execute(...)} methods.
     * @param subgrid Nodes available for this task execution. Note that order of nodes is
     *      guaranteed to be randomized by container. This ensures that every time
     *      you simply iterate through grid nodes, the order of nodes will be random which
     *      over time should result into all nodes being used equally.
     * @return Map of grid jobs assigned to subgrid node. Unless {@link ComputeTaskContinuousMapper} is
     *      injected into task, if {@code null} or empty map is returned, exception will be thrown.
     * @throws IgniteException If mapping could not complete successfully. This exception will be
     *      thrown out of {@link ComputeTaskFuture#get()} method.
     */
    @Nullable public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable T arg) throws IgniteException;

    /**
     * Asynchronous callback invoked every time a result from remote execution is
     * received. It is ultimately upto this method to return a policy based
     * on which the system will either wait for more results, reduce results
     * received so far, or failover this job to another node. See
     * {@link ComputeJobResultPolicy} for more information about result policies.
     *
     * @param res Received remote grid executable result.
     * @param rcvd All previously received results. Note that if task class has
     *      {@link ComputeTaskNoResultCache} annotation, then this list will be empty.
     * @return Result policy that dictates how to process further upcoming
     *       job results.
     * @throws IgniteException If handling a job result caused an error. This exception will
     *      be thrown out of {@link ComputeTaskFuture#get()} method.
     */
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException;

    /**
     * Reduces (or aggregates) results received so far into one compound result to be returned to
     * caller via {@link ComputeTaskFuture#get()} method.
     * <p>
     * Note, that if some jobs did not succeed and could not be failed over then the list of
     * results passed into this method will include the failed results. Otherwise, failed
     * results will not be in the list.
     *
     * @param results Received results of broadcasted remote executions. Note that if task class has
     *      {@link ComputeTaskNoResultCache} annotation, then this list will be empty.
     * @return Grid job result constructed from results of remote executions.
     * @throws IgniteException If reduction or results caused an error. This exception will
     *      be thrown out of {@link ComputeTaskFuture#get()} method.
     */
    @Nullable public R reduce(List<ComputeJobResult> results) throws IgniteException;
}