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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;

/**
 * Defines executable unit for {@link ComputeTask}.
 * <h1 class="header">Description</h1>
 * Grid job is an executable unit of {@link ComputeTask}. Grid task gets split into jobs
 * when {@link ComputeTask#map(List, Object)} method is called. This method returns
 * all jobs for the task mapped to their corresponding grid nodes for execution. Grid
 * will then serialize this jobs and send them to requested nodes for execution.
 * When a node receives a request to execute a job, the following sequence of events
 * takes place:
 * <ol>
 * <li>
 *      If collision SPI is defined, then job gets put on waiting list which is passed to underlying
 *      {@link org.apache.ignite.spi.collision.CollisionSpi} SPI. Otherwise job will be submitted to the executor
 *      service responsible for job execution immediately upon arrival.
 * </li>
 * <li>
 *      If collision SPI is configured, then it will decide one of the following scheduling policies:
 *      <ul>
 *      <li>
 *          Job will be kept on waiting list. In this case, job will not get a
 *          chance to execute until next time the Collision SPI is called.
 *      </li>
 *      <li>
 *          Job will be moved to active list. In this case system will proceed
 *          with job execution.
 *      </li>
 *      <li>
 *          Job will be rejected. In this case the {@link ComputeJobResult} passed into
 *          {@link ComputeTask#result(ComputeJobResult, List)} method will contain
 *          {@link ComputeExecutionRejectedException} exception. If you are using any
 *          of the task adapters shipped with Ignite, then job will be failed
 *          over automatically for execution on another node.
 *      </li>
 *      </ul>
 * </li>
 * <li>
 *      For activated jobs, an instance of distributed task session (see {@link ComputeTaskSession})
 *      will be injected.
 * </li>
 * <li>
 *      System will execute the job by calling {@link ComputeJob#execute()} method.
 * </li>
 * <li>
 *      If job gets cancelled while executing then {@link ComputeJob#cancel()}
 *      method will be called. Note that just like with {@link Thread#interrupt()}
 *      method, grid job cancellation serves as a hint that a job should stop
 *      executing or exhibit some other user defined behavior. Generally it is
 *      up to a job to decide whether it wants to react to cancellation or
 *      ignore it. Job cancellation can happen for several reasons:
 *      <ul>
 *      <li>Collision SPI cancelled an active job.</li>
 *      <li>Parent task has completed without waiting for this job's result.</li>
 *      <li>User cancelled task by calling {@link ComputeTaskFuture#cancel()} method.</li>
 *      </ul>
 * </li>
 * <li>
 *      Once job execution is complete, the return value will be sent back to parent
 *      task and will be passed into {@link ComputeTask#result(ComputeJobResult, List)}
 *      method via {@link ComputeJobResult} instance. If job execution resulted
 *      in a checked exception, then {@link ComputeJobResult#getException()} method
 *      will contain that exception. If job execution threw a runtime exception
 *      or error, then it will be wrapped into {@link ComputeUserUndeclaredException}
 *      exception.
 * </li>
 * </ol>
 * <p>
 * <h1 class="header">Resource Injection</h1>
 * Grid job implementation can be injected using IoC (dependency injection) with
 * ignite resources. Both, field and method based injection are supported.
 * The following ignite resources can be injected:
 * <ul>
 * <li>{@link org.apache.ignite.resources.TaskSessionResource}</li>
 * <li>{@link org.apache.ignite.resources.JobContextResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteInstanceResource}</li>
 * <li>{@link org.apache.ignite.resources.LoggerResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringApplicationContextResource}</li>
 * <li>{@link org.apache.ignite.resources.SpringResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <p>
 * <h1 class="header">GridComputeJobAdapter</h1>
 * Ignite comes with convenience {@link ComputeJobAdapter} adapter that provides
 * default empty implementation for {@link ComputeJob#cancel()} method and also
 * allows user to set and get job argument, if there is one.
 * <p>
 * <h1 class="header">Distributed Session Attributes</h1>
 * Jobs can communicate with parent task and with other job siblings from the same
 * task by setting session attributes (see {@link ComputeTaskSession}). Other jobs
 * can wait for an attribute to be set either synchronously or asynchronously.
 * Such functionality allows jobs to synchronize their execution with other jobs
 * at any point and can be useful when other jobs within task need to be made aware
 * of certain event or state change that occurred during job execution.
 * <p>
 * Distributed task session can be injected into {@link ComputeJob} implementation
 * using {@link org.apache.ignite.resources.TaskSessionResource @TaskSessionResource} annotation.
 * Both, field and method based injections are supported. Refer to
 * {@link ComputeTaskSession} documentation for more information on session functionality.
 * <p>
 * <h1 class="header">Saving Checkpoints</h1>
 * Long running jobs may wish to save intermediate checkpoints to protect themselves
 * from failures. There are three checkpoint management methods:
 * <ul>
 * <li>{@link ComputeTaskSession#saveCheckpoint(String, Object, ComputeTaskSessionScope, long)}</li>
 * <li>{@link ComputeTaskSession#loadCheckpoint(String)}</li>
 * <li>{@link ComputeTaskSession#removeCheckpoint(String)}</li>
 * </ul>
 * Jobs that utilize checkpoint functionality should attempt to load a check
 * point at the beginning of execution. If a {@code non-null} value is returned,
 * then job can continue from where it failed last time, otherwise it would start
 * from scratch. Throughout it's execution job should periodically save its
 * intermediate state to avoid starting from scratch in case of a failure.
 */
public interface ComputeJob extends Serializable {
    /**
     * This method is called when system detects that completion of this
     * job can no longer alter the overall outcome (for example, when parent task
     * has already reduced the results). Job is also cancelled when
     * {@link ComputeTaskFuture#cancel()} is called.
     * <p>
     * Note that job cancellation is only a hint, and just like with
     * {@link Thread#interrupt()}  method, it is really up to the actual job
     * instance to gracefully finish execution and exit.
     */
    public void cancel();

    /**
     * Executes this job.
     *
     * @return Job execution result (possibly {@code null}). This result will be returned
     *      in {@link ComputeJobResult#getData()} method passed into
     *      {@link ComputeTask#result(ComputeJobResult, List)} task method on caller node.
     * @throws IgniteException If job execution caused an exception. This exception will be
     *      returned in {@link ComputeJobResult#getException()} method passed into
     *      {@link ComputeTask#result(ComputeJobResult, List)} task method on caller node.
     *      If execution produces a {@link RuntimeException} or {@link Error}, then
     *      it will be wrapped into {@link IgniteCheckedException}.
     */
    public Object execute() throws IgniteException;
}