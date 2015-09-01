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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Defines a mapper that can be used for asynchronous job sending. Useful for
 * streaming jobs within the same task. Note that if job number within a task
 * grows too large, it is best to attach {@link ComputeTaskNoResultCache} annotation
 * to task to make sure that collection of job results and job siblings does
 * not grow indefinitely.
 * <p>
 * Continuous mapper methods can be used right after it injected into a task.
 * Mapper can not be used after {@link ComputeTask#result(ComputeJobResult, List)}
 * method returned the {@link ComputeJobResultPolicy#REDUCE} policy. Also if
 * {@link ComputeTask#result(ComputeJobResult, List)} method returned the
 * {@link ComputeJobResultPolicy#WAIT} policy and all jobs are finished then task
 * will go to reducing results and continuous mapper can not be used.
 * <p>
 * Note that whenever continuous mapper is used, {@link ComputeTask#map(List, Object)}
 * method is allowed to return {@code null} in case when at least one job
 * has been sent prior to completing the {@link ComputeTask#map(List, Object)} method.
 * <p>
 * Task continuous mapper can be injected into a task using IoC (dependency
 * injection) by attaching {@link org.apache.ignite.resources.TaskContinuousMapperResource}
 * annotation to a field or a setter method inside of {@link ComputeTask} implementations
 * as follows:
 * <pre name="code" class="java">
 * ...
 * // This field will be injected with task continuous mapper.
 * &#64TaskContinuousMapperResource
 * private ComputeTaskContinuousMapper mapper;
 * ...
 * </pre>
 * or from a setter method:
 * <pre name="code" class="java">
 * // This setter method will be automatically called by the system
 * // to set grid task continuous mapper.
 * &#64TaskContinuousMapperResource
 * void setSession(ComputeTaskContinuousMapper mapper) {
 *     this.mapper = mapper;
 * }
 * </pre>
 */
public interface ComputeTaskContinuousMapper {
    /**
     * Sends given job to a specific grid node.
     *
     * @param job Job instance to send. If {@code null} is passed, exception will be thrown.
     * @param node Grid node. If {@code null} is passed, exception will be thrown.
     * @throws IgniteException If job can not be processed.
     */
    public void send(ComputeJob job, ClusterNode node) throws IgniteException;

    /**
     * Sends collection of grid jobs to assigned nodes.
     *
     * @param mappedJobs Map of grid jobs assigned to grid node. If {@code null}
     *      or empty list is passed, exception will be thrown.
     * @throws IgniteException If job can not be processed.
     */
    public void send(Map<? extends ComputeJob, ClusterNode> mappedJobs) throws IgniteException;

    /**
     * Sends job to a node automatically picked by the underlying load balancer.
     *
     * @param job Job instance to send. If {@code null} is passed, exception will be thrown.
     * @throws IgniteException If job can not be processed.
     */
    public void send(ComputeJob job) throws IgniteException;

    /**
     * Sends collection of jobs to nodes automatically picked by the underlying load balancer.
     *
     * @param jobs Collection of grid job instances. If {@code null} or empty
     *      list is passed, exception will be thrown.
     * @throws IgniteException If job can not be processed.
     */
    public void send(Collection<? extends ComputeJob> jobs) throws IgniteException;
}