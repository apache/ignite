/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import java.util.*;

/**
 * Defines a mapper that can be used for asynchronous job sending. Useful for
 * streaming jobs within the same task. Note that if job number within a task
 * grows too large, it is best to attach {@link GridComputeTaskNoResultCache} annotation
 * to task to make sure that collection of job results and job siblings does
 * not grow indefinitely.
 * <p>
 * Continuous mapper methods can be used right after it injected into a task.
 * Mapper can not be used after {@link GridComputeTask#result(GridComputeJobResult, List)}
 * method returned the {@link GridComputeJobResultPolicy#REDUCE} policy. Also if
 * {@link GridComputeTask#result(GridComputeJobResult, List)} method returned the
 * {@link GridComputeJobResultPolicy#WAIT} policy and all jobs are finished then task
 * will go to reducing results and continuous mapper can not be used.
 * <p>
 * Note that whenever continuous mapper is used, {@link GridComputeTask#map(List, Object)}
 * method is allowed to return {@code null} in case when at least one job
 * has been sent prior to completing the {@link GridComputeTask#map(List, Object)} method.
 * <p>
 * Task continuous mapper can be injected into a task using IoC (dependency
 * injection) by attaching {@link GridTaskContinuousMapperResource}
 * annotation to a field or a setter method inside of {@link GridComputeTask} implementations
 * as follows:
 * <pre name="code" class="java">
 * ...
 * // This field will be injected with task continuous mapper.
 * &#64GridTaskContinuousMapperResource
 * private GridComputeTaskContinuousMapper mapper;
 * ...
 * </pre>
 * or from a setter method:
 * <pre name="code" class="java">
 * // This setter method will be automatically called by the system
 * // to set grid task continuous mapper.
 * &#64GridTaskContinuousMapperResource
 * void setSession(GridComputeTaskContinuousMapper mapper) {
 *     this.mapper = mapper;
 * }
 * </pre>
 */
public interface GridComputeTaskContinuousMapper {
    /**
     * Sends given job to a specific grid node.
     *
     * @param job Job instance to send. If {@code null} is passed, exception will be thrown.
     * @param node Grid node. If {@code null} is passed, exception will be thrown.
     * @throws GridException If job can not be processed.
     */
    public void send(GridComputeJob job, ClusterNode node) throws GridException;

    /**
     * Sends collection of grid jobs to assigned nodes.
     *
     * @param mappedJobs Map of grid jobs assigned to grid node. If {@code null}
     *      or empty list is passed, exception will be thrown.
     * @throws GridException If job can not be processed.
     */
    public void send(Map<? extends GridComputeJob, ClusterNode> mappedJobs) throws GridException;

    /**
     * Sends job to a node automatically picked by the underlying load balancer.
     *
     * @param job Job instance to send. If {@code null} is passed, exception will be thrown.
     * @throws GridException If job can not be processed.
     */
    public void send(GridComputeJob job) throws GridException;

    /**
     * Sends collection of jobs to nodes automatically picked by the underlying load balancer.
     *
     * @param jobs Collection of grid job instances. If {@code null} or empty
     *      list is passed, exception will be thrown.
     * @throws GridException If job can not be processed.
     */
    public void send(Collection<? extends GridComputeJob> jobs) throws GridException;
}
