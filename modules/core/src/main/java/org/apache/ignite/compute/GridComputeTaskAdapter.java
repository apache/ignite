/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.gridgain.grid.*;

import java.util.*;

/**
 * Convenience adapter for {@link ComputeTask} interface. Here is an example of
 * how {@code GridComputeTaskAdapter} can be used:
 * <pre name="code" class="java">
 * public class MyFooBarTask extends GridComputeTaskAdapter&lt;String, String&gt; {
 *     // Inject load balancer.
 *     &#64;GridLoadBalancerResource
 *     GridComputeLoadBalancer balancer;
 *
 *     // Map jobs to grid nodes.
 *     public Map&lt;? extends GridComputeJob, GridNode&gt; map(List&lt;GridNode&gt; subgrid, String arg) throws GridException {
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
 *     public String reduce(List&lt;GridComputeJobResult&gt; results) throws GridException {
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
 * For more information refer to {@link ComputeTask} documentation.
 * @param <T> Type of the task argument.
 * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
 */
public abstract class GridComputeTaskAdapter<T, R> implements ComputeTask<T, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default implementation which will wait for all jobs to complete before
     * calling {@link #reduce(List)} method.
     * <p>
     * If remote job resulted in exception ({@link ComputeJobResult#getException()} is not {@code null}),
     * then {@link ComputeJobResultPolicy#FAILOVER} policy will be returned if the exception is instance
     * of {@link GridTopologyException} or {@link ComputeExecutionRejectedException}, which means that
     * remote node either failed or job execution was rejected before it got a chance to start. In all
     * other cases the exception will be rethrown which will ultimately cause task to fail.
     *
     * @param res Received remote grid executable result.
     * @param rcvd All previously received results.
     * @return Result policy that dictates how to process further upcoming
     *       job results.
     * @throws GridException If handling a job result caused an error effectively rejecting
     *      a failover. This exception will be thrown out of {@link GridComputeTaskFuture#get()} method.
     */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws GridException {
        GridException e = res.getException();

        // Try to failover if result is failed.
        if (e != null) {
            // Don't failover user's code errors.
            if (e instanceof ComputeExecutionRejectedException ||
                e instanceof GridTopologyException ||
                // Failover exception is always wrapped.
                e.hasCause(ComputeJobFailoverException.class))
                return ComputeJobResultPolicy.FAILOVER;

            throw new GridException("Remote job threw user exception (override or implement GridComputeTask.result(..) " +
                "method if you would like to have automatic failover for this exception).", e);
        }

        // Wait for all job responses.
        return ComputeJobResultPolicy.WAIT;
    }
}
