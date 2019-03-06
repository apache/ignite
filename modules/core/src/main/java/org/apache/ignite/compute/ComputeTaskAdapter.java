/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.compute;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterTopologyException;

/**
 * Convenience adapter for {@link ComputeTask} interface. Here is an example of
 * how {@code ComputeTaskAdapter} can be used:
 * <pre name="code" class="java">
 * public class MyFooBarTask extends ComputeTaskAdapter&lt;String, String&gt; {
 *     // Inject load balancer.
 *     &#64;LoadBalancerResource
 *     ComputeLoadBalancer balancer;
 *
 *     // Map jobs to grid nodes.
 *     public Map&lt;? extends ComputeJob, ClusterNode&gt; map(List&lt;ClusterNode&gt; subgrid, String arg) throws IgniteCheckedException {
 *         Map&lt;MyFooBarJob, ClusterNode&gt; jobs = new HashMap&lt;MyFooBarJob, ClusterNode&gt;(subgrid.size());
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
 * For more information refer to {@link ComputeTask} documentation.
 * @param <T> Type of the task argument.
 * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
 */
public abstract class ComputeTaskAdapter<T, R> implements ComputeTask<T, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default implementation which will wait for all jobs to complete before
     * calling {@link #reduce(List)} method.
     * <p>
     * If remote job resulted in exception ({@link ComputeJobResult#getException()} is not {@code null}),
     * then {@link ComputeJobResultPolicy#FAILOVER} policy will be returned if the exception is instance
     * of {@link org.apache.ignite.cluster.ClusterTopologyException} or {@link ComputeExecutionRejectedException}, which means that
     * remote node either failed or job execution was rejected before it got a chance to start. In all
     * other cases the exception will be rethrown which will ultimately cause task to fail.
     *
     * @param res Received remote grid executable result.
     * @param rcvd All previously received results.
     * @return Result policy that dictates how to process further upcoming
     *       job results.
     * @throws IgniteException If handling a job result caused an error effectively rejecting
     *      a failover. This exception will be thrown out of {@link ComputeTaskFuture#get()} method.
     */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        IgniteException e = res.getException();

        // Try to failover if result is failed.
        if (e != null) {
            // Don't failover user's code errors.
            if (e instanceof ComputeExecutionRejectedException ||
                e instanceof ClusterTopologyException ||
                // Failover exception is always wrapped.
                e.hasCause(ComputeJobFailoverException.class))
                return ComputeJobResultPolicy.FAILOVER;

            throw new IgniteException("Remote job threw user exception (override or implement ComputeTask.result(..) " +
                "method if you would like to have automatic failover for this exception): " + e.getMessage(), e);
        }

        // Wait for all job responses.
        return ComputeJobResultPolicy.WAIT;
    }
}