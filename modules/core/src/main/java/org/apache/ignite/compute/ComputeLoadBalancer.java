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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Load balancer is used for finding the best balanced node according
 * to load balancing policy. Internally load balancer will
 * query the {@link org.apache.ignite.spi.loadbalancing.LoadBalancingSpi}
 * to get the balanced node.
 * <p>
 * Load balancer can be used <i>explicitly</i> from inside {@link ComputeTask#map(List, Object)}
 * method when you implement {@link ComputeTask} interface directly or use
 * {@link ComputeTaskAdapter}. If you use {@link ComputeTaskSplitAdapter} then
 * load balancer is accessed <i>implicitly</i> by the adapter so you don't have
 * to use it directly in your logic.
 * <h1 class="header">Coding Examples</h1>
 * If you are using {@link ComputeTaskSplitAdapter} then load balancing logic
 * is transparent to your code and is handled automatically by the adapter.
 * Here is an example of how your task will look:
 * <pre name="code" class="java">
 * public class MyFooBarTask extends GridComputeTaskSplitAdapter&lt;String> {
 *     &#64;Override
 *     protected Collection&lt;? extends ComputeJob> split(int gridSize, String arg) throws IgniteCheckedException {
 *         List&lt;MyFooBarJob> jobs = new ArrayList&lt;MyFooBarJob>(gridSize);
 *
 *         for (int i = 0; i &lt; gridSize; i++) {
 *             jobs.add(new MyFooBarJob(arg));
 *         }
 *
 *         // Node assignment via load balancer
 *         // happens automatically.
 *         return jobs;
 *     }
 *     ...
 * }
 * </pre>
 * If you need more fine-grained control over how some jobs within task get mapped to a node
 * and use affinity load balancing for some other jobs within task, then you should use
 * {@link ComputeTaskAdapter}. Here is an example of how your task will look. Note that in this
 * case we manually inject load balancer and use it to pick the best node. Doing it in
 * such way would allow user to map some jobs manually and for others use load balancer.
 * <pre name="code" class="java">
 * public class MyFooBarTask extends GridComputeTaskAdapter&lt;String, String> {
 *     // Inject load balancer.
 *     &#64;LoadBalancerResource
 *     ComputeLoadBalancer balancer;
 *
 *     // Map jobs to grid nodes.
 *     public Map&lt;? extends ComputeJob, GridNode> map(List&lt;GridNode> subgrid, String arg) throws IgniteCheckedException {
 *         Map&lt;MyFooBarJob, GridNode> jobs = new HashMap&lt;MyFooBarJob, GridNode>(subgrid.size());
 *
 *         // In more complex cases, you can actually do
 *         // more complicated assignments of jobs to nodes.
 *         for (int i = 0; i &lt; subgrid.size(); i++) {
 *             // Pick the next best balanced node for the job.
 *             ComputeJob myJob = new MyFooBarJob(arg);
 *
 *             jobs.put(myJob, balancer.getBalancedNode(myJob, null));
 *         }
 *
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
 *             buf.append(res.getData().toString());
 *         }
 *
 *         return buf.toString();
 *     }
 * }
 * </pre>
 */
public interface ComputeLoadBalancer {
    /**
     * Gets the next balanced node according to the underlying load balancing policy.
     *
     * @param job Job to get the balanced node for.
     * @param exclNodes Optional collection of nodes that should be excluded from balanced nodes.
     *      If collection is {@code null} or empty - no nodes will be excluded.
     * @return Next balanced node.
     * @throws IgniteException If any error occurred when finding next balanced node.
     */
    @Nullable public ClusterNode getBalancedNode(ComputeJob job, @Nullable Collection<ClusterNode> exclNodes)
        throws IgniteException;
}