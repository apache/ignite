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

package org.apache.ignite.spi.loadbalancing;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.spi.IgniteSpi;

/**
 * Load balancing SPI provides the next best balanced node for job
 * execution. This SPI is used either implicitly or explicitly whenever
 * a job gets mapped to a node during {@link org.apache.ignite.compute.ComputeTask#map(List, Object)}
 * invocation.
 * <h1 class="header">Coding Examples</h1>
 * If you are using {@link org.apache.ignite.compute.ComputeTaskSplitAdapter} then load balancing logic
 * is transparent to your code and is handled automatically by the adapter.
 * Here is an example of how your task could look:
 * <pre name="code" class="java">
 * public class MyFooBarTask extends GridComputeTaskSplitAdapter&lt;Object,Object&gt; {
 *    &#64;Override
 *    protected Collection&lt;? extends ComputeJob&gt; split(int gridSize, Object arg) throws IgniteCheckedException {
 *        List&lt;MyFooBarJob&gt; jobs = new ArrayList&lt;MyFooBarJob&gt;(gridSize);
 *
 *        for (int i = 0; i &lt; gridSize; i++) {
 *            jobs.add(new MyFooBarJob(arg));
 *        }
 *
 *        // Node assignment via load balancer
 *        // happens automatically.
 *        return jobs;
 *    }
 *    ...
 * }
 * </pre>
 * If you need more fine-grained control over how some jobs within task get mapped to a node
 * <i>and</i> use, for example, affinity load balancing for some other jobs within task, then you should use
 * {@link org.apache.ignite.compute.ComputeTaskAdapter}. Here is an example of how your task could look. Note that in this
 * case we manually inject load balancer and use it to pick the best node. Doing it in
 * such way would allow user to map some jobs manually and for others use load balancer.
 * <pre name="code" class="java">
 * public class MyFooBarTask extends GridComputeTaskAdapter&lt;String,String&gt; {
 *    // Inject load balancer.
 *    &#64;LoadBalancerResource
 *    ComputeLoadBalancer balancer;
 *
 *    // Map jobs to grid nodes.
 *    public Map&lt;? extends ComputeJob, GridNode&gt; map(List&lt;GridNode&gt; subgrid, String arg) throws IgniteCheckedException {
 *        Map&lt;MyFooBarJob, GridNode&gt; jobs = new HashMap&lt;MyFooBarJob, GridNode&gt;(subgrid.size());
 *
 *        // In more complex cases, you can actually do
 *        // more complicated assignments of jobs to nodes.
 *        for (int i = 0; i &lt; subgrid.size(); i++) {
 *            // Pick the next best balanced node for the job.
 *            ComputeJob myJob = new MyFooBarJob(arg);
 *
 *            jobs.put(myJob, balancer.getBalancedNode(myJob, null));
 *        }
 *
 *        return jobs;
 *    }
 *
 *    // Aggregate results into one compound result.
 *    public String reduce(List&lt;ComputeJobResult&gt; results) throws IgniteCheckedException {
 *        // For the purpose of this example we simply
 *        // concatenate string representation of every
 *        // job result
 *        StringBuilder buf = new StringBuilder();
 *
 *        for (ComputeJobResult res : results) {
 *            // Append string representation of result
 *            // returned by every job.
 *            buf.append(res.getData().toString());
 *        }
 *
 *        return buf.toString();
 *    }
 * }
 * </pre>
 * <p>
 * Ignite comes with the following load balancing SPI implementations out of the box:
 * <ul>
 * <li>{@link org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi} - default</li>
 * <li>{@link org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveLoadBalancingSpi}</li>
 * <li>{@link org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface LoadBalancingSpi extends IgniteSpi {
    /**
     * Gets balanced node for specified job within given task session.
     *
     * @param ses Grid task session for currently executing task.
     * @param top Topology of task nodes from which to pick the best balanced node for given job.
     * @param job Job for which to pick the best balanced node.
     * @throws IgniteException If failed to get next balanced node.
     * @return Best balanced node for the given job within given task session.
     */
    public ClusterNode getBalancedNode(ComputeTaskSession ses, List<ClusterNode> top, ComputeJob job) throws IgniteException;
}