/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.loadbalancing.adaptive.*;
import org.gridgain.grid.spi.loadbalancing.roundrobin.*;
import org.gridgain.grid.spi.loadbalancing.weightedrandom.*;

import java.util.*;

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
 *    protected Collection&lt;? extends GridComputeJob&gt; split(int gridSize, Object arg) throws GridException {
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
 *    &#64;GridLoadBalancerResource
 *    GridComputeLoadBalancer balancer;
 *
 *    // Map jobs to grid nodes.
 *    public Map&lt;? extends GridComputeJob, GridNode&gt; map(List&lt;GridNode&gt; subgrid, String arg) throws GridException {
 *        Map&lt;MyFooBarJob, GridNode&gt; jobs = new HashMap&lt;MyFooBarJob, GridNode&gt;(subgrid.size());
 *
 *        // In more complex cases, you can actually do
 *        // more complicated assignments of jobs to nodes.
 *        for (int i = 0; i &lt; subgrid.size(); i++) {
 *            // Pick the next best balanced node for the job.
 *            GridComputeJob myJob = new MyFooBarJob(arg);
 *
 *            jobs.put(myJob, balancer.getBalancedNode(myJob, null));
 *        }
 *
 *        return jobs;
 *    }
 *
 *    // Aggregate results into one compound result.
 *    public String reduce(List&lt;GridComputeJobResult&gt; results) throws GridException {
 *        // For the purpose of this example we simply
 *        // concatenate string representation of every
 *        // job result
 *        StringBuilder buf = new StringBuilder();
 *
 *        for (GridComputeJobResult res : results) {
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
 * GridGain comes with the following load balancing SPI implementations out of the box:
 * <ul>
 * <li>{@link GridRoundRobinLoadBalancingSpi} - default</li>
 * <li>{@link GridAdaptiveLoadBalancingSpi}</li>
 * <li>{@link GridWeightedRandomLoadBalancingSpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface GridLoadBalancingSpi extends IgniteSpi {
    /**
     * Gets balanced node for specified job within given task session.
     *
     * @param ses Grid task session for currently executing task.
     * @param top Topology of task nodes from which to pick the best balanced node for given job.
     * @param job Job for which to pick the best balanced node.
     * @throws GridException If failed to get next balanced node.
     * @return Best balanced node for the given job within given task session.
     */
    public ClusterNode getBalancedNode(ComputeTaskSession ses, List<ClusterNode> top, ComputeJob job) throws GridException;
}
