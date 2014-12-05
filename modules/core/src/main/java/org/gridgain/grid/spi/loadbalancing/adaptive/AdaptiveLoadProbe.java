/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.adaptive;

import org.apache.ignite.cluster.*;

/**
 * Pluggable implementation of node load probing. Implementations
 * of this can be configured to be used with {@link AdaptiveLoadBalancingSpi}
 * by setting {@link AdaptiveLoadBalancingSpi#setLoadProbe(AdaptiveLoadProbe)}
 * configuration parameter.
 * <p>
 * Note that if {@link #getLoad(org.apache.ignite.cluster.ClusterNode, int)} returns a value of {@code 0},
 * then implementation will assume that load value is simply not available and
 * will try to calculate an average of load values for other nodes. If such
 * average cannot be obtained (all node load values are {@code 0}), then a value
 * of {@code 1} will be used.
 * <p>
 * By default, {@link AdaptiveCpuLoadProbe} probing implementation is used.
 * <p>
 * <h1 class="header">Example</h1>
 * Here is an example of how probing can be implemented to use
 * number of active and waiting jobs as probing mechanism:
 * <pre name="code" class="java">
 * public class FooBarLoadProbe implements GridAdaptiveLoadProbe {
 *     // Flag indicating whether to use average value or current.
 *     private int useAvg = true;
 *
 *     public FooBarLoadProbe(boolean useAvg) {
 *         this.useAvg = useAvg;
 *     }
 *
 *     // Calculate load based on number of active and waiting jobs.
 *     public double getLoad(GridNode node, int jobsSentSinceLastUpdate) {
 *         GridNodeMetrics metrics = node.getMetrics();
 *
 *         if (useAvg) {
 *             double load = metrics.getAverageActiveJobs() + metrics.getAverageWaitingJobs();
 *
 *             if (load > 0) {
 *                 return load;
 *             }
 *         }
 *
 *         return metrics.getCurrentActiveJobs() + metrics.getCurrentWaitingJobs();
 *     }
 * }
 * </pre>
 * Below is an example of how a probe shown above would be configured with {@link AdaptiveLoadBalancingSpi}
 * SPI:
 * <pre name="code" class="xml">
 * &lt;property name="loadBalancingSpi"&gt;
 *     &lt;bean class="org.gridgain.grid.spi.loadBalancing.adaptive.GridAdaptiveLoadBalancingSpi"&gt;
 *         &lt;property name="loadProbe"&gt;
 *             &lt;bean class="foo.bar.FooBarLoadProbe"&gt;
 *                 &lt;constructor-arg value="true"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 */
public interface AdaptiveLoadProbe {
    /**
     * Calculates load value for a given node. Specific implementations would
     * usually take into account some of the values provided by
     * {@link org.apache.ignite.cluster.ClusterNode#metrics()} method. For example, load can be calculated
     * based on job execution time or number of active jobs, or CPU/Heap utilization.
     * <p>
     * Note that if this method returns a value of {@code 0},
     * then implementation will assume that load value is simply not available and
     * will try to calculate an average of load values for other nodes. If such
     * average cannot be obtained (all node load values are {@code 0}), then a value
     * of {@code 1} will be used.
     *
     * @param node Grid node to calculate load for.
     * @param jobsSentSinceLastUpdate Number of jobs sent to this node since
     *      last metrics update. This parameter may be useful when
     *      implementation takes into account the current job count on a node.
     * @return Non-negative load value for the node (zero and above).
     */
    public double getLoad(ClusterNode node, int jobsSentSinceLastUpdate);
}
