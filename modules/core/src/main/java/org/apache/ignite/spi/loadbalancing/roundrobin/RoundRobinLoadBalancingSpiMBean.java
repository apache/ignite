/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.loadbalancing.roundrobin;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * Management bean for {@link RoundRobinLoadBalancingSpi} SPI.
 */
@IgniteMBeanDescription("MBean that provides access to round robin load balancing SPI configuration.")
public interface RoundRobinLoadBalancingSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Configuration parameter indicating whether a new round robin order should be
     * created for every task. If {@code true} then load balancer is guaranteed
     * to iterate through nodes sequentially for every task - so as long as number
     * of jobs is less than or equal to the number of nodes, jobs are guaranteed to
     * be assigned to unique nodes. If {@code false} then one round-robin order
     * will be maintained for all tasks, so when tasks execute concurrently, it
     * is possible for more than one job within task to be assigned to the same
     * node.
     * <p>
     * Default is {@code true}.
     *
     * @return Configuration parameter indicating whether a new round robin order should
     *      be created for every task. Default is {@code true}.
     */
    @IgniteMBeanDescription("Configuration parameter indicating whether a new round robin order should be created for every task.")
    public boolean isPerTask();
}
