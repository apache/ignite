/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.resources;

import org.gridgain.grid.spi.loadbalancing.*;

import java.lang.annotation.*;

/**
 * Annotates a field or a setter method for injection of {@link org.apache.ignite.compute.ComputeLoadBalancer}.
 * Specific implementation for grid load balancer is defined by
 * {@link GridLoadBalancingSpi}
 * which is provided to grid via {@link org.apache.ignite.configuration.IgniteConfiguration}..
 * <p>
 * Load balancer can be injected into instances of following classes:
 * <ul>
 * <li>{@link org.apache.ignite.compute.ComputeTask}</li>
 * </ul>
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridTask extends GridComputeTask&lt;String, Integer&gt; {
 *    &#64;GridLoadBalancerResource
 *    private ` balancer;
 * }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridTask extends GridComputeTask&lt;String, Integer&gt; {
 *     ...
 *     private GridComputeLoadBalancer balancer;
 *     ...
 *     &#64;GridLoadBalancerResource
 *     public void setBalancer(GridComputeLoadBalancer balancer) {
 *         this.balancer = balancer;
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See {@link org.apache.ignite.configuration.IgniteConfiguration#getLoadBalancingSpi()} for Grid configuration details.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface GridLoadBalancerResource {
    // No-op.
}
