// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.loadbalancing.*;
import org.gridgain.grid.spi.topology.*;
import java.lang.annotation.*;

/**
 * This annotation allows task to specify what SPIs it wants to use.
 * Starting with {@code GridGain 2.1} you can start multiple instances
 * of {@link GridTopologySpi}, {@link GridLoadBalancingSpi},
 * {@link GridFailoverSpi}, and {@link GridCheckpointSpi}. If you do that,
 * you need to tell a task which SPI to use (by default it will use the fist
 * SPI in the list).
 * <p>
 * <h1 class="header">Example</h1>
 * This example shows how to configure different SPI's for different tasks. Let's
 * assume that you have two worker nodes, {@code Node1} and {@code Node2}.
 * Let's also assume that you configure {@code Node1} to belong to {@code SegmentA}
 * and {@code Node2} to belong to {@code SegmentB}. Here is a sample configuration
 * for {@code Node1}
 * <pre name="code" class="xml">
 * &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration" scope="singleton"&gt;
 *     &lt;property name="userAttributes"&gt;
 *         &lt;map&gt;
 *             &lt;entry key="segment" value="A"/&gt;
 *         &lt;/map&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * {@code Node2} configuration looks similar to {@code Node1} with {@code 'segment'} attribute
 * set to {@code 'B'}.
 * <pre name="code" class="xml">
 * &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration" scope="singleton"&gt;
 *     &lt;property name="userAttributes"&gt;
 *         &lt;map&gt;
 *             &lt;entry key="segment" value="B"/&gt;
 *         &lt;/map&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * Now, if you have {@code Task1} and {@code Task2} starting from some master node {@code NodeM},
 * you can easily configure {@code Task1} to only run on {@code SegmentA} and {@code Task2} to
 * only run on {@code SegmentB}. Here is how configuration on master node {@code NodeM} would
 * look like:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration" scope="singleton"&gt;
 *     &lt;!--
 *         Topology SPIs. We have two named SPIs: One picks up nodes
 *         that have attribute "segment" set to "A" and another one sees
 *         nodes that have attribute "segment" set to "B".
 *     --&gt;
 *     &lt;property name="topologySpi"&gt;
 *         &lt;list&gt;
 *             &lt;bean class="org.gridgain.grid.spi.topology.nodefilter.GridNodeFilterTopologySpi"&gt;
 *                 &lt;property name="name" value="topologyA"/&gt;
 *                 &lt;property name="filter"&gt;
 *                     &lt;bean class="org.gridgain.grid.lang.GridJexlPredicate2"&gt;
 *                         &lt;property name="expression" value="elem1.attributes().get('segment') == 'A'"/&gt;
 *                     &lt;/bean&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *             &lt;bean class="org.gridgain.grid.spi.topology.nodefilter.GridNodeFilterTopologySpi"&gt;
 *                 &lt;property name="name" value="topologyB"/&gt;
 *                 &lt;property name="filter"&gt;
 *                     &lt;bean class="org.gridgain.grid.lang.GridJexlPredicate2"&gt;
 *                         &lt;property name="expression" value="elem1.attributes().get('segment') == 'B'"/&gt;
 *                     &lt;/bean&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/list&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * Then your {@code Task1} and {@code Task2} would look as follows (note the {@code @GridComputeTaskSpis}
 * annotation).
 * <pre name="code" class="java">
 * ...
 * &#64;GridComputeTaskSpis(topologySpi="topologyA")
 * public class GridSegmentATask extends GridComputeTaskSplitAdapter&lt;String, Integer&gt; {
 * // Task class body.
 * }
 * ...
 * </pre>
 * and
 * <pre name="code" class="java">
 * &#64;GridComputeTaskSpis(topologySpi="topologyB")
 * ...
 * public class GridSegmentBTask extends GridComputeTaskSplitAdapter&lt;String, Integer&gt; {
 * // Task class body.
 * }
 * ...
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings({"JavaDoc"})
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridComputeTaskSpis {
    /**
     * Optional load balancing SPI name. By default, SPI name is equal
     * to the name of the SPI class. You can change SPI name by explicitely
     * supplying {@link GridSpi#getName()} parameter in grid configuration.
     */
    public String loadBalancingSpi() default "";

    /**
     * Optional failover SPI name. By default, SPI name is equal
     * to the name of the SPI class. You can change SPI name by explicitely
     * supplying {@link GridSpi#getName()} parameter in grid configuration.
     */
    public String failoverSpi() default "";

    /**
     * Optional topology SPI name. By default, SPI name is equal
     * to the name of the SPI class. You can change SPI name by explicitely
     * supplying {@link GridSpi#getName()} parameter in grid configuration.
     */
    public String topologySpi() default "";

    /**
     * Optional checkpoint SPI name. By default, SPI name is equal
     * to the name of the SPI class. You can change SPI name by explicitely
     * supplying {@link GridSpi#getName()} parameter in grid configuration.
     */
    public String checkpointSpi() default "";
}
