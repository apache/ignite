/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cluster;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.product.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Interface representing a single grid node. Use {@link #attribute(String)} or
 * {@link #metrics()} to get static and dynamic information about remote nodes.
 * {@code GridNode} list, which includes all nodes within task topology, is provided
 * to {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method. You can also get a handle on
 * discovered nodes by calling any of the following methods:
 * <ul>
 * <li>{@link IgniteCluster#localNode()}</li>
 * </ul>
 * <p>
 * <h1 class="header">Grid Node Attributes</h1>
 * You can use grid node attributes to provide static information about a node.
 * This information is initialized once within grid, during node startup, and
 * remains the same throughout the lifetime of a node. Use
 * {@link org.apache.ignite.configuration.IgniteConfiguration#getUserAttributes()} method to initialize your custom
 * node attributes at startup. Here is an example of how to assign an attribute to a node at startup:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration">
 *     ...
 *     &lt;property name="userAttributes">
 *         &lt;map>
 *             &lt;entry key="worker" value="true"/>
 *         &lt;/map>
 *     &lt;/property>
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * The system adds the following attributes automatically:
 * <ul>
 * <li>{@code {@link System#getProperties()}} - All system properties.</li>
 * <li>{@code {@link System#getenv(String)}} - All environment properties.</li>
 * <li>{@code org.gridgain.build.ver} - GridGain build version.</li>
 * <li>{@code org.gridgain.jit.name} - Name of JIT compiler used.</li>
 * <li>{@code org.gridgain.net.itf.name} - Name of network interface.</li>
 * <li>{@code org.gridgain.user.name} - Operating system user name.</li>
 * <li>{@code org.gridgain.grid.name} - Grid name (see {@link org.apache.ignite.Ignite#name()}).</li>
 * <li>
 *      {@code spiName.org.gridgain.spi.class} - SPI implementation class for every SPI,
 *      where {@code spiName} is the name of the SPI (see {@link org.apache.ignite.spi.IgniteSpi#getName()}.
 * </li>
 * <li>
 *      {@code spiName.org.gridgain.spi.ver} - SPI version for every SPI,
 *      where {@code spiName} is the name of the SPI (see {@link org.apache.ignite.spi.IgniteSpi#getName()}.
 * </li>
 * </ul>
 * <p>
 * Note that all System and Environment properties for all nodes are automatically included
 * into node attributes. This gives you an ability to get any information specified
 * in {@link System#getProperties()} about any node. So for example, in order to print out
 * information about Operating System for all nodes you would do the following:
 * <pre name="code" class="java">
 * for (GridNode node : G.grid().nodes()) {
 *     System.out.println("Operating system name: " + node.getAttribute("os.name"));
 *     System.out.println("Operating system architecture: " + node.getAttribute("os.arch"));
 *     System.out.println("Operating system version: " + node.getAttribute("os.version"));
 * }
 * </pre>
 * <p>
 * This interface provide a system view on the node instance. All user-level APIs work with
 * {@link ClusterNode} interface that provides much more functionality and extends this
 * interface. Consult {@link ClusterNode} for more information.
 * <p>
 * <h1 class="header">Grid Node Metrics</h1>
 * Grid node metrics (see {@link #metrics()}) are updated frequently for all nodes
 * and can be used to get dynamic information about a node. The frequency of update
 * is often directly related to the heartbeat exchange between nodes. So if, for example,
 * default {@link GridTcpDiscoverySpi} is used,
 * the metrics data will be updated every {@code 2} seconds by default.
 * <p>
 * Grid node metrics provide information about other nodes that can frequently change,
 * such as Heap and Non-Heap memory utilization, CPU load, number of active and waiting
 * grid jobs, etc... This information can become useful during job collision resolution or
 * {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} operation when jobs are assigned to remote nodes
 * for execution. For example, you can only pick nodes that don't have any jobs waiting
 * to be executed.
 * <p>
 * Local node metrics are registered as {@code MBean} and can be accessed from
 * any JMX management console. The simplest way is to use standard {@code jconsole}
 * that comes with JDK as it also provides ability to view any node parameter
 * as a graph.
 */
public interface ClusterNode extends GridMetadataAware {
    /**
     * Gets globally unique node ID. A new ID is generated every time a node restarts.
     *
     * @return Globally unique node ID.
     */
    public UUID id();

    /**
     * Gets consistent globally unique node ID. Unlike {@link #id()} method,
     * this method returns consistent node ID which survives node restarts.
     *
     * @return Consistent globally unique node ID.
     */
    public Object consistentId();

    /**
     * Gets a node attribute. Attributes are assigned to nodes at startup
     * via {@link org.apache.ignite.configuration.IgniteConfiguration#getUserAttributes()} method.
     * <p>
     * The system adds the following attributes automatically:
     * <ul>
     * <li>{@code {@link System#getProperties()}} - All system properties.</li>
     * <li>{@code {@link System#getenv(String)}} - All environment properties.</li>
     * <li>All attributes defined in {@link GridNodeAttributes}</li>
     * </ul>
     * <p>
     * Note that attributes cannot be changed at runtime.
     *
     * @param <T> Attribute Type.
     * @param name Attribute name. <b>Note</b> that attribute names starting with
     *      {@code org.gridgain} are reserved for internal use.
     * @return Attribute value or {@code null}.
     */
    @Nullable public <T> T attribute(String name);

    /**
     * Gets metrics snapshot for this node. Note that node metrics are constantly updated
     * and provide up to date information about nodes. For example, you can get
     * an idea about CPU load on remote node via {@link ClusterNodeMetrics#getCurrentCpuLoad()}
     * method and use it during {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} or during collision
     * resolution.
     * <p>
     * Node metrics are updated with some delay which is directly related to heartbeat
     * frequency. For example, when used with default
     * {@link GridTcpDiscoverySpi} the update will happen every {@code 2} seconds.
     *
     * @return Runtime metrics snapshot for this node.
     */
    public ClusterNodeMetrics metrics();

    /**
     * Gets all node attributes. Attributes are assigned to nodes at startup
     * via {@link org.apache.ignite.configuration.IgniteConfiguration#getUserAttributes()} method.
     * <p>
     * The system adds the following attributes automatically:
     * <ul>
     * <li>{@code {@link System#getProperties()}} - All system properties.</li>
     * <li>{@code {@link System#getenv(String)}} - All environment properties.</li>
     * <li>All attributes defined in {@link GridNodeAttributes}</li>
     * </ul>
     * <p>
     * Note that attributes cannot be changed at runtime.
     *
     * @return All node attributes.
     */
    public Map<String, Object> attributes();

    /**
     * Gets collection of addresses this node is known by.
     * <p>
     * If {@link org.apache.ignite.configuration.IgniteConfiguration#getLocalHost()} value isn't {@code null} node will try to use that
     * address for all communications and returned collection will contain only that address.
     * If it is {@code null} then local wildcard address will be used, and GridGain
     * will make the best effort to supply all addresses of that node in returned collection.
     *
     * @return Collection of addresses.
     */
    public Collection<String> addresses();

    /**
     * Gets collection of host names this node is known by.
     * <p>
     * If {@link org.apache.ignite.configuration.IgniteConfiguration#getLocalHost()} value isn't {@code null} node will try to use
     * the host name of that resolved address for all communications and
     * returned collection will contain only that host name.
     * If that host name can not be resolved then ip address returned by method {@link #addresses()} is used.
     * <p>
     * If {@link org.apache.ignite.configuration.IgniteConfiguration#getLocalHost()} value is {@code null} then local wildcard address will be used,
     * and this method returns host names of all addresses of that node.
     *
     * @return Collection of host names.
     */
    public Collection<String> hostNames();

    /**
     * Node order within grid topology. Discovery SPIs that support node ordering will
     * assign a proper order to each node and will guarantee that discovery event notifications
     * for new nodes will come in proper order. All other SPIs not supporting ordering
     * may choose to return node startup time here.
     * <p>
     * <b>NOTE</b>: in cases when discovery SPI doesn't support ordering GridGain cannot
     * guarantee that orders on all nodes will be unique or chronologically correct.
     * If such guarantee is required - make sure use discovery SPI that provides ordering.
     *
     * @return Node startup order.
     */
    public long order();

    /**
     * Gets node version.
     *
     * @return Node version.
     */
    public IgniteProductVersion version();

    /**
     * Tests whether or not this node is a local node.
     *
     * @return {@code True} if this node is a local node, {@code false} otherwise.
     */
    public boolean isLocal();

    /**
     * Tests whether or not this node is a daemon.
     * <p>
     * Daemon nodes are the usual grid nodes that participate in topology but not
     * visible on the main APIs, i.e. they are not part of any projections. The only
     * way to see daemon nodes is to use {@link IgniteCluster#forDaemons()} method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on GridGain and needs to participate in the topology but should be
     * excluded from "normal" topology so that it won't participate in task execution
     * or in-memory database.
     * <p>
     * Application code should never use daemon nodes.
     *
     * @return {@code True} if this node is a daemon, {@code false} otherwise.
     */
    public boolean isDaemon();

    /**
     * Tests whether or not this node is a client node.
     *
     * @return {@code True} if this node is a client node, {@code false} otherwise.
     */
    public boolean isClient();
}
