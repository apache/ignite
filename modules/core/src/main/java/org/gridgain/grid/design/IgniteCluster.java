// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.design.cluster.*;
import org.gridgain.grid.design.lang.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface IgniteCluster {
    /**
     * Gets read-only collections of nodes in this projection.
     *
     * @return All nodes in this projection.
     */
    public Collection<ClusterNode> nodes();

    /**
     * Gets a node for given ID from this grid projection.
     *
     * @param nid Node ID.
     * @return Node with given ID from this projection or {@code null} if such node does not exist in this
     *      projection.
     */
    @Nullable public ClusterNode node(UUID nid);

    /**
     * Creates a grid projection over a given set of nodes.
     *
     * @param nodes Collection of nodes to create a projection from.
     * @return Projection over provided grid nodes.
     */
    public ClusterTopology forNodes(Collection<? extends ClusterNode> nodes);

    /**
     * Creates a grid projection for the given node.
     *
     * @param node Node to get projection for.
     * @param nodes Optional additional nodes to include into projection.
     * @return Grid projection for the given node.
     */
    public ClusterTopology forNode(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a grid projection for nodes other than given nodes.
     *
     * @param node Node to exclude from new grid projection.
     * @param nodes Optional additional nodes to exclude from projection.
     * @return Projection that will contain all nodes that original projection contained excluding
     *      given nodes.
     */
    public ClusterTopology forOthers(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a grid projection for nodes not included into given projection.
     *
     * @param top Projection to exclude from new grid projection.
     * @return Projection for nodes not included into given projection.
     */
    public ClusterTopology forOthers(ClusterTopology top);

    /**
     * Creates a grid projection over nodes with specified node IDs.
     *
     * @param ids Collection of node IDs.
     * @return Projection over nodes with specified node IDs.
     */
    public ClusterTopology forNodeIds(Collection<UUID> ids);

    /**
     * Creates a grid projection for a node with specified ID.
     *
     * @param id Node ID to get projection for.
     * @param ids Optional additional node IDs to include into projection.
     * @return Projection over node with specified node ID.
     */
    public ClusterTopology forNodeId(UUID id, UUID... ids);

    /**
     * Creates a grid projection which includes all nodes that pass the given predicate filter.
     *
     * @param p Predicate filter for nodes to include into this projection.
     * @return Grid projection for nodes that passed the predicate filter.
     */
    public ClusterTopology forPredicate(IgnitePredicate<ClusterNode> p);

    /**
     * Creates projection for nodes containing given name and value
     * specified in user attributes.
     * <p>
     * User attributes for every node are optional and can be specified in
     * grid node configuration. See {@link GridConfiguration#getUserAttributes()}
     * for more information.
     *
     * @param name Name of the attribute.
     * @param val Optional attribute value to match.
     * @return Grid projection for nodes containing specified attribute.
     */
    public ClusterTopology forAttribute(String name, @Nullable String val);

    /**
     * Creates projection for all nodes that have cache with specified name running.
     *
     * @param cacheName Cache name.
     * @param cacheNames Optional additional cache names to include into projection.
     * @return Projection over nodes that have specified cache running.
     */
    public ClusterTopology forCache(String cacheName, @Nullable String... cacheNames);

    /**
     * Creates projection for all nodes that have streamer with specified name running.
     *
     * @param streamerName Streamer name.
     * @param streamerNames Optional additional streamer names to include into projection.
     * @return Projection over nodes that have specified streamer running.
     */
    public ClusterTopology forStreamer(String streamerName, @Nullable String... streamerNames);

    /**
     * Gets grid projection consisting from the nodes in this projection excluding the local node.
     *
     * @return Grid projection consisting from the nodes in this projection excluding the local node, if any.
     */
    public ClusterTopology forRemotes();

    /**
     * Gets grid projection consisting from the nodes in this projection residing on the
     * same host as given node.
     *
     * @param node Node residing on the host for which projection is created.
     * @return Projection for nodes residing on the same host as passed in node.
     */
    public ClusterTopology forHost(ClusterNode node);

    /**
     * Gets projection consisting from the daemon nodes in this projection.
     * <p>
     * Daemon nodes are the usual grid nodes that participate in topology but not
     * visible on the main APIs, i.e. they are not part of any projections. The only
     * way to see daemon nodes is to use this method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on GridGain and needs to participate in the topology but also needs to be
     * excluded from "normal" topology so that it won't participate in task execution
     * or in-memory data grid storage.
     *
     * @return Grid projection consisting from the daemon nodes in this projection.
     */
    public ClusterTopology forDaemons();

    /**
     * Creates grid projection with one random node from current projection.
     *
     * @return Grid projection with one random node from current projection.
     */
    public ClusterTopology forRandom();

    /**
     * Creates grid projection with one oldest node in the current projection.
     * The resulting projection is dynamic and will always pick the next oldest
     * node if the previous one leaves topology even after the projection has
     * been created.
     *
     * @return Grid projection with one oldest node from the current projection.
     */
    public ClusterTopology forOldest();

    /**
     * Creates grid projection with one youngest node in the current projection.
     * The resulting projection is dynamic and will always pick the newest
     * node in the topology, even if more nodes entered after the projection
     * has been created.
     *
     * @return Grid projection with one youngest node from the current projection.
     */
    public ClusterTopology forYoungest();

    /**
     * Gets local grid node.
     *
     * @return Local grid node.
     */
    public ClusterNode localNode();

    /**
     * Pings a remote node.
     * <p>
     * Discovery SPIs usually have some latency in discovering failed nodes. Hence,
     * communication to remote nodes may fail at times if an attempt was made to
     * establish communication with a failed node. This method can be used to check
     * if communication has failed due to node failure or due to some other reason.
     *
     * @param nodeId ID of a node to ping.
     * @return {@code true} if node for a given ID is alive, {@code false} otherwise.
     * @see GridDiscoverySpi
     */
    public boolean pingNode(UUID nodeId);

    /**
     * Gets current cluster version. In case of TCP discovery
     * (see {@link GridTcpDiscoverySpi}) cluster versions
     * are sequential - they start from {@code '1'} and get incremented every time whenever a
     * node joins or leaves. For other discovery SPIs topology versions may not be (and likely are
     * not) sequential.
     *
     * @return Current topology version.
     */
    public long clusterVersion();

    /**
     * Gets a topology by version. Returns {@code null} if topology history storage doesn't contain
     * specified topology version (history currently keeps last {@code 1000} snapshots).
     *
     * @param topVer Topology version.
     * @return Collection of grid nodes which represented by specified topology version,
     * if it is present in history storage, {@code null} otherwise.
     * @throws UnsupportedOperationException If underlying SPI implementation does not support
     *      topology history. Currently only {@link GridTcpDiscoverySpi}
     *      supports topology history.
     */
    @Nullable public Collection<ClusterNode> nodes(long topVer) throws UnsupportedOperationException;

    /**
     * This method provides ability to detect which cache keys are mapped to which nodes
     * on cache instance with given name. Use it to determine which nodes are storing which
     * keys prior to sending jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node mapped to all keys.</li>
     * <li>
     *      For fully replicated caches, {@link GridCacheAffinityFunction} is
     *      used to determine which keys are mapped to which groups of nodes.
     * </li>
     * <li>For partitioned caches, the returned map represents node-to-key affinity.</li>
     * </ul>
     *
     * @param cacheName Cache name, if {@code null}, then default cache instance is used.
     * @param keys Cache keys to map to nodes.
     * @return Map of nodes to cache keys or empty map if there are no alive nodes for this cache.
     * @throws GridException If failed to map cache keys.
     */
    public <K> Map<GridNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys) throws GridException;

    /**
     * This method provides ability to detect which cache keys are mapped to which nodes
     * on cache instance with given name. Use it to determine which nodes are storing which
     * keys prior to sending jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node ID.</li>
     * <li>
     *      For fully replicated caches first node ID returned by {@link GridCacheAffinityFunction}
     *      is returned.
     * </li>
     * <li>For partitioned caches, the returned node ID is the primary node for the key.</li>
     * </ul>
     *
     * @param cacheName Cache name, if {@code null}, then default cache instance is used.
     * @param key Cache key to map to a node.
     * @return Primary node for the key or {@code null} if cache with given name
     *      is not present in the grid.
     * @throws GridException If failed to map key.
     */
    @Nullable public <K> GridNode mapKeyToNode(@Nullable String cacheName, K key) throws GridException;

    /**
     * Resets local I/O, job, and task execution metrics.
     */
    public void resetMetrics();

    /**
     * Starts one or more nodes on remote host(s).
     * <p>
     * This method takes INI file which defines all startup parameters. It can contain one or
     * more sections, each for a host or for range of hosts (note that they must have different
     * names) and a special '{@code defaults}' section with default values. They are applied to
     * undefined parameters in host's sections.
     * <p>
     * Returned result is collection of tuples. Each tuple corresponds to one node start attempt and
     * contains hostname, success flag and error message if attempt was not successful. Note that
     * successful attempt doesn't mean that node was actually started and joined topology. For large
     * topologies (> 100s nodes) it can take over 10 minutes for all nodes to start. See individual
     * node logs for details.
     *
     * @param file Configuration file.
     * @param restart Whether to stop existing nodes. If {@code true}, all existing
     *      nodes on the host will be stopped before starting new ones. If
     *      {@code false}, nodes will be started only if there are less
     *      nodes on the host than expected.
     * @param timeout Connection timeout.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Future for collection of tuples, each containing host name, result (success of failure)
     *      and error message (if any).
     * @throws GridException In case of error.
     */
    public GridFuture<Collection<GridTuple3<String, Boolean, String>>> startNodes(File file,
        boolean restart, int timeout, int maxConn) throws GridException;

    /**
     * Starts one or more nodes on remote host(s).
     * <p>
     * Each map in {@code hosts} collection
     * defines startup parameters for one host or for a range of hosts. The following
     * parameters are supported:
     *     <table class="doctable">
     *         <tr>
     *             <th>Name</th>
     *             <th>Type</th>
     *             <th>Description</th>
     *         </tr>
     *         <tr>
     *             <td><b>host</b></td>
     *             <td>String</td>
     *             <td>
     *                 Hostname (required). Can define several hosts if their IPs are sequential.
     *                 E.g., {@code 10.0.0.1~5} defines range of five IP addresses. Other
     *                 parameters are applied to all hosts equally.
     *             </td>
     *         </tr>
     *         <tr>
     *             <td><b>port</b></td>
     *             <td>Integer</td>
     *             <td>Port number (default is {@code 22}).</td>
     *         </tr>
     *         <tr>
     *             <td><b>uname</b></td>
     *             <td>String</td>
     *             <td>Username (if not defined, current local username will be used).</td>
     *         </tr>
     *         <tr>
     *             <td><b>passwd</b></td>
     *             <td>String</td>
     *             <td>Password (if not defined, private key file must be defined).</td>
     *         </tr>
     *         <tr>
     *             <td><b>key</b></td>
     *             <td>File</td>
     *             <td>Private key file (if not defined, password must be defined).</td>
     *         </tr>
     *         <tr>
     *             <td><b>nodes</b></td>
     *             <td>Integer</td>
     *             <td>
     *                 Expected number of nodes on the host. If some nodes are started
     *                 already, then only remaining nodes will be started. If current count of
     *                 nodes is equal to this number, and {@code restart} flag is {@code false},
     *                 then nothing will happen.
     *             </td>
     *         </tr>
     *         <tr>
     *             <td><b>ggHome</b></td>
     *             <td>String</td>
     *             <td>
     *                 Path to GridGain installation folder. If not defined, GRIDGAIN_HOME
     *                 environment variable must be set on remote hosts.
     *             </td>
     *         </tr>
     *         <tr>
     *             <td><b>cfg</b></td>
     *             <td>String</td>
     *             <td>Path to configuration file (relative to {@code ggHome}).</td>
     *         </tr>
     *         <tr>
     *             <td><b>script</b></td>
     *             <td>String</td>
     *             <td>
     *                 Custom startup script file name and path (relative to {@code ggHome}).
     *                 You can also specify a space-separated list of parameters in the same
     *                 string (for example: {@code "bin/my-custom-script.sh -v"}).
     *             </td>
     *         </tr>
     *     </table>
     * <p>
     * {@code dflts} map defines default values. They are applied to undefined parameters in
     * {@code hosts} collection.
     * <p>
     * Returned result is collection of tuples. Each tuple corresponds to one node start attempt and
     * contains hostname, success flag and error message if attempt was not successful. Note that
     * successful attempt doesn't mean that node was actually started and joined topology. For large
     * topologies (> 100s nodes) it can take over 10 minutes for all nodes to start. See individual
     * node logs for details.
     *
     * @param hosts Startup parameters.
     * @param dflts Default values.
     * @param restart Whether to stop existing nodes. If {@code true}, all existing
     *      nodes on the host will be stopped before starting new ones. If
     *      {@code false}, nodes will be started only if there are less
     *      nodes on the host than expected.
     * @param timeout Connection timeout in milliseconds.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Future for collection of tuples, each containing host name, result (success of failure)
     *      and error message (if any).
     * @throws GridException In case of error.
     */
    public GridFuture<Collection<GridTuple3<String, Boolean, String>>> startNodes(Collection<Map<String, Object>> hosts,
        @Nullable Map<String, Object> dflts, boolean restart, int timeout, int maxConn) throws GridException;

    /**
     * Stops nodes satisfying optional set of predicates.
     * <p>
     * <b>NOTE:</b> {@code System.exit(GridGain.KILL_EXIT_CODE)} will be executed on each
     * stopping node. If you have other applications running in the same JVM along with GridGain,
     * those applications will be stopped as well.
     *
     * @throws GridException In case of error.
     */
    public void stopNodes() throws GridException;

    /**
     * Stops nodes defined by provided IDs.
     * <p>
     * <b>NOTE:</b> {@code System.exit(GridGain.KILL_EXIT_CODE)} will be executed on each
     * stopping node. If you have other applications running in the same JVM along with GridGain,
     * those applications will be stopped as well.
     *
     * @param ids IDs defining nodes to stop.
     * @throws GridException In case of error.
     */
    public void stopNodes(Collection<UUID> ids) throws GridException;

    /**
     * Restarts nodes satisfying optional set of predicates.
     * <p>
     * <b>NOTE:</b> this command only works for grid nodes started with GridGain
     * {@code ggstart.sh} or {@code ggstart.bat} scripts.
     *
     * @throws GridException In case of error.
     */
    public void restartNodes() throws GridException;

    /**
     * Restarts nodes defined by provided IDs.
     * <p>
     * <b>NOTE:</b> this command only works for grid nodes started with GridGain
     * {@code ggstart.sh} or {@code ggstart.bat} scripts.
     *
     * @param ids IDs defining nodes to restart.
     * @throws GridException In case of error.
     */
    public void restartNodes(Collection<UUID> ids) throws GridException;
}
