/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.scheduler.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Main entry-point for all GridGain APIs.
 * You can obtain an instance of {@code Grid} through {@link GridGain#grid()},
 * or for named grids you can use {@link GridGain#grid(String)}. Note that you
 * can have multiple instances of {@code Grid} running in the same VM by giving
 * each instance a different name.
 * <p>
 * Note that {@code Grid} extends {@link GridProjection} which means that it provides grid projection
 * functionality over the whole grid (instead os a subgroup of nodes).
 * <p>
 * In addition to {@link GridProjection} functionality, from here you can get the following:
 * <ul>
 * <li>{@link GridCache} - functionality for in-memory distributed cache.</li>
 * <li>{@link GridDataLoader} - functionality for loading data large amounts of data into cache.</li>
 * <li>{@link GridDr} - functionality for WAN-based Data Center Replication of in-memory cache.</li>
 * <li>{@link GridGgfs} - functionality for distributed Hadoop-compliant in-memory file system and map-reduce.</li>
 * <li>{@link GridStreamer} - functionality for streaming events workflow with queries and indexes into rolling windows.</li>
 * <li>{@link GridScheduler} - functionality for scheduling jobs using UNIX Cron syntax.</li>
 * <li>{@link GridProduct} - functionality for licence management and update and product related information.</li>
 * <li>{@link GridCompute} - functionality for executing tasks and closures on all grid nodes (inherited form {@link GridProjection}).</li>
 * <li>{@link GridMessaging} - functionality for topic-based message exchange on all grid nodes (inherited form {@link GridProjection}).</li>
 * <li>{@link GridEvents} - functionality for querying and listening to events on all grid nodes  (inherited form {@link GridProjection}).</li>
 * </ul>
 * {@code Grid} also provides a handle on {@link #nodeLocalMap()} which provides map-like functionality
 * linked to current grid node. Node-local map is useful for saving shared state between job executions
 * on the grid. Additionally you can also ping, start, and restart remote nodes, map keys to caching nodes,
 * and get other useful information about topology.
 */
public interface Grid extends GridProjection, AutoCloseable {
    /**
     * Gets the name of the grid this grid instance (and correspondingly its local node) belongs to.
     * Note that single Java VM can have multiple grid instances all belonging to different grids. Grid
     * name allows to indicate to what grid this particular grid instance (i.e. grid runtime and its
     * local node) belongs to.
     * <p>
     * If default grid instance is used, then
     * {@code null} is returned. Refer to {@link GridGain} documentation
     * for information on how to start named grids.
     *
     * @return Name of the grid, or {@code null} for default grid.
     */
    public String name();

    /**
     * Gets grid's logger.
     *
     * @return Grid's logger.
     */
    public GridLogger log();

    /**
     * Gets node-local storage instance.
     * <p>
     * Node-local values are similar to thread locals in a way that these values are not
     * distributed and kept only on local node (similar like thread local values are attached to the
     * current thread only). Node-local values are used primarily by closures executed from the remote
     * nodes to keep intermediate state on the local node between executions.
     * <p>
     * There's only one instance of node local storage per local node. Node local storage is
     * based on {@link ConcurrentMap} and is safe for multi-threaded access.
     *
     * @return Node local storage instance for the local node.
     */
    public <K, V> GridNodeLocalMap<K, V> nodeLocalMap();

    /**
     * Gets the configuration of this grid instance.
     * <p>
     * <b>NOTE:</b>
     * <br>
     * SPIs obtains through this method should never be used directly. SPIs provide
     * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
     * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
     * via this method to check its configuration properties or call other non-SPI
     * methods.
     *
     * @return Grid configuration instance.
     */
    public GridConfiguration configuration();

    /**
     * Gets monadic projection consisting from the local node.
     *
     * @return Monadic projection consisting from the local node.
     */
    public GridProjection forLocal();

    /**
     * Gets information about product as well as license management capabilities.
     *
     * @return Instance of product.
     */
    public GridProduct product();

    /**
     * Gets an instance of cron-based scheduler.
     *
     * @return Instance of scheduler.
     */
    public GridScheduler scheduler();

    /**
     * Gets an instance of {@code GridSecurity} interface. Available in enterprise edition only.
     *
     * @return Instance of {@code GridSecurity} interface.
     */
    public GridSecurity security();

    /**
     * Gets an instance of {@code GridPortables} interface. Available in enterprise edition only.
     *
     * @return Instance of {@code GridPortables} interface.
     */
    public GridPortables portables();

    /**
     * Gets an instance of Data Center Replication.
     *
     * @return Instance of Data Center Replication.
     */
    public GridDr dr();

    /**
     * Gets the cache instance for the given name, if one does not
     * exist {@link IllegalArgumentException} will be thrown.
     * Note that in case named cache instance is used as GGFS data or meta cache, {@link IllegalStateException}
     * will be thrown.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @param name Cache name.
     * @return Cache instance for given name.
     * @see GridGgfsConfiguration
     * @see GridGgfsConfiguration#getDataCacheName()
     * @see GridGgfsConfiguration#getMetaCacheName()
     */
    public <K, V> GridCache<K, V> cache(@Nullable String name);

    /**
     * Gets all configured caches.
     * Caches that are used as GGFS meta and data caches will not be returned in resulting collection.
     *
     * @see GridGgfsConfiguration
     * @see GridGgfsConfiguration#getDataCacheName()
     * @see GridGgfsConfiguration#getMetaCacheName()
     * @return All configured caches.
     */
    public Collection<GridCache<?, ?>> caches();

    /**
     * Gets a new instance of data loader associated with given cache name. Data loader
     * is responsible for loading external data into in-memory data grid. For more information
     * refer to {@link GridDataLoader} documentation.
     *
     * @param cacheName Cache name ({@code null} for default cache).
     * @return Data loader.
     */
    public <K, V> GridDataLoader<K, V> dataLoader(@Nullable String cacheName);

    /**
     * Gets an instance of GGFS - GridGain In-Memory File System, if one is not
     * configured then {@link IllegalArgumentException} will be thrown.
     * <p>
     * GGFS is fully compliant with Hadoop {@code FileSystem} APIs and can
     * be plugged into Hadoop installations. For more information refer to
     * documentation on Hadoop integration shipped with GridGain.
     *
     * @param name GGFS name.
     * @return GGFS instance.
     */
    public GridGgfs ggfs(String name);

    /**
     * Gets all instances of the grid file systems.
     *
     * @return Collection of grid file systems instances.
     */
    public Collection<GridGgfs> ggfss();

    /**
     * Gets an instance of Hadoop.
     *
     * @return Hadoop instance.
     */
    public GridHadoop hadoop();

    /**
     * Gets an instance of streamer by name, if one does not exist then
     * {@link IllegalArgumentException} will be thrown.
     *
     * @param name Streamer name.
     * @return Streamer for given name.
     */
    public GridStreamer streamer(@Nullable String name);

    /**
     * Gets all instances of streamers.
     *
     * @return Collection of all streamer instances.
     */
    public Collection<GridStreamer> streamers();

    /**
     * Gets local grid node.
     *
     * @return Local grid node.
     */
    public GridNode localNode();

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

    /**
     * Gets current topology version. In case of TCP discovery
     * (see {@link GridTcpDiscoverySpi}) topology versions
     * are sequential - they start from {@code '1'} and get incremented every time whenever a
     * node joins or leaves. For other discovery SPIs topology versions may not be (and likely are
     * not) sequential.
     *
     * @return Current topology version.
     */
    public long topologyVersion();

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
    @Nullable public Collection<GridNode> topology(long topVer) throws UnsupportedOperationException;

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
     * Closes {@code this} instance of grid. This method is identical to calling
     * {@link G#stop(String, boolean) G.stop(gridName, true)}.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws GridException If failed to stop grid.
     */
    @Override public void close() throws GridException;
}
