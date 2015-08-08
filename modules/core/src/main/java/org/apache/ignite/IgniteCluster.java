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

package org.apache.ignite;

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Represents whole cluster (all available nodes) and also provides a handle on {@link #nodeLocalMap()} which
 * provides map-like functionality linked to current grid node. Node-local map is useful for saving shared state
 * between job executions on the grid. Additionally you can also ping, start, and restart remote nodes, map keys to
 * caching nodes, and get other useful information about topology.
 */
public interface IgniteCluster extends ClusterGroup, IgniteAsyncSupport {
    /**
     * Gets local grid node.
     *
     * @return Local grid node.
     */
    public ClusterNode localNode();

    /**
     * Gets a cluster group consisting from the local node.
     *
     * @return Cluster group consisting from the local node.
     */
    public ClusterGroup forLocal();

    /**
     * Gets node-local storage instance.
     * <p>
     * Node-local values are similar to thread locals in a way that these values are not
     * distributed and kept only on local node (similar like thread local values are attached to the
     * current thread only). Node-local values are used primarily by closures executed from the remote
     * nodes to keep intermediate state on the local node between executions.
     * <p>
     * There's only one instance of node local storage per local node. Node local storage is
     * based on {@link java.util.concurrent.ConcurrentMap} and is safe for multi-threaded access.
     *
     * @return Node local storage instance for the local node.
     */
    public <K, V> ConcurrentMap<K, V> nodeLocalMap();

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
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public boolean pingNode(UUID nodeId);

    /**
     * Gets current topology version. In case of TCP discovery
     * (see {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}) topology versions
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
     *      topology history. Currently only {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}
     *      supports topology history.
     */
    public Collection<ClusterNode> topology(long topVer) throws UnsupportedOperationException;

    /**
     * This method provides ability to detect which cache keys are mapped to which nodes
     * on cache instance with given name. Use it to determine which nodes are storing which
     * keys prior to sending jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node mapped to all keys.</li>
     * <li>
     *      For fully replicated caches, {@link AffinityFunction} is
     *      used to determine which keys are mapped to which groups of nodes.
     * </li>
     * <li>For partitioned caches, the returned map represents node-to-key affinity.</li>
     * </ul>
     *
     * @param cacheName Cache name, if {@code null}, then default cache instance is used.
     * @param keys Cache keys to map to nodes.
     * @return Map of nodes to cache keys or empty map if there are no alive nodes for this cache.
     * @throws IgniteException If failed to map cache keys.
     */
    public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys) throws IgniteException;

    /**
     * This method provides ability to detect which cache keys are mapped to which nodes
     * on cache instance with given name. Use it to determine which nodes are storing which
     * keys prior to sending jobs that access these keys.
     * <p>
     * This method works as following:
     * <ul>
     * <li>For local caches it returns only local node ID.</li>
     * <li>
     *      For fully replicated caches first node ID returned by {@link AffinityFunction}
     *      is returned.
     * </li>
     * <li>For partitioned caches, the returned node ID is the primary node for the key.</li>
     * </ul>
     *
     * @param cacheName Cache name, if {@code null}, then default cache instance is used.
     * @param key Cache key to map to a node.
     * @return Primary node for the key or {@code null} if cache with given name
     *      is not present in the grid.
     * @throws IgniteException If failed to map key.
     */
    public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) throws IgniteException;

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
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param file Configuration file.
     * @param restart Whether to stop existing nodes. If {@code true}, all existing
     *      nodes on the host will be stopped before starting new ones. If
     *      {@code false}, nodes will be started only if there are less
     *      nodes on the host than expected.
     * @param timeout Connection timeout.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Collection of start node results, each containing host name, result (success or failure)
     *      and error message (if any).
     * @throws IgniteException In case of error.
     */
    @IgniteAsyncSupported
    public Collection<ClusterStartNodeResult> startNodes(File file, boolean restart, int timeout,
        int maxConn) throws IgniteException;

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
     *             <td><b>igniteHome</b></td>
     *             <td>String</td>
     *             <td>
     *                 Path to Ignite installation folder. If not defined, IGNITE_HOME
     *                 environment variable must be set on remote hosts.
     *             </td>
     *         </tr>
     *         <tr>
     *             <td><b>cfg</b></td>
     *             <td>String</td>
     *             <td>Path to configuration file (relative to {@code igniteHome}).</td>
     *         </tr>
     *         <tr>
     *             <td><b>script</b></td>
     *             <td>String</td>
     *             <td>
     *                 Custom startup script file name and path (relative to {@code igniteHome}).
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
     * <p>
     * Supports asynchronous execution (see {@link IgniteAsyncSupport}).
     *
     * @param hosts Startup parameters.
     * @param dflts Default values.
     * @param restart Whether to stop existing nodes. If {@code true}, all existing
     *      nodes on the host will be stopped before starting new ones. If
     *      {@code false}, nodes will be started only if there are less
     *      nodes on the host than expected.
     * @param timeout Connection timeout in milliseconds.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Collection of start node results, each containing host name, result (success or failure)
     *      and error message (if any).
     * @throws IgniteException In case of error.
     */
    @IgniteAsyncSupported
    public Collection<ClusterStartNodeResult> startNodes(Collection<Map<String, Object>> hosts,
        @Nullable Map<String, Object> dflts, boolean restart, int timeout, int maxConn) throws IgniteException;

    /**
     * Stops nodes satisfying optional set of predicates.
     * <p>
     * <b>NOTE:</b> {@code System.exit(Ignition.KILL_EXIT_CODE)} will be executed on each
     * stopping node. If you have other applications running in the same JVM along with Ignition,
     * those applications will be stopped as well.
     *
     * @throws IgniteException In case of error.
     */
    public void stopNodes() throws IgniteException;

    /**
     * Stops nodes defined by provided IDs.
     * <p>
     * <b>NOTE:</b> {@code System.exit(Ignition.KILL_EXIT_CODE)} will be executed on each
     * stopping node. If you have other applications running in the same JVM along with Ignition,
     * those applications will be stopped as well.
     *
     * @param ids IDs defining nodes to stop.
     * @throws IgniteException In case of error.
     */
    public void stopNodes(Collection<UUID> ids) throws IgniteException;

    /**
     * Restarts nodes satisfying optional set of predicates.
     * <p>
     * <b>NOTE:</b> this command only works for grid nodes started with Ignition
     * {@code ignite.sh} or {@code ignite.bat} scripts.
     *
     * @throws IgniteException In case of error.
     */
    public void restartNodes() throws IgniteException;

    /**
     * Restarts nodes defined by provided IDs.
     * <p>
     * <b>NOTE:</b> this command only works for grid nodes started with Ignition
     * {@code ignite.sh} or {@code ignite.bat} scripts.
     *
     * @param ids IDs defining nodes to restart.
     * @throws IgniteException In case of error.
     */
    public void restartNodes(Collection<UUID> ids) throws IgniteException;

    /**
     * Resets local I/O, job, and task execution metrics.
     */
    public void resetMetrics();

    /**
     * If local client node disconnected from cluster returns future
     * that will be completed when client reconnected.
     *
     * @return Future that will be completed when client reconnected.
     */
    @Nullable public IgniteFuture<?> clientReconnectFuture();

    /** {@inheritDoc} */
    @Override public IgniteCluster withAsync();
}
