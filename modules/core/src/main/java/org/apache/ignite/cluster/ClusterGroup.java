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

package org.apache.ignite.cluster;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Defines a cluster group which contains all or a subset of cluster nodes.
 * The {@link IgniteCluster} interface itself also extends {@code ClusterGroup} which makes
 * an instance of {@link IgniteCluster} into a cluster group containing all cluster nodes.
 * <h1 class="header">Clustering</h1>
 * Cluster group allows to group cluster nodes into various subgroups to perform distributed
 * operations on them. All {@code 'forXXX(...)'} methods will create a child cluster group
 * from the existing cluster group. If you create a new cluster group from the current one, then
 * the resulting cluster group will include a subset of nodes from the current one. The following
 * code shows how to create and nest cluster groups:
 * <pre name="code" class="java">
 * Ignite ignite = Ignition.ignite();
 *
 * IgniteCluster cluster = ignite.cluster();
 *
 * // Cluster group over remote nodes.
 * ClusterGroup remoteNodes = cluster.forRemotes();
 *
 * // Cluster group over random remote node.
 * ClusterGroup randomNode = remoteNodes.forRandom();
 *
 * // Cluster group over all nodes with cache named "myCache" enabled.
 * ClusterGroup cacheNodes = cluster.forCacheNodes("myCache");
 *
 * // Cluster group over all nodes that have the user attribute "group" set to the value "worker".
 * ClusterGroup workerNodes = cluster.forAttribute("group", "worker");
 * </pre>
 */
public interface ClusterGroup {
    /**
     * Gets instance of grid.
     *
     * @return Grid instance.
     */
    public Ignite ignite();

    /**
     * Creates a cluster group over a given set of nodes.
     *
     * @param nodes Collection of nodes to create the cluster group from.
     * @return Cluster group for the provided grid nodes.
     */
    public ClusterGroup forNodes(Collection<? extends ClusterNode> nodes);

    /**
     * Creates a cluster group for the given node.
     *
     * @param node Node to create cluster group for.
     * @param nodes Optional additional nodes to include into the cluster group.
     * @return Cluster group for the given nodes.
     */
    public ClusterGroup forNode(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a cluster group for nodes other than the given nodes.
     *
     * @param node Node to exclude from the new cluster group.
     * @param nodes Optional additional nodes to exclude from the cluster group.
     * @return Cluster group that will contain all nodes from the original cluster group excluding
     *      the given nodes.
     */
    public ClusterGroup forOthers(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a cluster group for nodes not included into the given cluster group.
     *
     * @param prj Cluster group to exclude from the new cluster group.
     * @return Cluster group for nodes not included into the given cluster group.
     */
    public ClusterGroup forOthers(ClusterGroup prj);

    /**
     * Creates a cluster group over nodes with specified node IDs.
     *
     * @param ids Collection of node IDs.
     * @return Cluster group over nodes with the specified node IDs.
     */
    public ClusterGroup forNodeIds(Collection<UUID> ids);

    /**
     * Creates a cluster group for a node with the specified ID.
     *
     * @param id Node ID to get the cluster group for.
     * @param ids Optional additional node IDs to include into the cluster group.
     * @return Cluster group over the node with the specified node IDs.
     */
    public ClusterGroup forNodeId(UUID id, UUID... ids);

    /**
     * Creates a new cluster group which includes all nodes that pass the given predicate filter.
     *
     * @param p Predicate filter for nodes to include into the cluster group.
     * @return Cluster group for nodes that passed the predicate filter.
     */
    public ClusterGroup forPredicate(IgnitePredicate<ClusterNode> p);

    /**
     * Creates a new cluster group for nodes containing given name and value
     * specified in user attributes.
     * <p>
     * User attributes for every node are optional and can be specified in
     * grid node configuration. See {@link IgniteConfiguration#getUserAttributes()}
     * for more information.
     *
     * @param name Name of the attribute.
     * @param val Optional attribute value to match.
     * @return Cluster group for nodes containing specified attribute.
     */
    public ClusterGroup forAttribute(String name, @Nullable Object val);

    /**
     * Creates a cluster group of nodes started in server mode.
     *
     * @see Ignition#setClientMode(boolean)
     * @see IgniteConfiguration#setClientMode(boolean)
     * @return Cluster group of nodes started in server mode.
     */
    public ClusterGroup forServers();

    /**
     * Creates a cluster group of nodes started in client mode.

     * @see Ignition#setClientMode(boolean)
     * @see IgniteConfiguration#setClientMode(boolean)
     * @return Cluster group of nodes started in client mode.
     */
    public ClusterGroup forClients();

    /**
     * Creates a cluster group for all nodes that have cache with specified name, either in client or server modes.
     *
     * @param cacheName Cache name.
     * @return Cluster group over nodes that have specified cache running.
     */
    public ClusterGroup forCacheNodes(String cacheName);

    /**
     * Creates a cluster group for all data nodes that have the cache with the specified name running.
     *
     * @param cacheName Cache name.
     * @return Cluster group over nodes that have the cache with the specified name running.
     */
    public ClusterGroup forDataNodes(String cacheName);

    /**
     * Creates a cluster group for all client nodes that access cache with the specified name.
     *
     * @param cacheName Cache name.
     * @return Cluster group over nodes that have the specified cache running.
     */
    public ClusterGroup forClientNodes(String cacheName);

    /**
     * Gets cluster group consisting from the nodes in this cluster group excluding the local node.
     *
     * @return Cluster group consisting from the nodes in this cluster group excluding the local node.
     */
    public ClusterGroup forRemotes();

    /**
     * Gets cluster group consisting from the nodes in this cluster group residing on the
     * same host as the given node.
     *
     * @param node Node to select the host for.
     * @return Cluster group for nodes residing on the same host as the specified node.
     */
    public ClusterGroup forHost(ClusterNode node);

    /**
     * Gets cluster group consisting from the nodes running on the hosts specified.
     *
     * @param host Host name to get nodes to put in cluster
     * @param hosts Host names to get nodes to put in cluster.
     * @return Cluster group for nodes residing on the hosts specified.
     */
    public ClusterGroup forHost(String host, String... hosts);

    /**
     * Gets a cluster group consisting from the daemon nodes.
     * <p>
     * Daemon nodes are the usual grid nodes that participate in topology but not
     * visible on the main APIs, i.e. they are not part of any cluster group. The only
     * way to see daemon nodes is to use this method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on Ignite and needs to participate in the topology, but also needs to be
     * excluded from the "normal" topology, so that it won't participate in the task execution
     * or in-memory data grid storage.
     *
     * @return Cluster group consisting from the daemon nodes.
     */
    public ClusterGroup forDaemons();

    /**
     * Creates a cluster group with one random node from the current cluster group.
     *
     * @return Cluster group containing one random node from the current cluster group.
     */
    public ClusterGroup forRandom();

    /**
     * Creates a cluster group with one oldest node from the current cluster group.
     * The resulting cluster group is dynamic and will always pick the next oldest
     * node if the previous one leaves topology even after the cluster group has
     * been created.
     * <p>
     * Use {@link #node()} method to get the oldest node.
     *
     * @return Cluster group containing one oldest node from the current cluster group.
     */
    public ClusterGroup forOldest();

    /**
     * Creates a cluster group with one youngest node in the current cluster group.
     * The resulting cluster group is dynamic and will always pick the newest
     * node in the topology, even if more nodes entered after the cluster group
     * has been created.
     *
     * @return Cluster group containing one youngest node from the current cluster group.
     */
    public ClusterGroup forYoungest();

    /**
     * Gets the read-only collection of nodes in this cluster group.
     *
     * @return All nodes in this cluster group.
     */
    public Collection<ClusterNode> nodes();

    /**
     * Gets a node for given ID from this cluster group.
     *
     * @param nid Node ID.
     * @return Node with given ID from this cluster group or {@code null}, if such node does not exist.
     */
    public ClusterNode node(UUID nid);

    /**
     * Gets first node from the list of nodes in this cluster group. This method is specifically
     * useful for cluster groups with one node only.
     *
     * @return First node from the list of nodes in this cluster group or {@code null} if the cluster group is empty.
     */
    public ClusterNode node();

    /**
     * Gets the read-only collection of hostnames in this cluster group.
     *
     * @return All hostnames in this cluster group.
     */
    public Collection<String> hostNames();

    /**
     * Gets predicate that defines a subset of nodes for this cluster group.
     *
     * @return Predicate that defines a subset of nodes for this cluster group.
     */
    public IgnitePredicate<ClusterNode> predicate();

    /**
     * Gets a metrics snapshot for this cluster group.
     *
     * @return Metrics snapshot.
     * @throws IgniteException If this cluster group is empty.
     */
    public ClusterMetrics metrics() throws IgniteException;
}