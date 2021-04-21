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

package org.apache.ignite.client;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Thin client cluster group facade. Defines a cluster group which contains all or a subset of cluster nodes.
 */
public interface ClientClusterGroup {
    /**
     * Creates a cluster group over a given set of nodes.
     *
     * @param nodes Collection of nodes to create the cluster group from.
     * @return Cluster group for the provided grid nodes.
     */
    public ClientClusterGroup forNodes(Collection<? extends ClusterNode> nodes);

    /**
     * Creates a cluster group for the given node.
     *
     * @param node Node to create cluster group for.
     * @param nodes Optional additional nodes to include into the cluster group.
     * @return Cluster group for the given nodes.
     */
    public ClientClusterGroup forNode(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a cluster group for nodes other than the given nodes.
     *
     * @param node Node to exclude from the new cluster group.
     * @param nodes Optional additional nodes to exclude from the cluster group.
     * @return Cluster group that will contain all nodes from the original cluster group excluding
     *      the given nodes.
     */
    public ClientClusterGroup forOthers(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a cluster group for nodes not included into the given cluster group.
     *
     * @param prj Cluster group to exclude from the new cluster group.
     * @return Cluster group for nodes not included into the given cluster group.
     */
    public ClientClusterGroup forOthers(ClientClusterGroup prj);

    /**
     * Creates a cluster group over nodes with specified node IDs.
     *
     * @param ids Collection of node IDs.
     * @return Cluster group over nodes with the specified node IDs.
     */
    public ClientClusterGroup forNodeIds(Collection<UUID> ids);

    /**
     * Creates a cluster group for a node with the specified ID.
     *
     * @param id Node ID to get the cluster group for.
     * @param ids Optional additional node IDs to include into the cluster group.
     * @return Cluster group over the node with the specified node IDs.
     */
    public ClientClusterGroup forNodeId(UUID id, UUID... ids);

    /**
     * Creates a new cluster group which includes all nodes that pass the given predicate filter.
     *
     * @param p Predicate filter for nodes to include into the cluster group.
     * @return Cluster group for nodes that passed the predicate filter.
     */
    public ClientClusterGroup forPredicate(Predicate<ClusterNode> p);

    /**
     * Creates a new cluster group for nodes containing given name and value
     * specified in user attributes.
     * <p>
     * User attributes for every node are optional and can be specified in
     * grid node configuration. See {@link IgniteConfiguration#getUserAttributes()}
     * for more information.
     *
     * @param name Name of the attribute.
     * @param val Optional attribute value to match (if null, just check if attribute exists).
     * @return Cluster group for nodes containing specified attribute.
     */
    public ClientClusterGroup forAttribute(String name, @Nullable Object val);

    /**
     * Creates a cluster group of nodes started in server mode.
     *
     * @see Ignition#setClientMode(boolean)
     * @see IgniteConfiguration#setClientMode(boolean)
     * @return Cluster group of nodes started in server mode.
     */
    public ClientClusterGroup forServers();

    /**
     * Creates a cluster group of nodes started in client mode.

     * @see Ignition#setClientMode(boolean)
     * @see IgniteConfiguration#setClientMode(boolean)
     * @return Cluster group of nodes started in client mode.
     */
    public ClientClusterGroup forClients();

    /**
     * Creates a cluster group with one random node from the current cluster group.
     *
     * @return Cluster group containing one random node from the current cluster group.
     */
    public ClientClusterGroup forRandom();

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
    public ClientClusterGroup forOldest();

    /**
     * Creates a cluster group with one youngest node in the current cluster group.
     * The resulting cluster group is dynamic and will always pick the newest
     * node in the topology, even if more nodes entered after the cluster group
     * has been created.
     *
     * @return Cluster group containing one youngest node from the current cluster group.
     */
    public ClientClusterGroup forYoungest();

    /**
     * Gets cluster group consisting from the nodes in this cluster group residing on the
     * same host (with the same MAC address) as the given node.
     *
     * @param node Node to select the host for.
     * @return Cluster group for nodes residing on the same host as the specified node.
     */
    public ClientClusterGroup forHost(ClusterNode node);

    /**
     * Gets cluster group consisting from the nodes running on the hosts specified.
     *
     * @param host Host name to get nodes to put in cluster
     * @param hosts Host names to get nodes to put in cluster.
     * @return Cluster group for nodes residing on the hosts specified.
     */
    public ClientClusterGroup forHost(String host, String... hosts);

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
}
