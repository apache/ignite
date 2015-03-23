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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Defines grid projection which represents a common functionality over a group of nodes.
 * The {@link Ignite} interface itself also extends {@code GridProjection} which makes
 * an instance of {@link Ignite} a projection over all grid nodes.
 * <h1 class="header">Clustering</h1>
 * Grid projection allows to group grid nodes into various subgroups to perform distributed
 * operations on them. All {@code 'forXXX(...)'} methods will create a child grid projection
 * from existing projection. If you create a new projection from current one, then the resulting
 * projection will include a subset of nodes from current projection. The following code snippet
 * shows how to create and nest grid projections:
 * <pre name="code" class="java">
 * Grid g = Ignition.ignite();
 *
 * // Projection over remote nodes.
 * GridProjection remoteNodes = g.forRemotes();
 *
 * // Projection over random remote node.
 * GridProjection randomNode = remoteNodes.forRandom();
 *
 * // Projection over all nodes with cache named "myCache" enabled.
 * GridProjection cacheNodes = g.forCacheNodes("myCache");
 *
 * // Projection over all nodes that have user attribute "group" set to value "worker".
 * GridProjection workerNodes = g.forAttribute("group", "worker");
 * </pre>
 * <h1 class="header">Features</h1>
 * Grid projection provides the following functionality over the underlying group of nodes:
 * <ul>
 * <li>{@link IgniteCompute} - functionality for executing tasks and closures over nodes in this projection.</li>
 * <li>{@link IgniteMessaging} - functionality for topic-based message exchange over nodes in this projection.</li>
 * <li>{@link IgniteEvents} - functionality for querying and listening to events on nodes in this projection.</li>
 * </ul>
 */
public interface ClusterGroup {
    /**
     * Gets instance of grid.
     *
     * @return Grid instance.
     */
    public Ignite ignite();

    /**
     * Creates a grid projection over a given set of nodes.
     *
     * @param nodes Collection of nodes to create a projection from.
     * @return Projection over provided grid nodes.
     */
    public ClusterGroup forNodes(Collection<? extends ClusterNode> nodes);

    /**
     * Creates a grid projection for the given node.
     *
     * @param node Node to get projection for.
     * @param nodes Optional additional nodes to include into projection.
     * @return Grid projection for the given node.
     */
    public ClusterGroup forNode(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a grid projection for nodes other than given nodes.
     *
     * @param node Node to exclude from new grid projection.
     * @param nodes Optional additional nodes to exclude from projection.
     * @return Projection that will contain all nodes that original projection contained excluding
     *      given nodes.
     */
    public ClusterGroup forOthers(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a grid projection for nodes not included into given projection.
     *
     * @param prj Projection to exclude from new grid projection.
     * @return Projection for nodes not included into given projection.
     */
    public ClusterGroup forOthers(ClusterGroup prj);

    /**
     * Creates a grid projection over nodes with specified node IDs.
     *
     * @param ids Collection of node IDs.
     * @return Projection over nodes with specified node IDs.
     */
    public ClusterGroup forNodeIds(Collection<UUID> ids);

    /**
     * Creates a grid projection for a node with specified ID.
     *
     * @param id Node ID to get projection for.
     * @param ids Optional additional node IDs to include into projection.
     * @return Projection over node with specified node ID.
     */
    public ClusterGroup forNodeId(UUID id, UUID... ids);

    /**
     * Creates a grid projection which includes all nodes that pass the given predicate filter.
     *
     * @param p Predicate filter for nodes to include into this projection.
     * @return Grid projection for nodes that passed the predicate filter.
     */
    public ClusterGroup forPredicate(IgnitePredicate<ClusterNode> p);

    /**
     * Creates projection for nodes containing given name and value
     * specified in user attributes.
     * <p>
     * User attributes for every node are optional and can be specified in
     * grid node configuration. See {@link IgniteConfiguration#getUserAttributes()}
     * for more information.
     *
     * @param name Name of the attribute.
     * @param val Optional attribute value to match.
     * @return Grid projection for nodes containing specified attribute.
     */
    public ClusterGroup forAttribute(String name, @Nullable String val);

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
     * Creates projection for all nodes that have cache with specified name running.
     *
     * @param cacheName Cache name.
     * @return Projection over nodes that have specified cache running.
     */
    public ClusterGroup forCacheNodes(String cacheName);

    /**
     * Creates projection for all affinity nodes that have cache with specified name running.
     *
     * @param cacheName Cache name.
     * @return Projection over nodes that have specified cache running.
     */
    public ClusterGroup forDataNodes(String cacheName);

    /**
     * Creates projection for all non-affinity nodes that have cache with specified name running.
     *
     * @param cacheName Cache name.
     * @return Projection over nodes that have specified cache running.
     */
    public ClusterGroup forClientNodes(String cacheName);

    /**
     * Gets grid projection consisting from the nodes in this projection excluding the local node.
     *
     * @return Grid projection consisting from the nodes in this projection excluding the local node, if any.
     */
    public ClusterGroup forRemotes();

    /**
     * Gets grid projection consisting from the nodes in this projection residing on the
     * same host as given node.
     *
     * @param node Node residing on the host for which projection is created.
     * @return Projection for nodes residing on the same host as passed in node.
     */
    public ClusterGroup forHost(ClusterNode node);

    /**
     * Gets projection consisting from the daemon nodes in this projection.
     * <p>
     * Daemon nodes are the usual grid nodes that participate in topology but not
     * visible on the main APIs, i.e. they are not part of any projections. The only
     * way to see daemon nodes is to use this method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on Ignite and needs to participate in the topology but also needs to be
     * excluded from "normal" topology so that it won't participate in task execution
     * or in-memory data grid storage.
     *
     * @return Grid projection consisting from the daemon nodes in this projection.
     */
    public ClusterGroup forDaemons();

    /**
     * Creates grid projection with one random node from current projection.
     *
     * @return Grid projection with one random node from current projection.
     */
    public ClusterGroup forRandom();

    /**
     * Creates grid projection with one oldest node in the current projection.
     * The resulting projection is dynamic and will always pick the next oldest
     * node if the previous one leaves topology even after the projection has
     * been created.
     *
     * @return Grid projection with one oldest node from the current projection.
     */
    public ClusterGroup forOldest();

    /**
     * Creates grid projection with one youngest node in the current projection.
     * The resulting projection is dynamic and will always pick the newest
     * node in the topology, even if more nodes entered after the projection
     * has been created.
     *
     * @return Grid projection with one youngest node from the current projection.
     */
    public ClusterGroup forYoungest();

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
     * Gets first node from the list of nodes in this projection. This method is specifically
     * useful for projection over one node only.
     *
     * @return First node from the list of nodes in this projection or {@code null} if projection is empty.
     */
    @Nullable public ClusterNode node();

    /**
     * Gets predicate that defines a subset of nodes for this projection.
     *
     * @return Predicate that defines a subset of nodes for this projection.
     */
    public IgnitePredicate<ClusterNode> predicate();

    /**
     * Gets a metrics snapshot for this projection.
     *
     * @return Grid projection metrics snapshot.
     * @throws IgniteException If projection is empty.
     */
    public ClusterMetrics metrics() throws IgniteException;
}
