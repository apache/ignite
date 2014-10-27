// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.cluster;

import org.gridgain.grid.design.configuration.*;
import org.gridgain.grid.design.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * A group of Ignite cluster nodes.
 *
 * @author @java.author
 * @version @java.version
 */
public interface ClusterGroup {
    /**
     * Creates a new cluster group over a given set of nodes.
     *
     * @param nodes Collection of nodes to create a cluster group from.
     * @return Cluster group over provided grid nodes.
     */
    public ClusterGroup forNodes(Collection<? extends ClusterNode> nodes);

    /**
     * Creates a new cluster group for the given node.
     *
     * @param node Node to get cluster group for.
     * @param nodes Optional additional nodes to include into cluster group.
     * @return Grid cluster group for the given node.
     */
    public ClusterGroup forNode(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a new cluster group for nodes other than the given nodes.
     *
     * @param node Node to exclude from new cluster group.
     * @param nodes Optional additional nodes to exclude from cluster group.
     * @return Cluster group that will contain all nodes from the original cluster group, excluding
     *      the given nodes.
     */
    public ClusterGroup forOthers(ClusterNode node, ClusterNode... nodes);

    /**
     * Creates a new cluster group for nodes not included into given cluster group.
     *
     * @param top Cluster group to exclude from new cluster group.
     * @return Cluster group for nodes not included into the passed in cluster group.
     */
    public ClusterGroup forOthers(ClusterGroup top);

    /**
     * Creates a new cluster group over nodes with specified node IDs.
     *
     * @param ids Collection of node IDs.
     * @return Cluster group over nodes with specified node IDs.
     */
    public ClusterGroup forNodeIds(Collection<UUID> ids);

    /**
     * Creates a new cluster group for the node with the specified ID.
     *
     * @param id Node ID to get cluster group for.
     * @param ids Optional additional node IDs to include into the cluster group.
     * @return Cluster group over the node with the specified node ID.
     */
    public ClusterGroup forNodeId(UUID id, UUID... ids);

    /**
     * Creates a new cluster group which includes all nodes that pass the given predicate filter.
     *
     * @param p Predicate filter for nodes to include into this cluster group.
     * @return Cluster group for nodes that passed the predicate filter.
     */
    public ClusterGroup forPredicate(IgnitePredicate<ClusterNode> p);

    /**
     * Creates a cluster group for nodes containing user attribute with specified name and value.
     * <p>
     * User attributes for every node are optional and can be specified in
     * Ignite configuration. See {@link IgniteConfiguration#getUserAttributes()}
     * for more information.
     *
     * @param name Name of the attribute.
     * @param val Optional attribute value to match.
     * @return Cluster group for nodes containing specified attribute.
     */
    public ClusterGroup forAttribute(String name, @Nullable String val);

    /**
     * Creates cluster group for all nodes that have a cache with the specified name.
     *
     * @param cacheName Cache name.
     * @param cacheNames Optional additional cache names to include into the cluster group.
     * @return Cluster group over nodes that have a cache the specified name.
     */
    public ClusterGroup forCache(String cacheName, @Nullable String... cacheNames);

    /**
     * Creates cluster group for all nodes that have a streamer with the specified name.
     *
     * @param streamerName Streamer name.
     * @param streamerNames Optional additional streamer names to include into cluster group.
     * @return Cluster group over nodes that have a streamer with a specified name running.
     */
    public ClusterGroup forStreamer(String streamerName, @Nullable String... streamerNames);

    /**
     * Gets a new cluster group consisting from the nodes in this cluster group excluding the local node.
     *
     * @return Cluster group consisting from the nodes in this cluster group excluding the local node.
     */
    public ClusterGroup forRemotes();

    /**
     * Gets a new cluster group consisting from the nodes in this cluster group residing on the
     * same host as the passed in node.
     *
     * @param node Node residing on the host for which the new cluster group should be created.
     * @return Cluster group for nodes residing on the same host as the passed in node.
     */
    public ClusterGroup forHost(ClusterNode node);

    /**
     * Gets a new cluster group consisting from the daemon nodes in this cluster group.
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
     * @return Grid cluster group consisting from the daemon nodes in this cluster group.
     */
    public ClusterGroup forDaemons();

    /**
     * Creates a new cluster group with one random node from the current cluster group.
     *
     * @return New cluster group with one random node from the current cluster group.
     */
    public ClusterGroup forRandom();

    /**
     * Creates a new cluster group with one oldest node in the current cluster group.
     * The resulting cluster group is dynamic and will always pick the next oldest
     * node if the previous one leaves topology, even after the cluster group has
     * been created.
     *
     * @return New cluster group with one oldest node from the current cluster group.
     */
    public ClusterGroup forOldest();

    /**
     * Creates a new cluster group with one youngest node in the current cluster group.
     * The resulting cluster group is dynamic and will always pick the newest
     * node in the topology, even if more nodes entered the cluster after the cluster group
     * has been created.
     *
     * @return New cluster group with one youngest node from the current cluster group.
     */
    public ClusterGroup forYoungest();

    /**
     * Predicate used to filter cluster nodes for this cluster group.
     *
     * @return Cluster group predicate.
     */
    public IgnitePredicate<ClusterNode> predicate();

    /**
     * Gets read-only collections of nodes in this cluster group.
     *
     * @return All nodes in this cluster group.
     */
    public Collection<ClusterNode> nodes();

    /**
     * Gets a node for given ID from this cluster group.
     *
     * @param nid Node ID.
     * @return Node with given ID from this cluster group or {@code null} if such node does not exist in this
     *      cluster group.
     */
    @Nullable public ClusterNode node(UUID nid);

    /**
     * Gets first node from the list of nodes in this cluster group. This method is specifically
     * useful for cluster groups containing one node only.
     *
     * @return First node from the list of nodes in this cluster group or {@code null} if cluster group is empty.
     */
    @Nullable public ClusterNode node();
}
