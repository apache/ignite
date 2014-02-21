// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridProjection {
    /**
     * @return TODO
     */
    public Grid grid();

    /**
     * @return TODO
     */
    public GridCompute compute();

    /**
     * @return TODO
     */
    public GridMessaging message();

    /**
     * @return TODO
     */
    public GridEvents events();

    /**
     * Creates monadic projection with a given set of nodes out of this projection.
     * Note that nodes not in this projection at the moment of call will be excluded.
     *
     * @param nodes Collection of nodes to create a projection from.
     * @return Monadic projection with given nodes.
     */
    public GridProjection forNodes(Collection<? extends GridNode> nodes);

    /**
     * // TODO
     * @param node Node to get projection for.
     * @param nodes Optional additional nodes to include into projection.
     * @return Grid projection for given node.
     */
    public GridProjection forNode(GridNode node, GridNode... nodes);

    /**
     * // TODO
     * @param node Node to exclude from grid projection.
     * @return Projection that will contain all nodes that original projection contained excluding
     *      given node.
     */
    public GridProjection forOthers(GridNode node);

    /**
     * Creates monadic projection with a given set of node IDs out of this projection.
     * Note that nodes not in this projection at the moment of call will excluded.
     *
     * @param ids Collection of node IDs defining collection of nodes to create projection with.
     * @return Monadic projection made out of nodes with given IDs.
     */
    public GridProjection forNodeIds(Collection<UUID> ids);

    /**
     * TODO: javadoc
     *
     * @param nodeId Node ID to get projection for.
     * @param nodeIds Optional additional node IDs to include into projection.
     * @return Projection over node with specified node ID.
     */
    public GridProjection forNodeId(UUID nodeId, UUID... nodeIds);

    /**
     * Creates monadic projection with the nodes from this projection that also satisfy given
     * set of predicates.
     *
     * @param p Predicate that all should evaluate to {@code true} for a node
     *      to be included in the final projection.
     * @return Monadic projection.
     * @see PN
     */
    public GridProjection forPredicate(GridPredicate<GridNode> p);

    /**
     * Creates monadic projection with the nodes from this projection that have given node
     * attribute with optional value. If value is {@code null} than simple attribute presence
     * (with any value) will be used for inclusion of the node.
     *
     * @param n Name of the attribute.
     * @param v Optional attribute value to match.
     * @return Monadic projection.
     */
    public GridProjection forAttribute(String n, @Nullable String v);

    /**
     * Creates monadic projection with the nodes from this projection that have configured
     * all caches with given names. If node does not have at least one of these caches, it will not
     * be included to result projection.
     *
     * @param cacheName Cache name.
     * @param cacheNames Optional additional cache names to include into projection.
     * @return Projection over nodes that have specified cache running.
     */
    public GridProjection forCache(String cacheName, @Nullable String... cacheNames);

    /**
     * Creates monadic projection with the nodes from this projection that have configured
     * streamers with given names. If node does not have at least one of these streamers, it will not
     * be included to result projection.
     *
     * @param streamerName Streamer name.
     * @param streamerNames Optional additional streamer names to include into projection.
     * @return Projection over nodes that have specified streamer running.
     */
    public GridProjection forStreamer(String streamerName, @Nullable String... streamerNames);

    /**
     * Gets monadic projection consisting from the nodes in this projection excluding the local node, if any.
     *
     * @return Monadic projection consisting from the nodes in this projection excluding the local node, if any.
     */
    public GridProjection forRemotes();

    /**
     * Gets monadic projection consisting from the nodes in this projection residing on the
     * same host as given node.
     *
     * @param node Node residing on the host for which projection is created.
     * @return Projection for nodes residing on the same host as passed in node.
     */
    public GridProjection forHost(GridNode node);

    /**
     * Gets monadic projection consisting from the daemon nodes in this projection.
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
     * @return Monadic projection consisting from the daemon nodes in this projection.
     */
    public GridProjection forDaemons();

    /**
     * Creates monadic projection with one random node from current projection.
     *
     * @return Monadic projection.
     */
    public GridProjection forRandom();

    /**
     * Gets read-only collections of nodes in this projection.
     *
     * @return All nodes in this projection.
     */
    public Collection<GridNode> nodes();

    /**
     * Gets a node for given ID from this optionally filtered projection.
     *
     * @param nid Node ID.
     * @return Node with given ID from this projection or {@code null} if such node does not exist in this
     *      projection.
     */
    @Nullable public GridNode node(UUID nid);

    /**
     * Tells whether or not this projection is dynamic.
     * <p>
     * Dynamic projection is based on predicate and in any particular moment of time
     * can consist of a different set of nodes. Static project does not change and always
     * consist of the same set of nodes (excluding the node that have left the topology
     * since the creation of the static projection).
     *
     * @return Whether or not projection is dynamic.
     */
    public boolean dynamic();

    /**
     * Gets predicate that defines a subset of nodes for this projection at the time of the call.
     * Note that if projection is based on dynamically changing set of nodes - the predicate
     * returning from this method will change accordingly from call to call.
     *
     * @return Predicate that defines a subset of nodes for this projection.
     */
    public GridPredicate<GridNode> predicate();

    /**
     * Gets a metrics snapshot for this projection.
     *
     * @return Grid project metrics snapshot.
     * @throws GridException If projection is empty.
     * @see GridNode#metrics()
     */
    public GridProjectionMetrics metrics() throws GridException;
}
