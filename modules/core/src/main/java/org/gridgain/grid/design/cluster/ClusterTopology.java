// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.cluster;

import org.gridgain.grid.design.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface ClusterTopology {
    /**
     * @return Topology node predicate.
     */
    public IgnitePredicate<ClusterNode> predicate();

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
}
