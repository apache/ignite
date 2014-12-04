/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity;

import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Affinity function context. This context is passed to {@link GridCacheAffinityFunction} for
 * partition reassignment on every topology change event.
 */
public interface GridCacheAffinityFunctionContext {
    /**
     * Gets affinity assignment for given partition on previous topology version. First node in returned list is
     * a primary node, other nodes are backups.
     *
     * @param part Partition to get previous assignment for.
     * @return List of nodes assigned to given partition on previous topology version or {@code null}
     *      if this information is not available.
     */
    @Nullable public List<ClusterNode> previousAssignment(int part);

    /**
     * Gets number of backups for new assignment.
     *
     * @return Number of backups for new assignment.
     */
    public int backups();

    /**
     * Gets current topology snapshot. Snapshot will contain only nodes on which particular cache is configured.
     * List of passed nodes is guaranteed to be sorted in a same order on all nodes on which partition assignment
     * is performed.
     *
     * @return Cache topology snapshot.
     */
    public List<ClusterNode> currentTopologySnapshot();

    /**
     * Gets current topology version number.
     *
     * @return Current topology version number.
     */
    public long currentTopologyVersion();

    /**
     * Gets discovery event caused topology change.
     *
     * @return Discovery event caused latest topology change or {@code null} if this information is
     *      not available.
     */
    @Nullable public GridDiscoveryEvent discoveryEvent();
}
