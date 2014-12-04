/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.events.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Listener for grid node discovery events. See
 * {@link GridDiscoverySpi} for information on how grid nodes get discovered.
 */
public interface GridDiscoverySpiListener {
    /**
     * Notification for grid node discovery events.
     *
     * @param type Node discovery event type. See {@link GridDiscoveryEvent}
     * @param topVer Topology version or {@code 0} if configured discovery SPI implementation
     *      does not support versioning.
     * @param node Node affected (e.g. newly joined node, left node, failed node or local node).
     * @param topSnapshot Topology snapshot after event has been occurred (e.g. if event is
     *      {@code EVT_NODE_JOINED}, then joined node will be in snapshot).
     * @param topHist Topology snapshots history.
     */
    public void onDiscovery(int type, long topVer, ClusterNode node, Collection<ClusterNode> topSnapshot,
        @Nullable Map<Long, Collection<ClusterNode>> topHist);
}
