/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for grid node discovery events. See
 * {@link DiscoverySpi} for information on how grid nodes get discovered.
 */
public interface DiscoverySpiListener {
    /**
     *  Notification of local node initialization. At the time this method is called, it is guaranteed that
     *  local node consistent ID is available, but the discovery process is not started yet.
     *  This method should not block for a long time since it blocks discovery.
     *
     * @param locNode Initialized local node.
     */
    public void onLocalNodeInitialized(ClusterNode locNode);

    /**
     * Notification for grid node discovery events.
     *
     * @param type Node discovery event type. See {@link DiscoveryEvent}
     * @param topVer Topology version or {@code 0} if configured discovery SPI implementation
     *      does not support versioning.
     * @param node Node affected (e.g. newly joined node, left node, failed node or local node).
     * @param topSnapshot Topology snapshot after event has been occurred (e.g. if event is
     *      {@code EVT_NODE_JOINED}, then joined node will be in snapshot).
     * @param topHist Topology snapshots history.
     * @param data Data for custom event.
     *
     * @return A future that will be completed when notification process has finished.
     */
    public IgniteFuture<?> onDiscovery(
        int type,
        long topVer,
        ClusterNode node,
        Collection<ClusterNode> topSnapshot,
        @Nullable Map<Long, Collection<ClusterNode>> topHist,
        @Nullable DiscoverySpiCustomMessage data);
}