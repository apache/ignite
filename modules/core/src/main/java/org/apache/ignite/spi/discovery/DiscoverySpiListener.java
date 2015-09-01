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

package org.apache.ignite.spi.discovery;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for grid node discovery events. See
 * {@link DiscoverySpi} for information on how grid nodes get discovered.
 */
public interface DiscoverySpiListener {
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
     */
    public void onDiscovery(
        int type,
        long topVer,
        ClusterNode node,
        Collection<ClusterNode> topSnapshot,
        @Nullable Map<Long, Collection<ClusterNode>> topHist,
        @Nullable DiscoverySpiCustomMessage data);
}