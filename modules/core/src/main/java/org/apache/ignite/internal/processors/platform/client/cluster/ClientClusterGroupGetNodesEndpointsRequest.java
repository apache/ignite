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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Cluster group get nodes endpoints request.
 */
public class ClientClusterGroupGetNodesEndpointsRequest extends ClientRequest {
    /** Indicates unknown topology version. */
    private static final long UNKNOWN_TOP_VER = -1;

    /** Start topology version. -1 for earliest. */
    private final long startTopVer;

    /** End topology version. -1 for latest. */
    private final long endTopVer;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterGroupGetNodesEndpointsRequest(BinaryRawReader reader) {
        super(reader);
        startTopVer = reader.readLong();
        endTopVer = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteClusterEx cluster = ctx.kernalContext().grid().cluster();

        long endTopVer0 = endTopVer == UNKNOWN_TOP_VER ? cluster.topologyVersion() : endTopVer;

        Collection<ClusterNode> topology = cluster.topology(endTopVer0);

        if (startTopVer == UNKNOWN_TOP_VER)
            return new ClientClusterGroupGetNodesEndpointsResponse(requestId(), endTopVer0, topology, null);

        Set<UUID> startNodes = toSet(cluster.topology(startTopVer));
        Set<UUID> endNodes = toSet(topology);

        Collection<UUID> removedNodeIds = new ArrayList<>();

        for (UUID startNode : startNodes) {
            if (!endNodes.contains(startNode)) {
                removedNodeIds.add(startNode);
            }
        }

        Collection<ClusterNode> addedNodes = new ArrayList<>();

        for (UUID endNode : endNodes) {
            if (!startNodes.contains(endNode)) {
                ClusterNode node = cluster.node(endNode);
                addedNodes.add(node);
            }
        }

        return new ClientClusterGroupGetNodesEndpointsResponse(requestId(), endTopVer0, addedNodes, removedNodeIds);
    }

    /**
     * Converts collection to a set of node ids.
     *
     * @param nodes Nodes.
     * @return Set of node ids.
     */
    private Set<UUID> toSet(Collection<ClusterNode> nodes) {
        Set<UUID> res = new HashSet<>(nodes.size());

        for (ClusterNode node : nodes)
            res.add(node.id());

        return res;
    }
}
