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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cluster group get nodes endpoints response.
 */
public class ClientClusterGroupGetNodesEndpointsResponse extends ClientResponse {
    /** Indicates unknown topology version. */
    private static final long UNKNOWN_TOP_VER = -1;

    /** Start topology version. -1 for earliest. */
    private final long startTopVer;

    /** End topology version. -1 for latest. */
    private final long endTopVer;

    /**
     * Constructor.
     *
     * @param reqId Request identifier.
     * @param startTopVer Start topology version.
     * @param endTopVer End topology version.
     */
    public ClientClusterGroupGetNodesEndpointsResponse(long reqId,
                                                       long startTopVer,
                                                       long endTopVer) {
        super(reqId);

        this.startTopVer = startTopVer;
        this.endTopVer = endTopVer;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        IgniteClusterEx cluster = ctx.kernalContext().grid().cluster();

        long endTopVer0 = endTopVer == UNKNOWN_TOP_VER ? cluster.topologyVersion() : endTopVer;

        Collection<ClusterNode> topology = cluster.topology(endTopVer0);

        writer.writeLong(endTopVer0);

        if (startTopVer == UNKNOWN_TOP_VER) {
            int pos = writer.reserveInt();
            int size = 0;

            for (ClusterNode node : topology) {
                if (writeNode(writer, node))
                    size++;
            }

            writer.writeInt(pos, size);
            writer.writeInt(0);

            return;
        }

        Map<UUID, ClusterNode> startNodes = toMap(cluster.topology(startTopVer));
        Map<UUID, ClusterNode> endNodes = toMap(topology);

        int pos = writer.reserveInt();
        int cnt = 0;

        for (Map.Entry<UUID, ClusterNode> endNode : endNodes.entrySet()) {
            if (!startNodes.containsKey(endNode.getKey())) {
                if (writeNode(writer, endNode.getValue()))
                    cnt++;
            }
        }

        writer.writeInt(pos, cnt);

        pos = writer.reserveInt();
        cnt = 0;

        for (Map.Entry<UUID, ClusterNode> startNode : startNodes.entrySet()) {
            if (!endNodes.containsKey(startNode.getKey()) && !startNode.getValue().isClient()) {
                writeUuid(writer, startNode.getKey());
                cnt++;
            }
        }

        writer.writeInt(pos, cnt);
    }

    /**
     * Writes node info.
     *
     * @param writer Writer.
     * @param node Node.
     */
    private static boolean writeNode(BinaryRawWriterEx writer, ClusterNode node) {
        if (node.isClient())
            return false;

        Object port = node.attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT);

        if (!(port instanceof Integer))
            return false; // No client connector.

        writeUuid(writer, node.id());
        writer.writeInt((int) port);

        Collection<String> addrs = node.addresses();
        Collection<String> hosts = node.hostNames();

        writer.writeInt(addrs.size() + hosts.size());

        for (String addr : addrs)
            writer.writeString(addr);

        for (String host : hosts)
            writer.writeString(host);

        return true;
    }

    /**
     * Writes UUID.
     *
     * @param writer Writer.
     * @param id id.
     */
    private static void writeUuid(BinaryRawWriterEx writer, UUID id) {
        writer.writeLong(id.getMostSignificantBits());
        writer.writeLong(id.getLeastSignificantBits());
    }

    /**
     * Converts collection to a set of node ids.
     *
     * @param nodes Nodes.
     * @return Set of node ids.
     */
    private static Map<UUID, ClusterNode> toMap(Collection<ClusterNode> nodes) {
        Map<UUID, ClusterNode> res = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes)
            res.put(node.id(), node);

        return res;
    }
}
