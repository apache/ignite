/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.platform.client.ClientBooleanResponse;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.Collection;
import java.util.UUID;

/**
 * Cluster group get node identifiers request.
 */
public class ClientClusterGroupGetNodeIdsRequest extends ClientRequest {
    /** Topology version. */
    private final long topVer;

    /** Client cluster group projection. */
    private ClientClusterGroupProjection prj;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterGroupGetNodeIdsRequest(BinaryRawReader reader) {
        super(reader);
        topVer = reader.readLong();
        prj = ClientClusterGroupProjection.read(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteClusterEx cluster = ctx.kernalContext().grid().cluster();
        long curTopVer = cluster.topologyVersion();

        // No topology changes, return false.
        if (curTopVer <= topVer)
            return new ClientBooleanResponse(requestId(), false);

        ClusterGroup clusterGrp = prj.apply(cluster);
        UUID[] nodeIds = getNodeIds(clusterGrp);
        return new ClientClusterGroupGetNodeIdsResponse(requestId(), curTopVer, nodeIds);
    }

    /** Tansforms nodes collection to node ids array. */
    private UUID[] getNodeIds(ClusterGroup clusterGrp){
        Collection<ClusterNode> nodes = clusterGrp.nodes();
        UUID[] nodeIds = new UUID[nodes.size()];
        int i = 0;
        for(ClusterNode node : nodes){
            nodeIds[i++] = node.id();
        }
        return nodeIds;
    }
}
