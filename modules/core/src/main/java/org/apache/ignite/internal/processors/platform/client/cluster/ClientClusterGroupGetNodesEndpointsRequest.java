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
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Cluster group get nodes endpoints request.
 */
public class ClientClusterGroupGetNodesEndpointsRequest extends ClientRequest {
    /** Node ids. */
    private final UUID[] nodeIds;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterGroupGetNodesEndpointsRequest(BinaryRawReader reader) {
        super(reader);
        int cnt = reader.readInt();
        nodeIds = new UUID[cnt];
        for (int i = 0; i < cnt; i++) {
            nodeIds[i] = new UUID(reader.readLong(), reader.readLong());
        }
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteClusterEx cluster = ctx.kernalContext().grid().cluster();

        ClusterGroup clusterGrp = nodeIds.length > 0 ? cluster.forNodeIds(Arrays.asList(nodeIds)) : cluster;

        // TODO: Cache results in ctx by node id. This is going to be a frequent request.
        String[] endpoints = ctx.kernalContext()
                .grid()
                .compute(clusterGrp)
                .broadcast(new ClientClusterGroupGetNodeEndpointsJob())
                .stream()
                .flatMap(Collection::stream)
                .toArray(String[]::new);

        return new ClientClusterGroupGetNodesEndpointsResponse(requestId(), endpoints);
    }
}
