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
import java.util.UUID;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Cluster get data center nodes request .
 */
public class ClientClusterGetDataCenterNodesRequest extends ClientRequest {
    /** */
    private final String dcId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterGetDataCenterNodesRequest(BinaryRawReader reader) {
        super(reader);

        dcId = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        if (dcId == null)
            throw new IllegalArgumentException("Data center ID cannot be null");

        Collection<ClusterNode> srvNodes = ctx.kernalContext().discovery().aliveServerNodes();

        Collection<UUID> nodeIds = F.viewReadOnly(srvNodes, ClusterNode::id, n -> dcId.equals(n.dataCenterId()));

        return new ClientClusterGetDataCenterNodesResponse(requestId(), nodeIds);
    }
}
