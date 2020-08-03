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

import java.util.UUID;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cluster group get nodes identifiers response.
 */
public class ClientClusterGroupGetNodeIdsResponse extends ClientResponse {
    /** Topology version. */
    private final long topVer;

    /** Node ids. */
    private final UUID[] nodeIds;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param topVer Topology version.
     * @param nodeIds Node ids.
     */
    public ClientClusterGroupGetNodeIdsResponse(long reqId, long topVer, UUID[] nodeIds) {
        super(reqId);
        this.topVer = topVer;
        this.nodeIds = nodeIds;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeBoolean(true);
        writer.writeLong(topVer);

        // At this moment topology version might have advanced, and due to this race
        // we return outdated top ver to the callee. But this race is benign, the only
        // possible side effect is that the user will re-request nodes and we will return
        // the same set of nodes but with more recent topology version.
        writer.writeInt(nodeIds.length);
        for (UUID node: nodeIds) {
            writer.writeLong(node.getMostSignificantBits());
            writer.writeLong(node.getLeastSignificantBits());
        }
    }
}
