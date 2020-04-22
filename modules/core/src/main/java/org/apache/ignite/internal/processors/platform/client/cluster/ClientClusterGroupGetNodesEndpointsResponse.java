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

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.Collection;
import java.util.UUID;

/**
 * Cluster group get nodes endpoints response.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class ClientClusterGroupGetNodesEndpointsResponse extends ClientResponse {
    /** */
    private final Collection<IgniteBiTuple<UUID, Collection<String>>> addedNodes;

    /** */
    private final Collection<UUID> removedNodeIds;

    /**
     * Constructor.
     *
     * @param reqId Request identifier.
     * @param topVer Topology version.
     * @param addedNodes Added nodes.
     * @param removedNodeIds Removed node ids.
     */
    public ClientClusterGroupGetNodesEndpointsResponse(long reqId,
                                                       long topVer,
                                                       Collection<IgniteBiTuple<UUID, Collection<String>>> addedNodes,
                                                       Collection<UUID> removedNodeIds) {
        super(reqId);

        assert addedNodes != null;
        assert removedNodeIds != null;

        this.addedNodes = addedNodes;
        this.removedNodeIds = removedNodeIds;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeInt(addedNodes.size());

        for (IgniteBiTuple<UUID, Collection<String>> node : addedNodes) {
            UUID id = node.get1();
            writer.writeLong(id.getMostSignificantBits());
            writer.writeLong(id.getLeastSignificantBits());

            Collection<String> endpoints = node.get2();
            writer.writeInt(endpoints.size());

            for (String e : endpoints) {
                writer.writeString(e);
            }
        }

        writer.writeInt(removedNodeIds.size());

        for (UUID id : removedNodeIds) {
            writer.writeLong(id.getMostSignificantBits());
            writer.writeLong(id.getLeastSignificantBits());
        }
    }
}
