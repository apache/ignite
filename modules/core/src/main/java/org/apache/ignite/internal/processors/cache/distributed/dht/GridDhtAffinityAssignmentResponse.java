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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.NotNull;

/**
 * Affinity assignment response.
 */
public class GridDhtAffinityAssignmentResponse extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Affinity assignment. */
    @GridDirectTransient
    @GridToStringInclude
    private List<List<ClusterNode>> affAssignment;

    /** Affinity assignment bytes. */
    private byte[] affAssignmentBytes;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityAssignmentResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment.
     */
    public GridDhtAffinityAssignmentResponse(int cacheId, @NotNull AffinityTopologyVersion topVer,
        List<List<ClusterNode>> affAssignment) {
        this.cacheId = cacheId;
        this.topVer = topVer;
        this.affAssignment = affAssignment;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionExchangeMessage() {
        return true;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> affinityAssignment() {
        return affAssignment;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 29;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /**
     * @param ctx Context.
     */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (affAssignment != null)
            affAssignmentBytes = ctx.marshaller().marshal(affAssignment);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (affAssignmentBytes != null) {
            affAssignment = ctx.marshaller().unmarshal(affAssignmentBytes, ldr);

            // TODO IGNITE-10: setting 'local' for nodes not needed when IGNITE-10 is implemented.
            int assignments = affAssignment.size();

            for (int n = 0; n < assignments; n++) {
                List<ClusterNode> nodes = affAssignment.get(n);

                int size = nodes.size();

                for (int i = 0; i < size; i++) {
                    ClusterNode node = nodes.get(i);

                    if (node instanceof TcpDiscoveryNode)
                        ((TcpDiscoveryNode)node).local(node.id().equals(ctx.localNodeId()));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeByteArray("affAssignmentBytes", affAssignmentBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                affAssignmentBytes = reader.readByteArray("affAssignmentBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAffinityAssignmentResponse.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentResponse.class, this);
    }
}