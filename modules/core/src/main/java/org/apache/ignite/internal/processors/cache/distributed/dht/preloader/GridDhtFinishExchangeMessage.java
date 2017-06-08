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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Message is send to finish exchange.
 */
public class GridDhtFinishExchangeMessage extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private GridDhtPartitionExchangeId exchId;

    /** Serialized exchange id. */
    private byte[] serializedExchId;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, Map<Integer, List<UUID>>> assignmentChange;

    /** Serialized exchange id. */
    private byte[] serializedAssignmentChange;

    /** */
    @GridToStringInclude
    private GridDhtPartitionsFullMessage partFullMsg;

    /**
     * @return Exchange id.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @param exchId New exchange id.
     */
    public void exchangeId(GridDhtPartitionExchangeId exchId) {
        this.exchId = exchId;
    }

    /**
     * @return Assignment change.
     */
    public Map<Integer, Map<Integer, List<UUID>>> assignmentChange() {
        return assignmentChange;
    }

    /**
     * @param assignmentChange New assignment change.
     */
    public void assignmentChange(Map<Integer, Map<Integer, List<UUID>>> assignmentChange) {
        this.assignmentChange = assignmentChange;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        serializedExchId = U.marshal(ctx, exchId);
        serializedAssignmentChange = U.marshal(ctx, assignmentChange);
    }

    /** {@inheritDoc} */
    public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        exchId = U.unmarshal(ctx, serializedExchId, ldr);
        assignmentChange = U.unmarshal(ctx, serializedAssignmentChange, ldr);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -53;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /**
     * @return Partition full message.
     */
    public GridDhtPartitionsFullMessage partitionFullMessage() {
        return partFullMsg;
    }

    /**
     * @param partFullMsg New partition full message.
     */
    public void partitionFullMessage(GridDhtPartitionsFullMessage partFullMsg) {
        this.partFullMsg = partFullMsg;
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
                if (!writer.writeMessage("partFullMsg", partFullMsg))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("serializedAssignmentChange", serializedAssignmentChange))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("serializedExchId", serializedExchId))
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
                partFullMsg = reader.readMessage("partFullMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                serializedAssignmentChange = reader.readByteArray("serializedAssignmentChange");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                serializedExchId = reader.readByteArray("serializedExchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtFinishExchangeMessage.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtFinishExchangeMessage.class, this);
    }
}
