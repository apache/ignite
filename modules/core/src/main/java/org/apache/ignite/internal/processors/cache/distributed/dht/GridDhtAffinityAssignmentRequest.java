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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Affinity assignment request.
 */
public class GridDhtAffinityAssignmentRequest extends GridCacheGroupIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int SND_PART_STATE_MASK = 0x01;

    /** */
    private byte flags;

    /** */
    private long futId;

    /** Topology version being queried. */
    private AffinityTopologyVersion topVer;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityAssignmentRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @param sndPartMap {@code True} if need send in response cache partitions state.
     */
    public GridDhtAffinityAssignmentRequest(
        long futId,
        int grpId,
        AffinityTopologyVersion topVer,
        boolean sndPartMap) {
        assert topVer != null;

        this.futId = futId;
        this.grpId = grpId;
        this.topVer = topVer;

        if (sndPartMap)
            flags |= SND_PART_STATE_MASK;
    }

    /**
     * @return {@code True} if need send in response cache partitions state.
     */
    public boolean sendPartitionsState() {
        return (flags & SND_PART_STATE_MASK) != 0;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionExchangeMessage() {
        return true;
    }

    /**
     * @return Requested topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 28;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
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
            case 4:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
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
            case 4:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAffinityAssignmentRequest.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentRequest.class, this, super.toString());
    }
}
