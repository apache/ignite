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

package org.apache.ignite.internal.processors.cache.mvcc.msg;

import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/** */
public class PartitionCountersNeighborcastRequest extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = -1893577108462486998L;

    /** */
    @GridDirectCollection(PartitionUpdateCountersMessage.class)
    private Collection<PartitionUpdateCountersMessage> updCntrs;

    /** */
    private IgniteUuid futId;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** */
    public PartitionCountersNeighborcastRequest() {
    }

    /** */
    public PartitionCountersNeighborcastRequest(
        Collection<PartitionUpdateCountersMessage> updCntrs,
        IgniteUuid futId,
        @NotNull AffinityTopologyVersion topVer
    ) {
        this.updCntrs = updCntrs;
        this.futId = futId;
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Partition update counters for remote node.
     */
    public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs;
    }

    /**
     * @return Sending future id.
     */
    public IgniteUuid futId() {
        return futId;
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
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection("updCntrs", updCntrs, MessageCollectionItemType.MSG))
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
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                updCntrs = reader.readCollection("updCntrs", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(PartitionCountersNeighborcastRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 165;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }
}
