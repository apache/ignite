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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * The demand request for partitions according to given assignments.
 */
public class GridPartitionsCopyDemandMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long rebId;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = GridLongList.class)
    private Map<Integer, GridIntList> assigns;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridPartitionsCopyDemandMessage() {
        // No-op.
    }

    /**
     * @param rebId Rebalance id for this node.
     * @param topVer Topology version.
     */
    public GridPartitionsCopyDemandMessage(
        long rebId,
        AffinityTopologyVersion topVer,
        Map<Integer, GridIntList> assigns0
    ) {
        assert assigns0 != null && !assigns0.isEmpty();

        this.rebId = rebId;
        this.topVer = topVer;

        assigns = U.newHashMap(assigns0.size());

        for (Map.Entry<Integer, GridIntList> e : assigns0.entrySet())
            assigns.put(e.getKey(), e.getValue().copy());
    }

    /**
     * @param rebId Rebalance identifier to set.
     */
    public void rebalanceId(long rebId) {
        this.rebId = rebId;
    }

    /**
     * @return Unique rebalance session id.
     */
    public long rebalanceId() {
        return rebId;
    }

    /**
     * @return Topology version for which demand message is sent.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return The demanded cache group partions per each cache group.
     */
    public Map<Integer, GridIntList> assignments() {
        return assigns;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeLong("rebId", rebId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap("assigns", assigns, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
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

        switch (reader.state()) {
            case 0:
                rebId = reader.readLong("rebId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                assigns = reader.readMap("assigns", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridPartitionsCopyDemandMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 172;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionsCopyDemandMessage.class, this);
    }
}
