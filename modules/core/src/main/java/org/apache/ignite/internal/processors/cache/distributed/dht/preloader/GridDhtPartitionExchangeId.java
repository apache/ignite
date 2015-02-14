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

import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Exchange ID.
 */
public class GridDhtPartitionExchangeId extends MessageAdapter implements Comparable<GridDhtPartitionExchangeId>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    @GridToStringExclude
    private UUID nodeId;

    /** Event. */
    @GridToStringExclude
    private int evt;

    /** Topology version. */
    private long topVer;

    /**
     * @param nodeId Node ID.
     * @param evt Event.
     * @param topVer Topology version.
     */
    public GridDhtPartitionExchangeId(UUID nodeId, int evt, long topVer) {
        assert nodeId != null;
        assert evt == EVT_NODE_LEFT || evt == EVT_NODE_FAILED || evt == EVT_NODE_JOINED;
        assert topVer > 0;

        this.nodeId = nodeId;
        this.evt = evt;
        this.topVer = topVer;
    }

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtPartitionExchangeId() {
        // No-op.
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Event.
     */
    public int event() {
        return evt;
    }

    /**
     * @return Order.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return {@code True} if exchange is for new node joining.
     */
    public boolean isJoined() {
        return evt == EVT_NODE_JOINED;
    }

    /**
     * @return {@code True} if exchange is for node leaving.
     */
    public boolean isLeft() {
        return evt == EVT_NODE_LEFT || evt == EVT_NODE_FAILED;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        out.writeLong(topVer);
        out.writeInt(evt);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        topVer = in.readLong();
        evt = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionExchangeId o) {
        if (o == this)
            return 0;

        return topVer < o.topVer ? -1 : topVer == o.topVer ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId.hashCode();

        res = 31 * res + evt;
        res = 31 * res + (int)(topVer ^ (topVer >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        GridDhtPartitionExchangeId id = (GridDhtPartitionExchangeId)o;

        return evt == id.evt && topVer == id.topVer && nodeId.equals(id.nodeId);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriteState state) {
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 0:
                if (!writer.writeInt("evt", evt))
                    return false;

                state.increment();

            case 1:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                state.increment();

            case 2:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (readState) {
            case 0:
                evt = reader.readInt("evt");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 87;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridDhtPartitionExchangeId _clone = (GridDhtPartitionExchangeId)_msg;

        _clone.nodeId = nodeId;
        _clone.evt = evt;
        _clone.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionExchangeId.class, this,
            "nodeId", U.id8(nodeId),
            "evt", U.gridEventName(evt));
    }
}
