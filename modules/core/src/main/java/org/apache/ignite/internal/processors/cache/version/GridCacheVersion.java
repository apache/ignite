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

package org.apache.ignite.internal.processors.cache.version;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Grid unique version.
 */
public class GridCacheVersion implements Message, Comparable<GridCacheVersion>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node order mask. */
    private static final int NODE_ORDER_MASK = 0x07_FF_FF_FF;

    /** DR center ID shift. */
    private static final int DR_ID_SHIFT = 27;

    /** DR center ID mask. */
    private static final int DR_ID_MASK = 0x1F;

    /** Topology version. */
    private int topVer;

    /** Node order (used as global order) and DR ID. */
    private int nodeOrderDrId;

    /** Globally adjusted time. */
    private long globalTime;

    /** Order. */
    private long order;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheVersion() {
        /* No-op. */
    }

    /**
     * @param topVer Topology version plus number of seconds from the start time of the first grid node.
     * @param globalTime Globally adjusted time.
     * @param order Version order.
     * @param nodeOrder Node order.
     * @param dataCenterId Replication data center ID.
     */
    public GridCacheVersion(int topVer, long globalTime, long order, int nodeOrder, int dataCenterId) {
        assert topVer >= 0 : topVer;
        assert order >= 0 : order;
        assert nodeOrder >= 0 : nodeOrder;
        assert dataCenterId < 32 && dataCenterId >= 0 : dataCenterId;

        if (nodeOrder > NODE_ORDER_MASK)
            throw new IllegalArgumentException("Node order overflow: " + nodeOrder);

        this.topVer = topVer;
        this.globalTime = globalTime;
        this.order = order;

        nodeOrderDrId = nodeOrder | (dataCenterId << DR_ID_SHIFT);
    }


    /**
     * @param topVer Topology version plus number of seconds from the start time of the first grid node.
     * @param nodeOrderDrId Node order and DR ID.
     * @param globalTime Globally adjusted time.
     * @param order Version order.
     */
    public GridCacheVersion(int topVer, int nodeOrderDrId, long globalTime, long order) {
        this.topVer = topVer;
        this.nodeOrderDrId = nodeOrderDrId;
        this.globalTime = globalTime;
        this.order = order;
    }

    /**
     * @return Topology version plus number of seconds from the start time of the first grid node..
     */
    public int topologyVersion() {
        return topVer;
    }

    /**
     * Gets combined node order and DR ID.
     *
     * @return Combined integer for node order and DR ID.
     */
    public int nodeOrderAndDrIdRaw() {
        return nodeOrderDrId;
    }

    /**
     * @return Adjusted time.
     */
    public long globalTime() {
        return globalTime;
    }

    /**
     * @return Version order.
     */
    public long order() {
        return order;
    }

    /**
     * @return Node order on which this version was assigned.
     */
    public int nodeOrder() {
        return nodeOrderDrId & NODE_ORDER_MASK;
    }

    /**
     * @return DR mask.
     */
    public byte dataCenterId() {
        return (byte)((nodeOrderDrId >> DR_ID_SHIFT) & DR_ID_MASK);
    }

    /**
     * @return Conflict version.
     */
    public GridCacheVersion conflictVersion() {
        return this; // Use current version.
    }

    /**
     * @param ver Version.
     * @return {@code True} if this version is greater.
     */
    public boolean isGreater(GridCacheVersion ver) {
        return compareTo(ver) > 0;
    }

    /**
     * @param ver Version.
     * @return {@code True} if this version is greater or equal.
     */
    public boolean isGreaterEqual(GridCacheVersion ver) {
        return compareTo(ver) >= 0;
    }

    /**
     * @param ver Version.
     * @return {@code True} if this version is less.
     */
    public boolean isLess(GridCacheVersion ver) {
        return compareTo(ver) < 0;
    }

    /**
     * @param ver Version.
     * @return {@code True} if this version is less or equal.
     */
    public boolean isLessEqual(GridCacheVersion ver) {
        return compareTo(ver) <= 0;
    }

    /**
     * @return Version represented as {@code GridUuid}
     */
    public IgniteUuid asGridUuid() {
        return new IgniteUuid(new UUID(((long)topVer << 32) | nodeOrderDrId, globalTime), order);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(topVer);
        out.writeLong(globalTime);
        out.writeLong(order);
        out.writeInt(nodeOrderDrId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        topVer = in.readInt();
        globalTime = in.readLong();
        order = in.readLong();
        nodeOrderDrId = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridCacheVersion))
            return false;

        GridCacheVersion that = (GridCacheVersion)o;

        return topVer == that.topVer && order == that.order && nodeOrder() == that.nodeOrder();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = topVer;

        res = 31 * res + nodeOrder();

        res = 31 * res + (int)(order ^ (order >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override public int compareTo(GridCacheVersion other) {
        int res = Integer.compare(topologyVersion(), other.topologyVersion());

        if (res != 0)
            return res;

        res = Long.compare(order, other.order);

        if (res != 0)
            return res;

        return Integer.compare(nodeOrder(), other.nodeOrder());
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
                if (!writer.writeLong("globalTime", globalTime))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("nodeOrderDrId", nodeOrderDrId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("order", order))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeInt("topVer", topVer))
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
                globalTime = reader.readLong("globalTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                nodeOrderDrId = reader.readInt("nodeOrderDrId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                order = reader.readLong("order");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                topVer = reader.readInt("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheVersion.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 86;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridCacheVersion [topVer=" + topologyVersion() +
            ", time=" + globalTime() +
            ", order=" + order() +
            ", nodeOrder=" + nodeOrder() + ']';
    }
}
