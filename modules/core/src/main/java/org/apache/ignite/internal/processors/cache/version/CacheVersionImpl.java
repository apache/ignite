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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 *
 */
public class CacheVersionImpl implements CacheVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final long ORDER_MASK = 0x00_FF_FF_FF_FF_FF_FF_FFL;

    /** */
    // public static final int TOP_VER_MASK = 0x00_FF_FF_FF;

    /** Topology version and first minor topology version byte. */
    @GridToStringInclude
    protected int topVer;

    /** Node order (used as global order) and DR ID. */
    @GridToStringInclude
    protected int nodeOrderDrId;

    /** Globally adjusted time. */
    @GridToStringInclude
    protected long globalTime;

    /** Order and second minor topology version byte. */
    @GridToStringInclude
    protected long order;

    /**
     *
     */
    public CacheVersionImpl() {
        // No-op.
    }

    /**
     * @param topVer Topology version plus number of seconds from the start time of the first grid node.
     * @param minorTopVer Minor topology version.
     * @param globalTime Globally adjusted time.
     * @param order Version order.
     * @param nodeOrder Node order.
     * @param dataCenterId Replication data center ID.
     */
    public CacheVersionImpl(int topVer, int minorTopVer, long globalTime, long order, int nodeOrder, int dataCenterId) {
        assert topVer >= 0 : topVer;
        assert minorTopVer >= 0 : minorTopVer;
        assert order >= 0 : order;
        assert nodeOrder >= 0 : nodeOrder;
        assert dataCenterId < 32 && dataCenterId >= 0 : dataCenterId;

        if (nodeOrder > GridCacheVersion.NODE_ORDER_MASK)
            throw new IllegalArgumentException("Node order overflow: " + nodeOrder);

        this.globalTime = globalTime;
        this.topVer = topVer;//topVer | (minorTopVer << 24);
        this.order = order | (((long)minorTopVer) << 56);//((minorTopVer & 0xFF00L) << 48) | order;

        nodeOrderDrId = nodeOrder | (dataCenterId << GridCacheVersion.DR_ID_SHIFT);
    }

    /**
     * @param topVerRaw Raw topology version field.
     * @param globalTime Raw time field.
     * @param orderRaw Raw order field.
     * @param nodeOrder Node order.
     * @param dataCenterId Replication data center ID.
     */
    public CacheVersionImpl(int topVerRaw, long globalTime, long orderRaw, int nodeOrder, int dataCenterId) {
        this.globalTime = globalTime;
        topVer = topVerRaw;
        order = orderRaw;
        nodeOrderDrId = nodeOrder | (dataCenterId << GridCacheVersion.DR_ID_SHIFT);
    }

    /**
     * @param topVer Raw topology version field.
     * @param nodeOrderDrId Raw nodeOrderDrId field.
     * @param globalTime Raw time field.
     * @param order Raw order field.
     */
    public CacheVersionImpl(int topVer, int nodeOrderDrId, long globalTime, long order) {
        this.topVer = topVer;
        this.nodeOrderDrId = nodeOrderDrId;
        this.globalTime = globalTime;
        this.order = order;
    }

    /** {@inheritDoc} */
    @Override public int minorTopologyVersion() {
//        int low = topVer >>> 24;
//        int high = ((int)(order >>> 48)) & 0xFF00;
//
//        return high | low;
        return (int)(order >>> 56);
    }

    /** {@inheritDoc} */
    @Override public int topologyVersion() {
        return topVer;// & TOP_VER_MASK;
    }

    /** {@inheritDoc} */
    @Override public int nodeOrderRaw() {
        return nodeOrderDrId;
    }

    /** {@inheritDoc} */
    @Override public int topologyVersionRaw() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public long orderRaw() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public long globalTime() {
        return globalTime;
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return order & ORDER_MASK;
    }

    /** {@inheritDoc} */
    @Override public int nodeOrder() {
        return nodeOrderDrId & GridCacheVersion.NODE_ORDER_MASK;
    }

    /** {@inheritDoc} */
    @Override public byte dataCenterId() {
        return (byte)((nodeOrderDrId >> GridCacheVersion.DR_ID_SHIFT) & GridCacheVersion.DR_ID_MASK);
    }

    /** {@inheritDoc} */
    @Override public CacheVersion conflictVersion() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean hasConflictVersion() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid asGridUuid() {
        return new IgniteUuid(new UUID(((long)topVer << 32) | nodeOrderDrId, globalTime), order);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(CacheVersion other) {
        assert other instanceof CacheVersionImpl : other;

        int res = Integer.compare(topologyVersion(), other.topologyVersion());

        if (res != 0)
            return res;

        res = Integer.compare(minorTopologyVersion(), other.minorTopologyVersion());

        if (res != 0)
            return res;

        res = Long.compare(order(), other.order());

        if (res != 0)
            return res;

        return Integer.compare(nodeOrder(), other.nodeOrder());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof CacheVersionImpl))
            return false;

        CacheVersionImpl that = (CacheVersionImpl)o;

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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(topVer);
        out.writeLong(globalTime);
        out.writeLong(order);
        out.writeInt(nodeOrderDrId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topVer = in.readInt();
        globalTime = in.readLong();
        order = in.readLong();
        nodeOrderDrId = in.readInt();
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

        return reader.afterMessageRead(CacheVersionImpl.class);
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
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    public String toString() {
        return "CacheVersionImpl [topVer=" + topologyVersion() +
            ", minorTopVer=" + minorTopologyVersion() +
            ", time=" + globalTime() +
            ", order=" + order() +
            ", nodeOrder=" + nodeOrder() + ']';
    }
}
