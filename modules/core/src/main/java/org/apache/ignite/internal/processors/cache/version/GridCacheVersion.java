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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Grid unique version.
 */
public class GridCacheVersion extends MessageAdapter implements Comparable<GridCacheVersion>, Externalizable,
    OptimizedMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
    private static Object GG_CLASS_ID;

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
        assert topVer >= 0;
        assert order >= 0;
        assert nodeOrder >= 0;
        assert dataCenterId < 32 && dataCenterId >= 0;

        if (nodeOrder > NODE_ORDER_MASK)
            throw new IllegalArgumentException("Node order overflow: " + nodeOrder);

        this.topVer = topVer;
        this.globalTime = globalTime;
        this.order = order;

        nodeOrderDrId = nodeOrder | (dataCenterId << DR_ID_SHIFT);
    }


    /**
     * @param topVer Topology version.
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
     * @return DR version.
     */
    @Nullable public GridCacheVersion drVersion() {
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

    /** {@inheritDoc} */
    @Override public Object ggClassId() {
        return GG_CLASS_ID;
    }

    /**
     * @return Version represented as {@code GridUuid}
     */
    public IgniteUuid asGridUuid() {
        return new IgniteUuid(new UUID(((long)topVer << 32) | nodeOrderDrId, globalTime), order);
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
        if (topologyVersion() == other.topologyVersion()) {
            if (order == other.order)
                return nodeOrder() == other.nodeOrder() ? 0 : nodeOrder() < other.nodeOrder() ? -1 : 1;
            else
                return order < other.order ? -1 : 1;
        }
        else
            return topologyVersion() < other.topologyVersion() ? -1 : 1;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        MessageWriteState state = MessageWriteState.get();
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 0:
                if (!writer.writeLong("globalTime", globalTime))
                    return false;

                state.increment();

            case 1:
                if (!writer.writeInt("nodeOrderDrId", nodeOrderDrId))
                    return false;

                state.increment();

            case 2:
                if (!writer.writeLong("order", order))
                    return false;

                state.increment();

            case 3:
                if (!writer.writeInt("topVer", topVer))
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
                globalTime = reader.readLong("globalTime");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                nodeOrderDrId = reader.readInt("nodeOrderDrId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                order = reader.readLong("order");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 3:
                topVer = reader.readInt("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 86;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridCacheVersion _clone = (GridCacheVersion)_msg;

        _clone.topVer = topVer;
        _clone.nodeOrderDrId = nodeOrderDrId;
        _clone.globalTime = globalTime;
        _clone.order = order;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheVersion.class, this);
    }
}
