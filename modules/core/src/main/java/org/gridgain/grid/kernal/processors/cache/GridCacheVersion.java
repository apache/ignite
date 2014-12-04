/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Grid unique version.
 */
public class GridCacheVersion implements Comparable<GridCacheVersion>, Externalizable, GridOptimizedMarshallable {
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
    @Override public String toString() {
        return S.toString(GridCacheVersion.class, this);
    }
}
