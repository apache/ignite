/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Information about partitions of all nodes in topology.
 */
public class GridDhtPartitionsFullMessage<K, V> extends GridDhtPartitionsAbstractMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, GridDhtPartitionFullMap> parts = new HashMap<>();

    /** */
    private byte[] partsBytes;

    /** Topology version. */
    private long topVer;

    @GridDirectTransient
    private List<List<ClusterNode>> affAssignment;

    /** */
    private byte[] affAssignmentBytes;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsFullMessage() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     * @param lastVer Last version.
     */
    public GridDhtPartitionsFullMessage(@Nullable GridDhtPartitionExchangeId id, @Nullable GridCacheVersion lastVer,
        long topVer) {
        super(id, lastVer);

        assert parts != null;
        assert id == null || topVer == id.topologyVersion();

        this.topVer = topVer;
    }

    /**
     * @return Local partitions.
     */
    public Map<Integer, GridDhtPartitionFullMap> partitions() {
        return parts;
    }

    /**
     * @param cacheId Cache ID.
     * @param fullMap Full partitions map.
     */
    public void addFullPartitionsMap(int cacheId, GridDhtPartitionFullMap fullMap) {
        parts.put(cacheId, fullMap);
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (parts != null)
            partsBytes = ctx.marshaller().marshal(parts);

        if (affAssignment != null)
            affAssignmentBytes = ctx.marshaller().marshal(affAssignment);
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Affinity assignment for topology version.
     */
    public List<List<ClusterNode>> affinityAssignment() {
        return affAssignment;
    }

    /**
     * @param affAssignment Affinity assignment for topology version.
     */
    public void affinityAssignment(List<List<ClusterNode>> affAssignment) {
        this.affAssignment = affAssignment;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (partsBytes != null)
            parts = ctx.marshaller().unmarshal(partsBytes, ldr);

        if (affAssignmentBytes != null)
            affAssignment = ctx.marshaller().unmarshal(affAssignmentBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtPartitionsFullMessage _clone = new GridDhtPartitionsFullMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtPartitionsFullMessage _clone = (GridDhtPartitionsFullMessage)_msg;

        _clone.parts = parts;
        _clone.partsBytes = partsBytes;
        _clone.topVer = topVer;
        _clone.affAssignment = affAssignment;
        _clone.affAssignmentBytes = affAssignmentBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 5:
                if (!commState.putByteArray(affAssignmentBytes))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putByteArray(partsBytes))
                    return false;

                commState.idx++;

            case 7:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 5:
                byte[] affAssignmentBytes0 = commState.getByteArray();

                if (affAssignmentBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                affAssignmentBytes = affAssignmentBytes0;

                commState.idx++;

            case 6:
                byte[] partsBytes0 = commState.getByteArray();

                if (partsBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                partsBytes = partsBytes0;

                commState.idx++;

            case 7:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 45;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsFullMessage.class, this, "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }
}
