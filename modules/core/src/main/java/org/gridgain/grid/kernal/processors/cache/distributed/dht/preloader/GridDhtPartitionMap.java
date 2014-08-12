/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Partition map.
 */
public class GridDhtPartitionMap extends HashMap<Integer, GridDhtPartitionState>
    implements Comparable<GridDhtPartitionMap>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nodeId;

    /** Update sequence number. */
    private long updateSeq;

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     */
    public GridDhtPartitionMap(UUID nodeId, long updateSeq) {
        assert nodeId != null;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.updateSeq = updateSeq;
    }

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     * @param m Map to copy.
     * @param onlyActive If {@code true}, then only active states will be included.
     */
    public GridDhtPartitionMap(UUID nodeId, long updateSeq, Map<Integer, GridDhtPartitionState> m, boolean onlyActive) {
        assert nodeId != null;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.updateSeq = updateSeq;

        for (Map.Entry<Integer, GridDhtPartitionState> e : m.entrySet()) {
            GridDhtPartitionState state = e.getValue();

            if (!onlyActive || state.active())
                put(e.getKey(), state);
        }
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionMap() {
        // No-op.
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Update sequence.
     */
    public long updateSequence() {
        return updateSeq;
    }

    /**
     * @param updateSeq New update sequence value.
     * @return Old update sequence value.
     */
    public long updateSequence(long updateSeq) {
        long old = this.updateSeq;

        assert updateSeq >= old : "Invalid update sequence [cur=" + old + ", new=" + updateSeq + ']';

        this.updateSeq = updateSeq;

        return old;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionMap o) {
        assert nodeId.equals(o.nodeId);

        return updateSeq < o.updateSeq ? -1 : updateSeq == o.updateSeq ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);

        out.writeLong(updateSeq);

        GridBitByteArrayOutputStream bitOut = new GridBitByteArrayOutputStream();

        for (Map.Entry<Integer, GridDhtPartitionState> entry : entrySet()) {
            int part = entry.getKey();
            GridDhtPartitionState state = entry.getValue();

            if (part > 32767)
                throw new IllegalStateException("Partition index overflow: " + part);

            if (part <= 127) {
                bitOut.writeBit(false);
                bitOut.writeBits(part, 7);
            }
            else {
                bitOut.writeBit(true);
                bitOut.writeBits(part, 15);
            }

            if (state.ordinal() > 3)
                throw new IllegalStateException("State ordinal index overflow: " + state.ordinal());

            bitOut.writeBits(state.ordinal(), 2);
        }

        out.writeLong(bitOut.bitsLength());
        U.writeByteArray(out, bitOut.internalArray(), bitOut.bytesLength());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);

        updateSeq = in.readLong();

        long bitsLen = in.readLong();

        byte[] data = U.readByteArray(in);

        GridBitByteArrayInputStream bitIn = new GridBitByteArrayInputStream(data, bitsLen);

        while (bitIn.available() > 0) {
            boolean extended = bitIn.readBit();

            int part = bitIn.readBits(extended ? 15 : 7);

            GridDhtPartitionState state = GridDhtPartitionState.fromOrdinal(bitIn.readBits(2));

            put(part, state);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridDhtPartitionMap other = (GridDhtPartitionMap)o;

        return other.nodeId.equals(nodeId) && other.updateSeq == updateSeq;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * nodeId.hashCode() + (int)(updateSeq ^ (updateSeq >>> 32));
    }

    /**
     * @return Full string representation.
     */
    public String toFullString() {
        return S.toString(GridDhtPartitionMap.class, this, "size", size(), "map", super.toString());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionMap.class, this, "size", size());
    }
}
