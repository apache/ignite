/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.clock;

import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Time server message.
 */
public class GridClockMessage {
    /** Packet size. */
    public static final int PACKET_SIZE = 48;

    /** Originating node ID. */
    private UUID origNodeId;

    /** Target node ID. */
    private UUID targetNodeId;

    /** Originating timestamp. */
    private long origTs;

    /** Remote node reply ts. */
    private long replyTs;

    /**
     * @param origNodeId Originating node ID.
     * @param targetNodeId Target node ID.
     * @param origTs Originating timestamp.
     * @param replyTs Reply timestamp.
     */
    public GridClockMessage(UUID origNodeId, UUID targetNodeId, long origTs, long replyTs) {
        this.origNodeId = origNodeId;
        this.targetNodeId = targetNodeId;
        this.origTs = origTs;
        this.replyTs = replyTs;
    }

    /**
     * @return Originating node ID.
     */
    public UUID originatingNodeId() {
        return origNodeId;
    }

    /**
     * @param origNodeId Originating node ID.
     */
    public void originatingNodeId(UUID origNodeId) {
        this.origNodeId = origNodeId;
    }

    /**
     * @return Target node ID.
     */
    public UUID targetNodeId() {
        return targetNodeId;
    }

    /**
     * @param targetNodeId Target node ID.
     */
    public void targetNodeId(UUID targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    /**
     * @return Originating timestamp.
     */
    public long originatingTimestamp() {
        return origTs;
    }

    /**
     * @param origTs Originating timestamp.
     */
    public void originatingTimestamp(long origTs) {
        this.origTs = origTs;
    }

    /**
     * @return Reply timestamp.
     */
    public long replyTimestamp() {
        return replyTs;
    }

    /**
     * @param replyTs Reply timestamp.
     */
    public void replyTimestamp(long replyTs) {
        this.replyTs = replyTs;
    }

    /**
     * Converts message to bytes to send over network.
     *
     * @return Bytes representing this packet.
     */
    public byte[] toBytes() {
        byte[] buf = new byte[PACKET_SIZE];

        int off = 0;

        off = U.longToBytes(origNodeId.getLeastSignificantBits(), buf, off);
        off = U.longToBytes(origNodeId.getMostSignificantBits(), buf, off);

        off = U.longToBytes(targetNodeId.getLeastSignificantBits(), buf, off);
        off = U.longToBytes(targetNodeId.getMostSignificantBits(), buf, off);

        off = U.longToBytes(origTs, buf, off);

        off = U.longToBytes(replyTs, buf, off);

        assert off == PACKET_SIZE;

        return buf;
    }

    /**
     * Constructs message from bytes.
     *
     * @param buf Bytes.
     * @param off Offset.
     * @param len Packet length.
     * @return Assembled message.
     * @throws IgniteCheckedException If message length is invalid.
     */
    public static GridClockMessage fromBytes(byte[] buf, int off, int len) throws IgniteCheckedException {
        if (len < PACKET_SIZE)
            throw new IgniteCheckedException("Failed to assemble time server packet (message is too short).");

        long lsb = U.bytesToLong(buf, off);
        long msb = U.bytesToLong(buf, off + 8);

        UUID origNodeId = new UUID(msb, lsb);

        lsb = U.bytesToLong(buf, off + 16);
        msb = U.bytesToLong(buf, off + 24);

        UUID targetNodeId = new UUID(msb, lsb);

        long origTs = U.bytesToLong(buf, off + 32);
        long replyTs = U.bytesToLong(buf, off + 40);

        return new GridClockMessage(origNodeId, targetNodeId, origTs, replyTs);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClockMessage.class, this);
    }
}
