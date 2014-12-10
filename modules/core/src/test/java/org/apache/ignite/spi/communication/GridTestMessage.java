/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.communication;

import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;

import java.nio.*;
import java.util.*;

/**
 * Test message for communication SPI tests.
 */
public class GridTestMessage extends GridTcpCommunicationMessageAdapter {
    /** */
    public static final byte DIRECT_TYPE = (byte)200;

    /** */
    private UUID srcNodeId;

    /** */
    private long msgId;

    /** */
    private long resId;

    /** Network payload */
    private byte[] payload;

    /** */
    public GridTestMessage() {
        // No-op.
    }

    /**
     * @param srcNodeId Node that originated message.
     * @param msgId Message sequence id.
     * @param resId Response id.
     */
    public GridTestMessage(UUID srcNodeId, long msgId, long resId) {
        this.srcNodeId = srcNodeId;
        this.msgId = msgId;
        this.resId = resId;
    }

    /**
     * @return Id of message originator.
     */
    public UUID getSourceNodeId() {
        return srcNodeId;
    }

    /**
     * @return Message sequence id.
     */
    public long getMsgId() {
        return msgId;
    }

    /**
     * @return Response id.
     */
    public long getResponseId() {
        return resId;
    }

    /**
     * @param payload Payload to be set.
     */
    public void payload(byte[] payload) {
        this.payload = payload;
    }

    /**
     * @return Network payload.
     */
    public byte[] payload() {
        return payload;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridTestMessage msg = new GridTestMessage();

        clone0(msg);

        return msg;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridTestMessage _clone = (GridTestMessage)_msg;

        _clone.srcNodeId = srcNodeId;
        _clone.msgId = msgId;
        _clone.resId = resId;
        _clone.payload = payload;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putUuid(srcNodeId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putLong(msgId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putLong(resId))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putByteArray(payload))
                    return false;

                commState.idx++;
        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                srcNodeId = commState.getUuid();

                if (srcNodeId == UUID_NOT_READ)
                    return false;

                commState.idx++;

            case 1:
                if (buf.remaining() < 8)
                    return false;

                msgId = commState.getLong();

                commState.idx++;

            case 2:
                if (buf.remaining() < 8)
                    return false;

                resId = commState.getLong();

                commState.idx++;

            case 3:
                payload = commState.getByteArray();

                if (payload == BYTE_ARR_NOT_READ)
                    return false;

                commState.idx++;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return DIRECT_TYPE;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridTestMessage))
            return false;

        GridTestMessage m = (GridTestMessage)o;

        return F.eq(srcNodeId, m.srcNodeId) && F.eq(msgId, m.msgId) && F.eq(resId, m.resId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = srcNodeId.hashCode();

        res = 31 * res + (int)(msgId ^ (msgId >>> 32));
        res = 31 * res + (int)(resId ^ (resId >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getSimpleName());
        buf.append(" [srcNodeId=").append(srcNodeId);
        buf.append(", msgId=").append(msgId);
        buf.append(", resId=").append(resId);
        buf.append(']');

        return buf.toString();
    }
}
