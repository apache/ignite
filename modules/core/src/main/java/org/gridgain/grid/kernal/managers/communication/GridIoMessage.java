/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.communication;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;

/**
 * Wrapper for all grid messages.
 */
public class GridIoMessage extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Policy. */
    private GridIoPolicy plc;

    /** Message topic. */
    @GridToStringInclude
    @GridDirectTransient
    private Object topic;

    /** Topic bytes. */
    private byte[] topicBytes;

    /** Topic ordinal. */
    private int topicOrd = -1;

    /** Message order. */
    private long msgId = -1;

    /** Message timeout. */
    private long timeout;

    /** Whether message can be skipped on timeout. */
    private boolean skipOnTimeout;

    /** Message. */
    private GridTcpCommunicationMessageAdapter msg;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    public GridIoMessage() {
        // No-op.
    }

    /**
     * @param plc Policy.
     * @param topic Communication topic.
     * @param topicOrd Topic ordinal value.
     * @param msg Message.
     * @param msgId Message ID.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     */
    public GridIoMessage(GridIoPolicy plc, Object topic, int topicOrd, GridTcpCommunicationMessageAdapter msg,
        long msgId, long timeout, boolean skipOnTimeout) {
        assert plc != null;
        assert topic != null;
        assert topicOrd <= Byte.MAX_VALUE;
        assert msg != null;

        this.plc = plc;
        this.msg = msg;
        this.topic = topic;
        this.topicOrd = topicOrd;
        this.msgId = msgId;
        this.timeout = timeout;
        this.skipOnTimeout = skipOnTimeout;
    }

    /**
     * @return Policy.
     */
    GridIoPolicy policy() {
        return plc;
    }

    /**
     * @return Topic.
     */
    Object topic() {
        return topic;
    }

    /**
     * @param topic Topic.
     */
    void topic(Object topic) {
        this.topic = topic;
    }

    /**
     * @return Topic bytes.
     */
    byte[] topicBytes() {
        return topicBytes;
    }

    /**
     * @param topicBytes Topic bytes.
     */
    void topicBytes(byte[] topicBytes) {
        this.topicBytes = topicBytes;
    }

    /**
     * @return Topic ordinal.
     */
    int topicOrdinal() {
        return topicOrd;
    }

    /**
     * @return Message.
     */
    public Object message() {
        return msg;
    }

    /**
     * @return Message ID.
     */
    long messageId() {
        return msgId;
    }

    /**
     * @return Message timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @return Whether message can be skipped on timeout.
     */
    public boolean skipOnTimeout() {
        return skipOnTimeout;
    }

    /**
     * @return {@code True} if message is ordered, {@code false} otherwise.
     */
    boolean isOrdered() {
        return msgId > 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof GridIoMessage))
            return false;

        GridIoMessage other = (GridIoMessage)obj;

        return topic.equals(other.topic) && msgId == other.msgId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = topic.hashCode();

        res = 31 * res + (int)(msgId ^ (msgId >>> 32));
        res = 31 * res + topic.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridIoMessage _clone = new GridIoMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("RedundantCast")
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridIoMessage _clone = (GridIoMessage)_msg;

        _clone.plc = plc;
        _clone.topic = topic;
        _clone.topicBytes = topicBytes;
        _clone.topicOrd = topicOrd;
        _clone.msgId = msgId;
        _clone.timeout = timeout;
        _clone.skipOnTimeout = skipOnTimeout;
        _clone.msg = msg != null ? (GridTcpCommunicationMessageAdapter)msg.clone() : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putMessage(null, msg))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putLong(null, msgId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putEnum(null, plc))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putBoolean(null, skipOnTimeout))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLong(null, timeout))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putByteArray(null, topicBytes))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putInt(null, topicOrd))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                Object msg0 = commState.getMessage(null);

                if (msg0 == MSG_NOT_READ)
                    return false;

                msg = (GridTcpCommunicationMessageAdapter)msg0;

                commState.idx++;

            case 1:
                if (buf.remaining() < 8)
                    return false;

                msgId = commState.getLong(null);

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                byte plc0 = commState.getByte(null);

                plc = GridIoPolicy.fromOrdinal(plc0);

                commState.idx++;

            case 3:
                if (buf.remaining() < 1)
                    return false;

                skipOnTimeout = commState.getBoolean(null);

                commState.idx++;

            case 4:
                if (buf.remaining() < 8)
                    return false;

                timeout = commState.getLong(null);

                commState.idx++;

            case 5:
                byte[] topicBytes0 = commState.getByteArray(null);

                if (topicBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                topicBytes = topicBytes0;

                commState.idx++;

            case 6:
                if (buf.remaining() < 4)
                    return false;

                topicOrd = commState.getInt(null);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIoMessage.class, this);
    }
}
