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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.tostring.*;

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

    /** Message ordered flag. */
    private boolean ordered;

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
     * @param ordered Message ordered flag.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     */
    public GridIoMessage(
        GridIoPolicy plc,
        Object topic,
        int topicOrd,
        GridTcpCommunicationMessageAdapter msg,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout
    ) {
        assert plc != null;
        assert topic != null;
        assert topicOrd <= Byte.MAX_VALUE;
        assert msg != null;

        this.plc = plc;
        this.msg = msg;
        this.topic = topic;
        this.topicOrd = topicOrd;
        this.ordered = ordered;
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
        return ordered;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        throw new AssertionError();
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
        _clone.ordered = ordered;
        _clone.timeout = timeout;
        _clone.skipOnTimeout = skipOnTimeout;
        _clone.msg = msg != null ? (GridTcpCommunicationMessageAdapter)msg.clone() : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putMessage(msg))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putBoolean(ordered))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putEnum(plc))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putBoolean(skipOnTimeout))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLong(timeout))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putByteArray(topicBytes))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putInt(topicOrd))
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
                Object msg0 = commState.getMessage();

                if (msg0 == MSG_NOT_READ)
                    return false;

                msg = (GridTcpCommunicationMessageAdapter)msg0;

                commState.idx++;

            case 1:
                if (buf.remaining() < 1)
                    return false;

                ordered = commState.getBoolean();

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                byte plc0 = commState.getByte();

                plc = GridIoPolicy.fromOrdinal(plc0);

                commState.idx++;

            case 3:
                if (buf.remaining() < 1)
                    return false;

                skipOnTimeout = commState.getBoolean();

                commState.idx++;

            case 4:
                if (buf.remaining() < 8)
                    return false;

                timeout = commState.getLong();

                commState.idx++;

            case 5:
                byte[] topicBytes0 = commState.getByteArray();

                if (topicBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                topicBytes = topicBytes0;

                commState.idx++;

            case 6:
                if (buf.remaining() < 4)
                    return false;

                topicOrd = commState.getInt();

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
