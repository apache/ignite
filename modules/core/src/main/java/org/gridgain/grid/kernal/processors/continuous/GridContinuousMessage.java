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

package org.gridgain.grid.kernal.processors.continuous;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Continuous processor message.
 */
public class GridContinuousMessage extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message type. */
    private GridContinuousMessageType type;

    /** Routine ID. */
    private UUID routineId;

    /** Optional message data. */
    @GridToStringInclude
    @GridDirectTransient
    private Object data;

    /** Serialized message data. */
    private byte[] dataBytes;

    /**
     * Required by {@link Externalizable}.
     */
    public GridContinuousMessage() {
        // No-op.
    }

    /**
     * @param type Message type.
     * @param routineId Consume ID.
     * @param data Optional message data.
     */
    GridContinuousMessage(GridContinuousMessageType type, UUID routineId, @Nullable Object data) {
        assert type != null;
        assert routineId != null;

        this.type = type;
        this.routineId = routineId;
        this.data = data;
    }

    /**
     * @return Message type.
     */
    public GridContinuousMessageType type() {
        return type;
    }

    /**
     * @return Consume ID.
     */
    public UUID routineId() {
        return routineId;
    }

    /**
     * @return Message data.
     */
    public <T> T data() {
        return (T)data;
    }

    /**
     * @param data Message data.
     */
    public void data(Object data) {
        this.data = data;
    }

    /**
     * @return Serialized message data.
     */
    public byte[] dataBytes() {
        return dataBytes;
    }

    /**
     * @param dataBytes Serialized message data.
     */
    public void dataBytes(byte[] dataBytes) {
        this.dataBytes = dataBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridContinuousMessage clone = new GridContinuousMessage();

        clone0(clone);

        return clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter msg) {
        GridContinuousMessage clone = (GridContinuousMessage)msg;

        clone.type = type;
        clone.routineId = routineId;
        clone.data = data;
        clone.dataBytes = dataBytes;
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
                if (!commState.putByteArray(dataBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putUuid(routineId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putEnum(type))
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
                byte[] dataBytes0 = commState.getByteArray();

                if (dataBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                dataBytes = dataBytes0;

                commState.idx++;

            case 1:
                UUID routineId0 = commState.getUuid();

                if (routineId0 == UUID_NOT_READ)
                    return false;

                routineId = routineId0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                byte type0 = commState.getByte();

                type = GridContinuousMessageType.fromOrdinal(type0);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 60;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridContinuousMessage.class, this);
    }
}
