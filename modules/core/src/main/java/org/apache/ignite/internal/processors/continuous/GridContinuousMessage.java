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

package org.apache.ignite.internal.processors.continuous;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.apache.ignite.internal.processors.continuous.GridContinuousMessageType.*;

/**
 * Continuous processor message.
 */
public class GridContinuousMessage extends MessageAdapter {
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

    /** Future ID for synchronous event notifications. */
    private IgniteUuid futId;

    /**
     * Required by {@link Externalizable}.
     */
    public GridContinuousMessage() {
        // No-op.
    }

    /**
     * @param type Message type.
     * @param routineId Consume ID.
     * @param futId Future ID.
     * @param data Optional message data.
     */
    GridContinuousMessage(GridContinuousMessageType type,
        @Nullable UUID routineId,
        @Nullable IgniteUuid futId,
        @Nullable Object data) {
        assert type != null;
        assert routineId != null || type == MSG_EVT_ACK;

        this.type = type;
        this.routineId = routineId;
        this.futId = futId;
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
    @SuppressWarnings("unchecked")
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

    /**
     * @return Future ID for synchronous event notification.
     */
    @Nullable public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeField("dataBytes", dataBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeField("futId", futId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeField("routineId", routineId, MessageFieldType.UUID))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeField("type", type != null ? (byte)type.ordinal() : -1, MessageFieldType.BYTE))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (readState) {
            case 0:
                dataBytes = reader.readField("dataBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                futId = reader.readField("futId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                routineId = reader.readField("routineId", MessageFieldType.UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 3:
                byte typeOrd;

                typeOrd = reader.readField("type", MessageFieldType.BYTE);

                if (!reader.isLastRead())
                    return false;

                type = GridContinuousMessageType.fromOrdinal(typeOrd);

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 61;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridContinuousMessage.class, this);
    }
}
