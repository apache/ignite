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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.continuous.GridContinuousMessageType.MSG_EVT_ACK;

/**
 * Continuous processor message.
 */
public class GridContinuousMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message type. */
    private GridContinuousMessageType type;

    /** Routine ID. */
    private UUID routineId;

    /** Optional message data. */
    @GridToStringInclude(sensitive = true)
    @GridDirectTransient
    private Object data;

    /** */
    @GridDirectCollection(Message.class)
    private Collection<Message> msgs;

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
     * @param msgs If {@code true} then data is collection of messages.
     */
    GridContinuousMessage(GridContinuousMessageType type,
        @Nullable UUID routineId,
        @Nullable IgniteUuid futId,
        @Nullable Object data,
        boolean msgs) {
        assert type != null;
        assert routineId != null || type == MSG_EVT_ACK;

        this.type = type;
        this.routineId = routineId;
        this.futId = futId;

        if (msgs)
            this.msgs = (Collection)data;
        else
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
     * @return {@code True} is data is collection of messages.
     */
    public boolean messages() {
        return msgs != null;
    }

    /**
     * @return Message data.
     */
    @SuppressWarnings("unchecked")
    public <T> T data() {
        return msgs != null ? (T)msgs : (T)data;
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
    @Override public void onAckReceived() {
        // No-op.
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

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("dataBytes", dataBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection("msgs", msgs, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeUuid("routineId", routineId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByte("type", type != null ? (byte)type.ordinal() : -1))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                dataBytes = reader.readByteArray("dataBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msgs = reader.readCollection("msgs", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                routineId = reader.readUuid("routineId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                byte typeOrd;

                typeOrd = reader.readByte("type");

                if (!reader.isLastRead())
                    return false;

                type = GridContinuousMessageType.fromOrdinal(typeOrd);

                reader.incrementState();

        }

        return reader.afterMessageRead(GridContinuousMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 61;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridContinuousMessage.class, this);
    }
}