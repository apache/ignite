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

package org.apache.ignite.internal.processors.query.schema.message;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Schema operation status message.
 */
@IgniteCodeGeneratingFail
public class SchemaOperationStatusMessage implements Message {
    /** Operation ID. */
    private UUID opId;

    /** Error bytes (if any). */
    private byte[] errBytes;

    /** Sender node ID. */
    @GridDirectTransient
    private UUID sndNodeId;

    /** No-op flag. */
    private boolean nop;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public SchemaOperationStatusMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param errBytes Error bytes.
     * @param nop No-op flag.
     */
    public SchemaOperationStatusMessage(UUID opId, byte[] errBytes, boolean nop) {
        this.opId = opId;
        this.errBytes = errBytes;
        this.nop = nop;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @return Error bytes.
     */
    @Nullable public byte[] errorBytes() {
        return errBytes;
    }

    /**
     * @return Sender node ID.
     */
    public UUID senderNodeId() {
        return sndNodeId;
    }

    /**
     * @param sndNodeId Sender node ID.
     */
    public void senderNodeId(UUID sndNodeId) {
        this.sndNodeId = sndNodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeUuid(opId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray(errBytes))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean(nop))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                opId = reader.readUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                errBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                nop = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /**
     * @return <code>True</code> if message is no-op.
     */
    public boolean nop() {
        return nop;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -53;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaOperationStatusMessage.class, this);
    }
}
