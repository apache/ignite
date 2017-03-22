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

package org.apache.ignite.internal.processors.query.index;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Message with index operation status. Sent from participant to coordinator when index creation is completed or
 * when coordinator changes.
 */
public class IndexOperationStatusResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Sender node ID. */
    private UUID sndNodeId;

    /** Operation ID. */
    private UUID opId;

    /** Error message. */
    private String errMsg;

    /**
     * Default constructor.
     */
    public IndexOperationStatusResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param sndNodeId Sender node ID.
     * @param opId Operation ID.
     * @param errMsg Error message.
     */
    public IndexOperationStatusResponse(UUID sndNodeId, UUID opId, String errMsg) {
        this.sndNodeId = sndNodeId;
        this.opId = opId;
        this.errMsg = errMsg;
    }

    /**
     * @return Sender node ID.
     */
    public UUID senderNodeId() {
        return sndNodeId;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @return Error message.
     */
    public String errorMessage() {
        return errMsg;
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
                if (!writer.writeUuid("sndNodeId", sndNodeId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid("opId", opId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString("errMsg", errMsg))
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
                sndNodeId = reader.readUuid("sndNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                opId = reader.readUuid("opId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                errMsg = reader.readString("errMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(IndexOperationStatusResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -50;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexOperationStatusResponse.class, this);
    }
}
