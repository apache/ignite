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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * WAL state ack message (sent from participant node to coordinator).
 */
@IgniteCodeGeneratingFail
public class WalStateAckMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Operation ID. */
    private UUID opId;

    /** Affinity node flag. */
    private boolean affNode;

    /** Operation result. */
    private boolean changed;

    /** Error message. */
    private String errMsg;

    /** Sender node ID. */
    @GridDirectTransient
    private UUID sndNodeId;

    /**
     * Default constructor.
     */
    public WalStateAckMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param affNode Affinity node.
     * @param changed Operation result.
     * @param errMsg Error message.
     */
    public WalStateAckMessage(UUID opId, boolean affNode, boolean changed, @Nullable String errMsg) {
        this.opId = opId;
        this.affNode = affNode;
        this.changed = changed;
        this.errMsg = errMsg;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @return Affinity node flag.
     */
    public boolean affNode() {
        return affNode;
    }

    /**
     * @return Result.
     */
    public boolean changed() {
        return changed;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errMsg;
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
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeUuid("opId", opId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean("affNode", affNode))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("changed", changed))
                    return false;

                writer.incrementState();

            case 3:
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
                opId = reader.readUuid("opId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                affNode = reader.readBoolean("affNode");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                changed = reader.readBoolean("changed");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                errMsg = reader.readString("errMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(SchemaOperationStatusMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 129;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateAckMessage.class, this);
    }
}
