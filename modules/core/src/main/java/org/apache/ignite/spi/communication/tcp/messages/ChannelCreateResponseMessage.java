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

package org.apache.ignite.spi.communication.tcp.messages;

import java.nio.ByteBuffer;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message response for creation of {@link IgniteSocketChannel}.
 */
public class ChannelCreateResponseMessage implements Message {
    /** Request message type */
    public static final int CHANNEL_RESPONSE_MSG_TYPE = -30;

    /** */
    private static final long serialVersionUID = 0L;

    /** Message. */
    private boolean created;

    /**
     * Default constructor required by {@link Message}.
     */
    public ChannelCreateResponseMessage() {
        // Default constructor used only for GridIoMessageFactory.
    }

    /**
     * @param created flag.
     */
    public ChannelCreateResponseMessage(boolean created) {
        this.created = created;
    }

    /**
     * @return {@code True} if created successfully.
     */
    public boolean isCreated() {
        return created;
    }

    /**
     * @param created {@code True} if channel created successfully.
     */
    public void setCreated(boolean created) {
        this.created = created;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        if (writer.state() == 0) {
            if (!writer.writeBoolean("created", created))
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

        if (reader.state() == 0) {
            created = reader.readBoolean("created");

            if (!reader.isLastRead())
                return false;

            reader.incrementState();
        }

        return reader.afterMessageRead(ChannelCreateResponseMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return CHANNEL_RESPONSE_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChannelCreateResponseMessage.class, this);
    }

}
