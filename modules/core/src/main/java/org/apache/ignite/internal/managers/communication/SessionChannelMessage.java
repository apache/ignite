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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * A message with additional {@link Channel} attibutes which is send on connection established and
 * an appropriate channel is opened.
 */
class SessionChannelMessage implements Message {
    /** Initial channel message type (value is {@code 175}). */
    public static final short TYPE_CODE = 175;

    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Channel session unique identifier. */
    private IgniteUuid sesId;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    public SessionChannelMessage() {
        // No-op.
    }

    /**
     * @param sesId Channel session unique identifier.
     */
    public SessionChannelMessage(IgniteUuid sesId) {
        this.sesId = sesId;
    }

    /**
     * @return The unique session id for the channel.
     */
    public IgniteUuid sesId() {
        return sesId;
    }

    /**
     * @param sesId The unique session id for the channel.
     * @return {@code This} for chaining.
     */
    public SessionChannelMessage sesId(IgniteUuid sesId) {
        this.sesId = sesId;
        return this;
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

        switch (writer.state()) {
            case 0:
                if (!writer.writeIgniteUuid("sesId", sesId))
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
                sesId = reader.readIgniteUuid("sesId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(SessionChannelMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionChannelMessage.class, this);
    }

}
