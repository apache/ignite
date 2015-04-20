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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class TcpClientMessageWrapper implements Message {
    /** */
    private Message msg;

    /** */
    private UUID dest;

    /** */
    private UUID snd;

    /**
     * Empty constructor required by {@link Message}.
     */
    public TcpClientMessageWrapper() {
        // No-op.
    }

    /**
     * @param msg Message.
     * @param dest Destination.
     */
    public TcpClientMessageWrapper(Message msg, UUID snd, UUID dest) {
        this.msg = msg;
        this.snd = snd;
        this.dest = dest;
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
                if (!writer.writeMessage("msg", msg))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeUuid("sender", snd))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("dest", dest))
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
                msg = reader.readMessage("msg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                snd = reader.readUuid("src");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                dest = reader.readUuid("dest");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 112;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /**
     * @return Message.
     */
    public Message message() {
        return msg;
    }

    /**
     * @param msg New message.
     */
    public void message(Message msg) {
        this.msg = msg;
    }

    /**
     * @return Destination.
     */
    public UUID destination() {
        return dest;
    }

    /**
     * @param dest New destination.
     */
    public void destination(UUID dest) {
        this.dest = dest;
    }

    /**
     * @return Source.
     */
    public UUID sender() {
        return snd;
    }

    /**
     * @param snd New sender.
     */
    public void sender(UUID snd) {
        this.snd = snd;
    }
}
