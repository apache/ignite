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

package org.apache.ignite.internal.processors.authentication;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message is sent from client to coordinator node when a user needs to authorize on client node.
 */
public class UserAuthenticateRequestMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** User name. */
    private String name;

    /** User password.. */
    private String passwd;

    /** Request ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /**
     *
     */
    public UserAuthenticateRequestMessage() {
        // No-op.
    }

    /**
     * @param name User name.
     * @param passwd User password.
     */
    public UserAuthenticateRequestMessage(String name, String passwd) {
        this.name = name;
        this.passwd = passwd;
    }

    /**
     * @return User name.
     */
    public String name() {
        return name;
    }

    /**
     * @return User password.
     */
    public String password() {
        return passwd;
    }

    /**
     * @return Request ID.
     */
    public IgniteUuid id() {
        return id;
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
                if (!writer.writeIgniteUuid("id", id))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("name", name))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString("passwd", passwd))
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
                id = reader.readIgniteUuid("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                name = reader.readString("name");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                passwd = reader.readString("passwd");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(UserAuthenticateRequestMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 131;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserAuthenticateRequestMessage.class, this);
    }
}
