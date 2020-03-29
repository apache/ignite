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
 * Is sent from coordinator node to client to deliver the results of the user authentication.
 */
public class UserAuthenticateResponseMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request ID. */
    private IgniteUuid id;

    /** Error message. */
    private String errMsg;

    /**
     *
     */
    public UserAuthenticateResponseMessage() {
        // No-op.
    }

    /**
     * @param id Request ID.
     * @param errMsg error message
     */
    public UserAuthenticateResponseMessage(IgniteUuid id, String errMsg) {
        this.id = id;
        this.errMsg = errMsg;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return errMsg == null;
    }

    /**
     * @return Error message.
     */
    public String errorMessage() {
        return errMsg;
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
                if (!writer.writeString("errMsg", errMsg))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("id", id))
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
                errMsg = reader.readString("errMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                id = reader.readIgniteUuid("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(UserAuthenticateResponseMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 132;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserAuthenticateResponseMessage.class, this);
    }
}
