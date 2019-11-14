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

package org.apache.ignite.internal.util.distributed;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Single node result message.
 */
public class SingleNodeMessage implements Message {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Requiest id. */
    private UUID reqId;

    /** Single node response. */
    private Serializable response;

    /** Error. */
    private Exception err;

    /** Empty constructor for marshalling purposes. */
    public SingleNodeMessage() {
    }

    /**
     * @param reqId Requiest id.
     * @param response Single node response.
     * @param err Error.
     */
    public SingleNodeMessage(UUID reqId, Serializable response, Exception err) {
        this.reqId = reqId;
        this.response = response;
        this.err = err;
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
                if (!writer.writeUuid("reqId", reqId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("data", U.toBytes(response)))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray("err", U.toBytes(err)))
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
                reqId = reader.readUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                response = U.fromBytes(reader.readByteArray("data"));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                err = U.fromBytes(reader.readByteArray("err"));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(SingleNodeMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 177;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** @return Requiest id. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Single node response. */
    public Serializable response() {
        return response;
    }

    /** @return {@code True} if finished with error. */
    public boolean hasError() {
        return err != null;
    }

    /** @return Error. */
    public Exception error() {
        return err;
    }
}
