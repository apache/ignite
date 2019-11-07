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

package org.apache.ignite.internal.managers.encryption;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Master key change single node result.
 *
 * @see GridEncryptionManager.MasterKeyChangeProcess
 */
public class MasterKeyChangeSingleNodeRequestAck implements SingleNodeMessage {
    /** Request id. */
    private UUID reqId;

    /** Error. */
    private String err;

    /**
     * Empty constructor for marshalling purposes.
     */
    public MasterKeyChangeSingleNodeRequestAck() {
    }

    /**
     * @param reqId Request id.
     * @param err Error.
     */
    public MasterKeyChangeSingleNodeRequestAck(UUID reqId, String err) {
        this.reqId = reqId;
        this.err = err;
    }

    /** @return Error. */
    public String error() {
        return err;
    }

    /** @return {@code True} if has error. */
    public boolean hasError() {
        return err != null;
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
                if (!writer.writeString("err", err))
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
                err = reader.readString("err");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(MasterKeyChangeSingleNodeRequestAck.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 177;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public UUID requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MasterKeyChangeSingleNodeRequestAck.class, this, "reqId", reqId);
    }
}
