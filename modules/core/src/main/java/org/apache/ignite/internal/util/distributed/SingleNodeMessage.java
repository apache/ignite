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
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Single node result message.
 *
 * @param <R> Result type.
 * @see DistributedProcess
 * @see FullMessage
 * @see InitMessage
 */
public class SingleNodeMessage<R extends Serializable> implements Message {
    /** Initial channel message type (value is {@code 176}). */
    public static final short TYPE_CODE = 176;

    /** Process id. */
    private UUID processId;

    /** Process type. */
    private int type;

    /** Single node response. */
    private R resp;

    /** Error. */
    private Throwable err;

    /** Empty constructor for marshalling purposes. */
    public SingleNodeMessage() {
    }

    /**
     * @param processId Process id.
     * @param type Process type.
     * @param resp Single node response.
     * @param err Error.
     */
    public SingleNodeMessage(UUID processId, DistributedProcessType type, R resp, Throwable err) {
        this.processId = processId;
        this.type = type.ordinal();
        this.resp = resp;
        this.err = err;
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
                if (!writer.writeUuid(processId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt(type))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray(U.toBytes(resp)))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeByteArray(U.toBytes(err)))
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
                processId = reader.readUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                type = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                byte[] dataBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                resp = U.fromBytes(dataBytes);

                reader.incrementState();

            case 3:
                byte[] errBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                err = U.fromBytes(errBytes);

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** @return Process id. */
    public UUID processId() {
        return processId;
    }

    /** @return Process type. */
    public int type() {
        return type;
    }

    /** @return Response. */
    public R response() {
        return resp;
    }

    /** @return {@code True} if finished with error. */
    public boolean hasError() {
        return err != null;
    }

    /** @return Error. */
    public Throwable error() {
        return err;
    }
}
