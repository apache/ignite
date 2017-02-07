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

package org.apache.ignite.internal.processors.query.h2.ddl.msg;

import java.nio.ByteBuffer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message sent from <b>peer node</b> to <b>coordinator</b> when local portion of work is done.
 */
public class DdlOperationNodeResult implements Message {
    /** Whole DDL operation ID. */
    private IgniteUuid opId;

    /** Map from node ID to its error, if any. */
    private byte[] err;

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
                if (!writer.writeByteArray("err", err))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("opId", opId))
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
                err = reader.readByteArray("err");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                opId = reader.readIgniteUuid("opId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(DdlOperationNodeResult.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return GridDdlMessageFactory.NODE_RESULT;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * @return Whole DDL operation ID.
     */
    public IgniteUuid getOperationId() {
        return opId;
    }

    /**
     * @param operationId Whole DDL operation ID.
     */
    public void setOperationId(IgniteUuid operationId) {
        this.opId = operationId;
    }

    /**
     * @return Error, if any.
     */
    public byte[] getError() {
        return err;
    }

    /**
     * @param err Error, if any.
     */
    public void setError(byte[] err) {
        this.err = err;
    }
}
