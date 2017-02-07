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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message sent from <b>coordinator</b> to <b>client</b> when operation is ultimately finished.
 * Empty map of errors means operation has been successful.
 */
public class DdlOperationResult implements Message {
    /** Whole DDL operation ID. */
    private IgniteUuid opId;

    /** Map from node ID to its error, if any. */
    @GridDirectMap(keyType = UUID.class, valueType = byte[].class)
    private Map<UUID, byte[]> errors;

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
                if (!writer.writeMap("errors", errors, MessageCollectionItemType.UUID,
                    MessageCollectionItemType.BYTE_ARR))
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
                errors = reader.readMap("errors", MessageCollectionItemType.UUID, MessageCollectionItemType.BYTE_ARR,
                    false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                opId = reader.readIgniteUuid("opId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(DdlOperationResult.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return GridDdlMessageFactory.OPERATION_RESULT;
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
     * @return Map from node ID to its error, if any.
     */
    public Map<UUID, byte[]> getErrors() {
        return errors;
    }

    /**
     * @param errors Map from node ID to its error, if any.
     */
    public void setErrors(Map<UUID, byte[]> errors) {
        this.errors = errors;
    }
}
