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
 * Message indicating that user operation is finished locally on the node.
 * Server nodes send this message to coordinator when the user operation is finished.
 */
public class UserManagementOperationFinishedMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Operation Id. */
    private IgniteUuid opId;

    /** Error message. */
    private String errorMsg;

    /**
     *
     */
    public UserManagementOperationFinishedMessage() {
        // No-op.
    }

    /**
     * @param opId operation id
     * @param errorMsg error message
     */
    public UserManagementOperationFinishedMessage(IgniteUuid opId, String errorMsg) {
        this.opId = opId;
        this.errorMsg = errorMsg;
    }

    /**
     * @return Operation ID,
     */
    public IgniteUuid operationId() {
        return opId;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return errorMsg == null;
    }

    /**
     * @return Error message.
     */
    public String errorMessage() {
        return errorMsg;
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
                if (!writer.writeString("errorMsg", errorMsg))
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
                errorMsg = reader.readString("errorMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                opId = reader.readIgniteUuid("opId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(UserManagementOperationFinishedMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 130;
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
        return S.toString(UserManagementOperationFinishedMessage.class, this);
    }
}
