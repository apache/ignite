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

package org.apache.ignite.internal.processors.query.schema.message;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Schema operation status message.
 */
public class SchemaOperationStatusMessage implements Message {
    /** Operation ID. */
    @Order(value = 0, method = "operationId")
    private UUID opId;

    /** Error code. */
    @Order(value = 1, method = "errorCode")
    private int errCode;

    /** Error message. */
    @Order(value = 2, method = "errorMessage")
    private String errMsg;

    /** Sender node ID. */
    private UUID sndNodeId;

    /** No-op flag. */
    @Order(3)
    private boolean nop;

    /**
     * Default constructor.
     */
    public SchemaOperationStatusMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param errCode Error code.
     * @param errMsg Error message.
     * @param nop No-op flag.
     */
    public SchemaOperationStatusMessage(UUID opId, int errCode, @Nullable String errMsg, boolean nop) {
        this.opId = opId;
        this.errCode = errCode;
        this.errMsg = errMsg;
        this.nop = nop;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @param opId Operation ID.
     */
    public void operationId(UUID opId) {
        this.opId = opId;
    }

    /**
     * @return Error code.
     */
    public int errorCode() {
        return errCode;
    }

    /**
     * @param errCode Error code.
     */
    public void errorCode(int errCode) {
        this.errCode = errCode;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Error message.
     */
    public void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @return Sender node ID.
     */
    public UUID senderNodeId() {
        return sndNodeId;
    }

    /**
     * @param sndNodeId Sender node ID.
     */
    public void senderNodeId(UUID sndNodeId) {
        this.sndNodeId = sndNodeId;
    }

    /**
     * @return <code>True</code> if message is no-op.
     */
    public boolean nop() {
        return nop;
    }

    /**
     * @param nop <code>True</code> if message is no-op.
     */
    public void nop(boolean nop) {
        this.nop = nop;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -53;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaOperationStatusMessage.class, this);
    }
}
