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

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * WAL state ack message (sent from participant node to coordinator).
 */
public class WalStateAckMessage implements Message {
    /** Operation ID. */
    @Order(value = 0, method = "operationId")
    private UUID opId;

    /** Affinity node flag. */
    @Order(1)
    private boolean affNode;

    /** Operation result. */
    @Order(2)
    private boolean changed;

    /** Error message. */
    @Order(value = 3, method = "errorMessage")
    private String errMsg;

    /** Sender node ID. */
    private UUID sndNodeId;

    /**
     * Default constructor.
     */
    public WalStateAckMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param affNode Affinity node.
     * @param changed Operation result.
     * @param errMsg Error message.
     */
    public WalStateAckMessage(UUID opId, boolean affNode, boolean changed, @Nullable String errMsg) {
        this.opId = opId;
        this.affNode = affNode;
        this.changed = changed;
        this.errMsg = errMsg;
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @param opId New operation ID.
     */
    public void operationId(UUID opId) {
        this.opId = opId;
    }

    /**
     * @return Affinity node flag.
     */
    public boolean affNode() {
        return affNode;
    }

    /**
     * @param affNode New affinity node flag.
     */
    public void affNode(boolean affNode) {
        this.affNode = affNode;
    }

    /**
     * @return Result.
     */
    public boolean changed() {
        return changed;
    }

    /**
     * @param changed New operation result.
     */
    public void changed(boolean changed) {
        this.changed = changed;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg New error message.
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

    /** {@inheritDoc} */
    @Override public short directType() {
        return 129;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateAckMessage.class, this);
    }
}
