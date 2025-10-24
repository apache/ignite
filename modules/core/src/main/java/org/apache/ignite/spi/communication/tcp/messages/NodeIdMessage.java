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

package org.apache.ignite.spi.communication.tcp.messages;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 * Node ID message.
 */
public class NodeIdMessage implements Message {
    /** Message body size (with message type) in bytes. */
    static final int MESSAGE_SIZE = 1 + 16;  // null flag, UUID value.

    /** Full message size (with message type) in bytes. */
    public static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    /** */
    @Order(0)
    private UUID nodeId;

    /** */
    public NodeIdMessage() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     */
    public NodeIdMessage(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /**
     * @return Node ID bytes.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID bytes.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TcpCommunicationSpi.NODE_ID_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeIdMessage.class, this);
    }
}
