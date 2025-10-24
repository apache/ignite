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
 * Handshake message.
 */
public class HandshakeMessage implements Message {
    /** Message body size in bytes. */
    private static final int MESSAGE_SIZE = 36 + 1; // additional byte for null flag of UUID value.

    /** Full message size (with message type) in bytes. */
    public static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    /** */
    @Order(0)
    private UUID nodeId;

    /** */
    @Order(value = 1, method = "received")
    private long rcvCnt;

    /** */
    @Order(value = 2, method = "connectCount")
    private long connectCnt;

    /** */
    @Order(value = 3, method = "connectionIndex")
    private int connIdx;

    /**
     * Default constructor.
     */
    public HandshakeMessage() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param connectCnt Connect count.
     * @param rcvCnt Number of received messages.
     * @param connIdx Connection index.
     */
    public HandshakeMessage(UUID nodeId, long connectCnt, long rcvCnt, int connIdx) {
        assert nodeId != null;
        assert rcvCnt >= 0 : rcvCnt;

        this.nodeId = nodeId;
        this.connectCnt = connectCnt;
        this.rcvCnt = rcvCnt;
        this.connIdx = connIdx;
    }

    /**
     * @return Connection index.
     */
    public int connectionIndex() {
        return connIdx;
    }

    /**
     * @return Connect count.
     */
    public long connectCount() {
        return connectCnt;
    }

    /**
     * @return Number of received messages.
     */
    public long received() {
        return rcvCnt;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param connIdx Connection index.
     */
    public void connectionIndex(int connIdx) {
        this.connIdx = connIdx;
    }

    /**
     * @param connectCnt Connect count.
     */
    public void connectCount(long connectCnt) {
        this.connectCnt = connectCnt;
    }

    /**
     * @param rcvCnt Number of received messages.
     */
    public void received(long rcvCnt) {
        this.rcvCnt = rcvCnt;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Message size in bytes.
     */
    public int getMessageSize() {
        return MESSAGE_FULL_SIZE;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TcpCommunicationSpi.HANDSHAKE_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HandshakeMessage.class, this);
    }
}
