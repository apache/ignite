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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 * Recovery acknowledgment message.
 */
public class RecoveryLastReceivedMessage implements Message {
    /** */
    public static final long ALREADY_CONNECTED = -1;

    /** */
    public static final long NODE_STOPPING = -2;

    /** Need wait. */
    public static final long NEED_WAIT = -3;

    /** Initiator node is not in current topogy. */
    public static final long UNKNOWN_NODE = -4;

    /**
     * Message body size in bytes. In worst case it uses 10 bytes for serialization.
     *
     * @see DirectByteBufferStream#writeLong(long).
     */
    private static final int MESSAGE_SIZE = 10;

    /** Full message size (with message type) in bytes. */
    public static final int MESSAGE_FULL_SIZE = MESSAGE_SIZE + DIRECT_TYPE_SIZE;

    /** */
    @Order(0)
    long rcvCnt;

    /**
     * Default constructor.
     */
    public RecoveryLastReceivedMessage() {
        // No-op.
    }

    /**
     * @param rcvCnt Number of received messages.
     */
    public RecoveryLastReceivedMessage(long rcvCnt) {
        this.rcvCnt = rcvCnt;
    }

    /**
     * @return Number of received messages.
     */
    public long received() {
        return rcvCnt;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RecoveryLastReceivedMessage.class, this);
    }
}
