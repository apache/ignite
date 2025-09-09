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

package org.apache.ignite.spi.communication.tcp.internal;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Inverse connection response message sent by client node as a response to
 * inverse connection request received by discovery.
 *
 * The main purpose of this message is to communicate back to server node connection index of a thread waiting for
 * establishing of communication connection.
 */
public class TcpInverseConnectionResponseMessage implements TcpConnectionIndexAwareMessage {
    /** */
    @Order(value = 0, method = "connectionIndex")
    private int connIdx;

    /** */
    public TcpInverseConnectionResponseMessage() {
    }

    /** */
    public TcpInverseConnectionResponseMessage(int connIdx) {
        this.connIdx = connIdx;
    }

    /** {@inheritDoc} */
    @Override public int connectionIndex() {
        return connIdx;
    }

    /**
     * @param connIdx New connection index.
     */
    public void connectionIndex(int connIdx) {
        this.connIdx = connIdx;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 177;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpInverseConnectionResponseMessage.class, this);
    }
}
