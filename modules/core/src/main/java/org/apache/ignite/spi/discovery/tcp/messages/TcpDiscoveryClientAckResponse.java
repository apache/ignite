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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class TcpDiscoveryClientAckResponse extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final IgniteProductVersion CLIENT_ACK_SINCE_VERSION = IgniteProductVersion.fromString("1.4.1");

    /** */
    private final IgniteUuid msgId;

    /**
     * @param creatorNodeId Creator node ID.
     * @param msgId Message ID to ack.
     */
    public TcpDiscoveryClientAckResponse(UUID creatorNodeId, IgniteUuid msgId) {
        super(creatorNodeId);

        this.msgId = msgId;
    }

    /**
     * @return Acknowledged message ID.
     */
    public IgniteUuid messageId() {
        return msgId;
    }

    /** {@inheritDoc} */
    @Override public boolean highPriority() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientAckResponse.class, this, "super", super.toString());
    }
}