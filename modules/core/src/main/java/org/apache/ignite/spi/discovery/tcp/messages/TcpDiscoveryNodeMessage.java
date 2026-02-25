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

import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/**
 * Message for {@link TcpDiscoveryNode}.
 *
 * @see TcpDiscoveryNode#writeExternal(ObjectOutput)
 */
public class TcpDiscoveryNodeMessage extends ClusterNodeMessage {
    /** */
    @Order(10)
    public int discPort;

    /** */
    @Order(11)
    public long intOrder;

    /** */
    public @Nullable UUID clientRouterNodeId;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryNodeMessage() {
        // No-op.
    }

    /** @param tcpDiscoveryNode Tcp Discovery node. */
    public TcpDiscoveryNodeMessage(TcpDiscoveryNode tcpDiscoveryNode) {
        super(tcpDiscoveryNode);

        discPort = tcpDiscoveryNode.discoveryPort();
        intOrder = tcpDiscoveryNode.internalOrder();
        clientRouterNodeId = tcpDiscoveryNode.clientRouterNodeId();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -112;
    }
}
