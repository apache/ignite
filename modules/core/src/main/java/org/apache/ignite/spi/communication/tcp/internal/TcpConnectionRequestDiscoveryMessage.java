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

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Message is part of communication via discovery protocol.
 *
 * It is used when a node (say node A) cannot establish a communication connection to other node (node B) in topology
 * due to firewall or network configuration and sends this message requesting inverse connection:
 * node B receives request and opens communication connection to node A
 * thus allowing both nodes to communicate to each other.
 */
public class TcpConnectionRequestDiscoveryMessage implements DiscoveryCustomMessage, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short DIRECT_TYPE = 502;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    @GridToStringInclude
    @Order(0)
    private UUID receiverNodeId;

    /** */
    @GridToStringInclude
    @Order(value = 1, method = "connectionIndex")
    private int connIdx;

    /** */
    public TcpConnectionRequestDiscoveryMessage() {}

    /** */
    public TcpConnectionRequestDiscoveryMessage(UUID receiverNodeId, int connIdx) {
        this.receiverNodeId = receiverNodeId;
        this.connIdx = connIdx;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** */
    public UUID receiverNodeId() {
        return receiverNodeId;
    }

    /** */
    public void receiverNodeId(UUID receiverNodeId) {
        this.receiverNodeId = receiverNodeId;
    }

    /** */
    public int connectionIndex() {
        return connIdx;
    }

    /** */
    public void connIdx(int connIdx) {
        this.connIdx = connIdx;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return DIRECT_TYPE;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(
        GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache
    ) {
        throw new UnsupportedOperationException("createDiscoCache");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpConnectionRequestDiscoveryMessage.class, this);
    }
}
