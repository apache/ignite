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

import java.net.InetAddress;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Message telling joining node that its authentication failed.
 */
public class TcpDiscoveryAuthFailedMessage extends TcpDiscoveryAbstractMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Creator address. */
    @Order(value = 5, method = "creatorAddressMessage")
    private InetAddressMessage creatorAddrMsg;

    /** Node id for which authentication was failed. */
    @Order(6)
    private UUID targetNodeId;

    /** Default constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryAuthFailedMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param creatorAddr Creator address.
     * @param targetNodeId Node for which authentication was failed.
     */
    public TcpDiscoveryAuthFailedMessage(UUID creatorNodeId, InetAddress creatorAddr, UUID targetNodeId) {
        super(creatorNodeId);

        this.creatorAddrMsg = new InetAddressMessage(creatorAddr);
        this.targetNodeId = targetNodeId;
    }

    /** @return Node for which authentication was failed. */
    public UUID targetNodeId() {
        return targetNodeId;
    }

    /** @param targetNodeId Node for which authentication was failed. */
    public void targetNodeId(UUID targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    /** @return Creator address message. */
    public InetAddressMessage creatorAddressMessage() {
        return creatorAddrMsg;
    }

    /** @param addr Creator address message. */
    public void creatorAddressMessage(InetAddressMessage addr) {
        this.creatorAddrMsg = addr;
    }

    /** @return Creator address. */
    public InetAddress creatorAddress() {
        return creatorAddrMsg.address();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 11;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryAuthFailedMessage.class, this, "super", super.toString());
    }
}
