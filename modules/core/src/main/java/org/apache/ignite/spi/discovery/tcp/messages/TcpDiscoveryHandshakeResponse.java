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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Handshake response.
 */
public class TcpDiscoveryHandshakeResponse extends TcpDiscoveryAbstractMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(5)
    long order;

    /** */
    @Order(6)
    boolean prevNodeAliveFlag;

    /** Redirect addresses messages serialization holder. */
    @Order(7)
    @Nullable Collection<InetSocketAddressMessage> redirectAddrsMsgs;

    /**
     * Default constructor for {@link DiscoveryMessageFactory}.
     */
    public TcpDiscoveryHandshakeResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param locNodeOrder Local node order.
     */
    public TcpDiscoveryHandshakeResponse(UUID creatorNodeId, long locNodeOrder) {
        super(creatorNodeId);

        order = locNodeOrder;
    }

    /**
     * Gets previous node alive flag.<br>
     * {@code True} means node has connectivity to it's previous node in a ring.
     *
     * @return previous node alive flag.
     */
    public boolean previousNodeAlive() {
        return prevNodeAliveFlag;
    }

    /**
     * Sets topology change flag.<br>
     * {@code True} means node has connectivity to it's previous node in a ring.
     *
     * @param prevNodeAliveFlag previous node alive flag.
     */
    public void previousNodeAlive(boolean prevNodeAliveFlag) {
        this.prevNodeAliveFlag = prevNodeAliveFlag;
    }

    /**
     * Gets order of the node sent the response.
     *
     * @return Order of the node sent the response.
     */
    public long order() {
        return order;
    }

    /** @return Socket addresses list for redirect. */
    public @Nullable Collection<InetSocketAddress> redirectAddresses() {
        return F.isEmpty(redirectAddrsMsgs)
            ? null
            : F.transform(redirectAddrsMsgs, msg -> new InetSocketAddress(msg.address(), msg.port()));
    }

    /** @param sockAddrs Socket addresses list for redirect. */
    public void redirectAddresses(@Nullable Collection<InetSocketAddress> sockAddrs) {
        redirectAddrsMsgs = sockAddrs == null
            ? null
            : F.viewReadOnly(sockAddrs, addr -> new InetSocketAddressMessage(addr.getAddress(), addr.getPort()));
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryHandshakeResponse.class, this, "super", super.toString(),
            "isPreviousNodeAlive", previousNodeAlive());
    }
}
