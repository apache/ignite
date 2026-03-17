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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/** Message for {@link TcpDiscoveryNode}. */
public class TcpDiscoveryNodeMessage implements MarshallableMessage {
    /** Node ID. */
    @Order(0)
    public UUID id;

    /** */
    public Map<String, Object> attrs;

    /** */
    @Order(1)
    byte[] attrsBytes;

    /** Internal discovery addresses as strings. */
    @Order(2)
    Collection<String> addrs;

    /** Internal discovery host names as strings. */
    @Order(3)
    Collection<String> hostNames;

    /** */
    @Order(4)
    public int discPort;

    /** */
    @Order(5)
    public TcpDiscoveryNodeMetricsMessage metricsMsg;

    /** */
    @Order(6)
    long order;

    /** */
    @Order(7)
    public long intOrder;

    /** */
    @Order(8)
    IgniteProductVersionMessage verMsg;

    /** */
    @Order(9)
    public @Nullable UUID clientRouterNodeId;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryNodeMessage() {
        // No-op.
    }

    /** @param tcpDiscoveryNode Tcp Discovery node. */
    public TcpDiscoveryNodeMessage(TcpDiscoveryNode tcpDiscoveryNode) {
        id = tcpDiscoveryNode.id();
        attrs = new HashMap<>(tcpDiscoveryNode.attributes());
        attrsBytes = null;
        addrs = tcpDiscoveryNode.addresses();
        hostNames = tcpDiscoveryNode.hostNames();
        discPort = tcpDiscoveryNode.discoveryPort();
        metricsMsg = new TcpDiscoveryNodeMetricsMessage(tcpDiscoveryNode.metrics());
        order = tcpDiscoveryNode.order();
        intOrder = tcpDiscoveryNode.internalOrder();
        verMsg = new IgniteProductVersionMessage(tcpDiscoveryNode.version());
        clientRouterNodeId = tcpDiscoveryNode.clientRouterNodeId();
    }



    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (attrs != null && attrsBytes == null)
            attrsBytes = U.marshal(marsh, attrs);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (attrsBytes != null && attrs == null)
            attrs = U.unmarshal(marsh, attrsBytes, clsLdr);

        attrsBytes = null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -111;
    }
}
