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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.jetbrains.annotations.Nullable;

/**
 * Message for {@link TcpDiscoveryNode} and for {@link org.apache.ignite.cluster.ClusterNode}.
 */
public class TcpDiscoveryNodeMessage implements TcpDiscoveryMarshallableMessage, ClusterNode {
    /** */
    @Order(0)
    UUID id;

    /** */
    Map<String, Object> attrs;

    /** */
    @Order(1)
    byte[] attrsBytes;

    /** */
    @Order(2)
    Collection<String> addrs;

    /** */
    @Order(3)
    Collection<String> hostNames;

    /** Port */
    @Order(4)
    int port;

    /** */
    @Order(5)
    TcpDiscoveryNodeMetricsMessage metrics;

    /** */
    @Order(6)
    long order;

    /** */
    @Order(7)
    long intOrder;

    /** */
    @Order(8)
    IgniteProductVersionMessage verMsg;

    /** */
    @Order(9)
    UUID clientRouterNodeId;

    /** */
    @Nullable Object consistentId;

    /** */
    @Order(10)
    @Nullable byte[] consistentIdBytes;

    /** */
    @Order(11)
    boolean loc;

    /** */
    @Order(12)
    boolean client;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryNodeMessage() {
        // No-op.
    }

    /** @param tcpDiscoNode {@link TcpDiscoveryNode}. */
    public TcpDiscoveryNodeMessage(TcpDiscoveryNode tcpDiscoNode) {
        id = tcpDiscoNode.id();
        attrs = tcpDiscoNode.attributes();
        addrs = tcpDiscoNode.addresses();
        hostNames = tcpDiscoNode.hostNames();
        port = tcpDiscoNode.discoveryPort();
        metrics = new TcpDiscoveryNodeMetricsMessage(tcpDiscoNode.metrics());
        order = tcpDiscoNode.order();
        verMsg = new IgniteProductVersionMessage(tcpDiscoNode.version());
        // Not a ClusterNode.
        clientRouterNodeId = tcpDiscoNode.clientRouterNodeId();
        intOrder = tcpDiscoNode.internalOrder();
    }

    /** @param clusterNode {@link ClusterNode}. */
    public TcpDiscoveryNodeMessage(ClusterNode clusterNode) {
        id = clusterNode.id();
        attrs = clusterNode.attributes();
        addrs = clusterNode.addresses();
        hostNames = clusterNode.hostNames();
        metrics = new TcpDiscoveryNodeMetricsMessage(clusterNode.metrics());
        order = clusterNode.order();
        verMsg = new IgniteProductVersionMessage(clusterNode.version());
        // Not transfered by TcpDiscoveryNode.
        consistentId = clusterNode.consistentId();
        loc = clusterNode.isLocal();
        client = clusterNode.isClient();
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) {
        if (attrs != null && attrsBytes == null) {
            try {
                attrsBytes = U.marshal(marsh, attrs);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal cluster node attributes.", e);
            }
        }

        if (consistentId == null && consistentIdBytes == null) {
            try {
                consistentIdBytes = U.marshal(marsh, consistentId);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal consistent id.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (attrsBytes != null && attrs == null) {
            try {
                attrs = U.unmarshal(marsh, attrsBytes, clsLdr);

                attrsBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal cluster node attributes.", e);
            }
        }

        if (consistentId != null && consistentIdBytes == null) {
            try {
                consistentId = U.unmarshal(marsh, consistentIdBytes, clsLdr);

                consistentIdBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal consistent id.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Object consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T attribute(String name) {
        return (T)attributes().get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        assert metrics != null;

        return new ClusterMetricsSnapshot(metrics);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attrs == null ? Collections.emptyMap() : attrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return hostNames;
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return order;
    }

    /** @return Port. */
    public int port() {
        return port;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return new IgniteProductVersion(verMsg);
    }

    /** @return Message of {@link IgniteProductVersion}. */
    public IgniteProductVersionMessage versionMessage() {
        return verMsg;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return client;
    }

    /** @return Internal order. */
    public UUID clientRouterNodeId() {
        return clientRouterNodeId;
    }

    /** @return Internal order. */
    public long internalOrder() {
        return intOrder;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -109;
    }
}
