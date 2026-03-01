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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Message for {@link ClusterNode}.
 */
public class ClusterNodeMessage implements TcpDiscoveryMarshallableMessage, ClusterNode {
    /** Node ID. */
    @Order(0)
    UUID id;

    /** Internal discovery addresses as strings. */
    @Order(1)
    Collection<String> addrs;

    /** Internal discovery host names as strings. */
    @Order(2)
    Collection<String> hostNames;

    /** */
    @Order(3)
    public TcpDiscoveryNodeMetricsMessage metricsMsg;

    /** */
    @Order(4)
    long order;

    /** */
    @Order(5)
    IgniteProductVersionMessage verMsg;

    /** Consistent ID. Should be {@link String}, but is not guaranteed by the various APIs. */
    Object consistentId;

    /** */
    @Order(6)
    byte[] consistentIdBytes;

    /** */
    Map<String, Object> attrs;

    /** */
    @Order(7)
    byte[] attrsBytes;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public ClusterNodeMessage() {
        // No-op.
    }

    /** @param clusterNode Cluster node. */
    public ClusterNodeMessage(ClusterNode clusterNode) {
        id = clusterNode.id();
        consistentId = clusterNode.consistentId();
        order = clusterNode.order();
        addrs = clusterNode.addresses();
        hostNames = clusterNode.hostNames();
        verMsg = new IgniteProductVersionMessage(clusterNode.version());

        attrs = new HashMap<>(clusterNode.attributes());

        metricsMsg = new TcpDiscoveryNodeMetricsMessage(clusterNode.metrics());
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

        if (consistentId != null && consistentIdBytes == null) {
            try {
                consistentIdBytes = U.marshal(marsh, consistentId);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal cluster node's consistent id.", e);
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

        if (consistentIdBytes != null && consistentId == null) {
            try {
                consistentId = U.unmarshal(marsh, consistentIdBytes, clsLdr);

                consistentIdBytes = null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to cluster node's consistent id.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T attribute(String name) {
        return F.isEmpty(attrs) ? null : (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        return new ClusterMetricsSnapshot(metricsMsg);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attrs;
    }

    /** @param attrs Attributes. */
    public void attributes(Map<String, Object> attrs) {
        this.attrs = attrs;
        attrsBytes = null;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return hostNames;
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return new IgniteProductVersion(verMsg);
    }

    /** The local flag is transient due to the current logic. */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return Boolean.TRUE.equals(attrs.get(IgniteNodeAttributes.ATTR_CLIENT_MODE));
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -110;
    }
}
