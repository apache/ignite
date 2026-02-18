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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Message for {@link ClusterNode}. Requites pre- and post- serialization with the class loader.
 * <br>
 * Requires pre- and -post marshalling.
 *
 * @see #prepareMarshal(Marshaller)
 * @see #finishUnmarshal(Marshaller, ClassLoader)
 */
public class ClusterNodeMessage implements Message {
    /** Node ID. */
    @Order(0)
    private UUID id;

    /** Internal discovery addresses as strings. */
    @Order(value = 1, method = "addresses")
    private Collection<String> addrs;

    /** Internal discovery host names as strings. */
    @Order(2)
    private Collection<String> hostNames;

    /** */
    @Order(value = 3, method = "clusterMetricsMessage")
    private NodeMetricsMessage clusterMetricsMsg;

    /** */
    @Order(value = 4)
    private long order;

    /** */
    @Order(value = 5, method = "productVersionMessage")
    private IgniteProductVersionMessage productVerMsg;

    /** Grid local node flag (transient). */
    @Order(value = 6, method = "local")
    private boolean loc;

    /** */
    @Order(7)
    private boolean client;

    /** */
    @Order(8)
    private String dataCenterId;

    /** Consistent ID. */
    private Object consistentId;

    /** */
    private byte[] consistentIdBytes;

    /** Node attributes. */
    private Map<String, Object> attrs;

    /** */
    private byte[] attrsBytes;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public ClusterNodeMessage() {
        // No-op.
    }

    /** @param clusterNode Cluster node. */
    public ClusterNodeMessage(ClusterNode clusterNode) {
        id = clusterNode.id();
        addrs = clusterNode.addresses();
        hostNames = clusterNode.hostNames();
        if (clusterNode.metrics() != null)
            clusterMetricsMsg = new NodeMetricsMessage(clusterNode.metrics());
        order = clusterNode.order();
        productVerMsg = new IgniteProductVersionMessage(clusterNode.version());
        loc = clusterNode.isLocal();
        client = clusterNode.isClient();
        dataCenterId = clusterNode.dataCenterId();
        attrs = clusterNode.attributes();
    }

    /** @param marsh Marshalled. */
    public void prepareMarshal(Marshaller marsh) {
        if (F.isEmpty(attrs) && attrsBytes == null) {
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

    /**
     * @param marsh Marshalled.
     * @param clsLdr Class loader.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) {
        if (attrsBytes != null && F.isEmpty(attrs)) {
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

    /** @return Addresses. */
    public Collection<String> addresses() {
        return addrs;
    }

    /** @param addrs Addresses. */
    public void addresses(Collection<String> addrs) {
        this.addrs = addrs;
    }

    /** @return Node metrics message. */
    public NodeMetricsMessage clusterMetricsMessage() {
        return clusterMetricsMsg;
    }

    /** @param clusterMetricsMsg Node metrics message. */
    public void clusterMetricsMessage(NodeMetricsMessage clusterMetricsMsg) {
        this.clusterMetricsMsg = clusterMetricsMsg;
    }

    /** @return Client flag. */
    public boolean client() {
        return client;
    }

    /** @param client Client flag. */
    public void client(boolean client) {
        this.client = client;
    }

    /** @return Datacenter id. */
    public String dataCenterId() {
        return dataCenterId;
    }

    /** @param dataCenterId Datacenter id. */
    public void dataCenterId(String dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /** @return Host names. */
    public Collection<String> hostNames() {
        return hostNames;
    }

    /** @param hostNames Host names. */
    public void hostNames(Collection<String> hostNames) {
        this.hostNames = hostNames;
    }

    /** @return Node id. */
    public UUID id() {
        return id;
    }

    /** @param id Node id. */
    public void id(UUID id) {
        this.id = id;
    }

    /** @return Node order. */
    public long order() {
        return order;
    }

    /** @param order Node order. */
    public void order(long order) {
        this.order = order;
    }

    /** @return Local node flag. */
    public boolean local() {
        return loc;
    }

    /** @param loc Local node flag. */
    public void local(boolean loc) {
        this.loc = loc;
    }

    /** @return Product version. */
    public IgniteProductVersionMessage productVersionMessage() {
        return productVerMsg;
    }

    /** @param productVerMsg Product version.  */
    public void productVersionMessage(IgniteProductVersionMessage productVerMsg) {
        this.productVerMsg = productVerMsg;
    }

    /** @return Node consistent id. */
    public Object consistentId() {
        return consistentId;
    }

    /** @param consistentId Node consistent id. */
    public void consistentId(Object consistentId) {
        this.consistentId = consistentId;
    }

    /** @return Node's attributes. */
    public Map<String, Object> attributes() {
        return attrs;
    }

    /** @param attrs Node's attributes. */
    public void attributes(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -109;
    }
}
