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

package org.apache.ignite.spi.discovery.tcp.internal;

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.apache.ignite.internal.IgniteNodeAttributes.*;

/**
 * Node for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}.
 * <p>
 * <strong>This class is not intended for public use</strong> and has been made
 * <tt>public</tt> due to certain limitations of Java technology.
 */
public class TcpDiscoveryNode extends GridMetadataAwareAdapter implements ClusterNode,
    Comparable<TcpDiscoveryNode>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID id;

    /** Consistent ID. */
    private Object consistentId;

    /** Node attributes. */
    @GridToStringExclude
    private Map<String, Object> attrs;

    /** Internal discovery addresses as strings. */
    @GridToStringInclude
    private Collection<String> addrs;

    /** Internal discovery host names as strings. */
    private Collection<String> hostNames;

    /** */
    @GridToStringInclude
    private Collection<InetSocketAddress> sockAddrs;

    /** */
    @GridToStringInclude
    private int discPort;

    /** Node metrics. */
    @GridToStringExclude
    private volatile ClusterMetrics metrics;

    /** Node order in the topology. */
    private volatile long order;

    /** Node order in the topology (internal). */
    private volatile long intOrder;

    /** The most recent time when heartbeat message was received from the node. */
    @GridToStringExclude
    private volatile long lastUpdateTime = U.currentTimeMillis();

    /** Metrics provider (transient). */
    @GridToStringExclude
    private DiscoveryMetricsProvider metricsProvider;

    /** Visible flag (transient). */
    @GridToStringExclude
    private boolean visible;

    /** Grid local node flag (transient). */
    private boolean loc;

    /** Version. */
    private IgniteProductVersion ver;

    /** Alive check (used by clients). */
    @GridToStringExclude
    private transient int aliveCheck;

    /** Client router node ID. */
    @GridToStringExclude
    private UUID clientRouterNodeId;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryNode() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param id Node Id.
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @param discPort Port.
     * @param metricsProvider Metrics provider.
     * @param ver Version.
     */
    public TcpDiscoveryNode(UUID id, Collection<String> addrs, Collection<String> hostNames, int discPort,
                            DiscoveryMetricsProvider metricsProvider, IgniteProductVersion ver) {
        assert id != null;
        assert !F.isEmpty(addrs);
        assert metricsProvider != null;
        assert ver != null;

        this.id = id;
        this.addrs = addrs;
        this.hostNames = hostNames;
        this.discPort = discPort;
        this.metricsProvider = metricsProvider;
        this.ver = ver;

        consistentId = U.consistentId(addrs, discPort);

        metrics = metricsProvider.metrics();
        sockAddrs = U.toSocketAddresses(this, discPort);
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
    @SuppressWarnings("unchecked")
    @Override public <T> T attribute(String name) {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        if (IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(name))
            return null;

        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        return F.view(attrs, new IgnitePredicate<String>() {
            @Override public boolean apply(String s) {
                return !IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(s);
            }
        });
    }

    /**
     * Sets node attributes.
     *
     * @param attrs Node attributes.
     */
    public void setAttributes(Map<String, Object> attrs) {
        this.attrs = U.sealMap(attrs);
    }

    /**
     * Gets node attributes without filtering.
     *
     * @return Node attributes without filtering.
     */
    public Map<String, Object> getAttributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        if (metricsProvider != null)
            metrics = metricsProvider.metrics();

        return metrics;
    }

    /**
     * Sets node metrics.
     *
     * @param metrics Node metrics.
     */
    public void setMetrics(ClusterMetrics metrics) {
        assert metrics != null;

        this.metrics = metrics;
    }

    /**
     * @return Internal order.
     */
    public long internalOrder() {
        return intOrder;
    }

    /**
     * @param intOrder Internal order of the node.
     */
    public void internalOrder(long intOrder) {
        assert intOrder > 0;

        this.intOrder = intOrder;
    }

    /**
     * @return Order.
     */
    @Override public long order() {
        return order;
    }

    /**
     * @param order Order of the node.
     */
    public void order(long order) {
        assert order >= 0 : "Order is invalid: " + this;

        this.order = order;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(IgniteProductVersion ver) {
        assert ver != null;

        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return loc;
    }

    /**
     * @param loc Grid local node flag.
     */
    public void local(boolean loc) {
        this.loc = loc;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return "true".equalsIgnoreCase((String)attribute(ATTR_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return hostNames;
    }

    /**
     * @return Discovery port.
     */
    public int discoveryPort() {
        return discPort;
    }

    /**
     * @return Addresses that could be used by discovery.
     */
    public Collection<InetSocketAddress> socketAddresses() {
        return sockAddrs;
    }

    /**
     * Gets node last update time.
     *
     * @return Time of the last heartbeat.
     */
    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    /**
     * Sets node last update.
     *
     * @param lastUpdateTime Time of last metrics update.
     */
    public void lastUpdateTime(long lastUpdateTime) {
        assert lastUpdateTime > 0;

        this.lastUpdateTime = lastUpdateTime;
    }

    /**
     * Gets visible flag.
     *
     * @return {@code true} if node is in visible state.
     */
    public boolean visible() {
        return visible;
    }

    /**
     * Sets visible flag.
     *
     * @param visible {@code true} if node is in visible state.
     */
    public void visible(boolean visible) {
        this.visible = visible;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return clientRouterNodeId != null;
    }

    /**
     * Decrements alive check value and returns new one.
     *
     * @return Alive check value.
     */
    public int decrementAliveCheck() {
        assert isClient();

        return --aliveCheck;
    }

    /**
     * @param aliveCheck Alive check value.
     */
    public void aliveCheck(int aliveCheck) {
        assert isClient();

        this.aliveCheck = aliveCheck;
    }

    /**
     * @return Client router node ID.
     */
    public UUID clientRouterNodeId() {
        return clientRouterNodeId;
    }

    /**
     * @param clientRouterNodeId Client router node ID.
     */
    public void clientRouterNodeId(UUID clientRouterNodeId) {
        this.clientRouterNodeId = clientRouterNodeId;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@Nullable TcpDiscoveryNode node) {
        if (node == null)
            return 1;

        int res = Long.compare(internalOrder(), node.internalOrder());

        if (res == 0) {
            assert id().equals(node.id()) : "Duplicate order [this=" + this + ", other=" + node + ']';

            res = id().compareTo(node.id());
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, id);
        U.writeMap(out, attrs);
        U.writeCollection(out, addrs);
        U.writeCollection(out, hostNames);
        out.writeInt(discPort);

        byte[] mtr = null;

        if (metrics != null) {
            mtr = new byte[ClusterMetricsSnapshot.METRICS_SIZE];

            ClusterMetricsSnapshot.serialize(mtr, 0, metrics);
        }

        U.writeByteArray(out, mtr);

        out.writeLong(order);
        out.writeLong(intOrder);
        out.writeObject(ver);
        U.writeUuid(out, clientRouterNodeId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readUuid(in);

        attrs = U.sealMap(U.<String, Object>readMap(in));
        addrs = U.readCollection(in);
        hostNames = U.readCollection(in);
        discPort = in.readInt();

        sockAddrs = U.toSocketAddresses(this, discPort);

        consistentId = U.consistentId(addrs, discPort);

        byte[] mtr = U.readByteArray(in);

        if (mtr != null)
            metrics = ClusterMetricsSnapshot.deserialize(mtr, 0);

        order = in.readLong();
        intOrder = in.readLong();
        ver = (IgniteProductVersion)in.readObject();
        clientRouterNodeId = U.readUuid(in);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return F.eqNodes(this, o);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNode.class, this, "isClient", isClient());
    }
}
