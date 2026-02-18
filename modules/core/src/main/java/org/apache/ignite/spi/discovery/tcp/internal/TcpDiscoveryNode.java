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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.ClusterNodeMessage;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.eqNodes;

/**
 * Node for {@link TcpDiscoverySpi}.
 * <p>
 * <strong>This class is not intended for public use</strong> and has been made
 * <tt>public</tt> due to certain limitations of Java technology.
 */
public class TcpDiscoveryNode extends GridMetadataAwareAdapter implements IgniteClusterNode,
    Comparable<TcpDiscoveryNode>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Values holding message of {@link ClusterNode}. */
    private ClusterNodeMessage clusterNodeMsg;

    /** */
    @GridToStringInclude
    private volatile Collection<InetSocketAddress> sockAddrs;

    /** */
    @GridToStringInclude
    private int discPort;

    /** Node cache metrics. */
    @GridToStringExclude
    private volatile Map<Integer, CacheMetrics> cacheMetrics;

    /** Node order in the topology (internal). */
    private volatile long intOrder;

    /** The most recent time when metrics update message was received from the node. */
    @GridToStringExclude
    private volatile long lastUpdateTimeNanos = System.nanoTime();

    /** The most recent time when node exchanged a message with a remote node. */
    @GridToStringExclude
    private volatile long lastExchangeTimeNanos = System.nanoTime();

    /** Metrics provider (transient). */
    @GridToStringExclude
    private DiscoveryMetricsProvider metricsProvider;

    /** Visible flag (transient). */
    @GridToStringExclude
    private boolean visible;

    /** Alive check time (used by clients). */
    @GridToStringExclude
    private transient volatile long aliveCheckTimeNanos;

    /** Client router node ID. */
    @GridToStringExclude
    private UUID clientRouterNodeId;

    /** */
    @GridToStringExclude
    private transient volatile InetSocketAddress lastSuccessfulAddr;

    /** Cache client initialization flag. */
    @GridToStringExclude
    private transient volatile boolean cacheCliInit;

    /** Cache client flag. */
    @GridToStringExclude
    private transient boolean cacheCli;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public TcpDiscoveryNode() {
        clusterNodeMsg = new ClusterNodeMessage();
    }

    /** */
    public TcpDiscoveryNode(ClusterNodeMessage msg) {
        clusterNodeMsg = msg;
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
     * @param consistentId Node consistent ID.
     */
    public TcpDiscoveryNode(
        UUID id,
        Collection<String> addrs,
        Collection<String> hostNames,
        int discPort,
        DiscoveryMetricsProvider metricsProvider,
        IgniteProductVersion ver,
        Serializable consistentId
    ) {
        assert id != null;
        assert metricsProvider != null;
        assert ver != null;

        List<String> sortedAddrs = new ArrayList<>(addrs);
        Collections.sort(sortedAddrs);

        clusterNodeMsg = new ClusterNodeMessage();
        id(id);
        clusterNodeMsg.consistentId(consistentId != null ? consistentId : U.consistentId(sortedAddrs, discPort));
        clusterNodeMsg.addresses(sortedAddrs);
        clusterNodeMsg.hostNames(hostNames);
        version(ver);

        ClusterMetrics metrics = metricsProvider.metrics();

        if (metrics != null)
            setMetrics(metrics);

        this.metricsProvider = metricsProvider;
        this.discPort = discPort;
        cacheMetrics = metricsProvider.cacheMetrics();
        sockAddrs = U.toSocketAddresses(this, discPort);
    }

    /**
     * @return Last successfully connected address.
     */
    @Nullable public InetSocketAddress lastSuccessfulAddress() {
        return lastSuccessfulAddr;
    }

    /**
     * @param lastSuccessfulAddr Last successfully connected address.
     */
    public void lastSuccessfulAddress(InetSocketAddress lastSuccessfulAddr) {
        this.lastSuccessfulAddr = lastSuccessfulAddr;
    }

    /** {@inheritDoc} */
    @Override public synchronized UUID id() {
        return clusterNodeMsg.id();
    }

    /** */
    private synchronized void id(UUID id) {
        clusterNodeMsg.id(id);
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return clusterNodeMsg.consistentId();
    }

    /**
     * Sets consistent globally unique node ID which survives node restarts.
     *
     * @param consistentId Consistent globally unique node ID.
     */
    @Override public void setConsistentId(Serializable consistentId) {
        clusterNodeMsg.consistentId(consistentId);

        Map<String, Object> map = new HashMap<>(clusterNodeMsg.attributes());

        map.put(ATTR_NODE_CONSISTENT_ID, consistentId);

        clusterNodeMsg.attributes(Collections.unmodifiableMap(map));
    }

    /** {@inheritDoc} */
    @Override public <T> T attribute(String name) {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        if (IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(name))
            return null;

        return (T)clusterNodeMsg.attributes().get(name);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        return F.view(clusterNodeMsg.attributes(), new IgnitePredicate<>() {
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
        clusterNodeMsg.attributes(U.sealMap(attrs));
    }

    /**
     * Gets node attributes without filtering.
     *
     * @return Node attributes without filtering.
     */
    public Map<String, Object> getAttributes() {
        return clusterNodeMsg.attributes();
    }

    /** {@inheritDoc} */
    @Override public synchronized ClusterMetrics metrics() {
        if (metricsProvider != null) {
            ClusterMetrics metrics = metricsProvider.metrics();

            setMetrics(metrics);

            return metrics;
        }

        assert clusterNodeMsg.clusterMetricsMessage() != null;

        return new ClusterMetricsSnapshot(clusterNodeMsg.clusterMetricsMessage());
    }

    /** {@inheritDoc} */
    @Override public synchronized void setMetrics(ClusterMetrics metrics) {
        assert metrics != null;

        clusterNodeMsg.clusterMetricsMessage(new NodeMetricsMessage(metrics));
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, CacheMetrics> cacheMetrics() {
        if (metricsProvider != null) {
            Map<Integer, CacheMetrics> cacheMetrics0 = metricsProvider.cacheMetrics();

            cacheMetrics = cacheMetrics0;

            return cacheMetrics0;
        }

        return cacheMetrics;
    }

    /** {@inheritDoc} */
    @Override public void setCacheMetrics(Map<Integer, CacheMetrics> cacheMetrics) {
        this.cacheMetrics = cacheMetrics != null ? cacheMetrics : Collections.<Integer, CacheMetrics>emptyMap();
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
    @Override public synchronized long order() {
        return clusterNodeMsg.order();
    }

    /**
     * @param order Order of the node.
     */
    public synchronized void order(long order) {
        assert order > 0 : "Order is invalid: " + this;

        clusterNodeMsg.order(order);
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return new IgniteProductVersion(clusterNodeMsg.productVersionMessage());
    }

    /**
     * @param ver Version.
     */
    public void version(IgniteProductVersion ver) {
        assert ver != null;

        clusterNodeMsg.productVersionMessage(ver.message());
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return clusterNodeMsg.addresses();
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return clusterNodeMsg.local();
    }

    /**
     * @param loc Grid local node flag.
     */
    public void local(boolean loc) {
        clusterNodeMsg.local(loc);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return clusterNodeMsg.hostNames();
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
        if (this.sockAddrs == null)
            sockAddrs = U.toSocketAddresses(this, discPort);

        return sockAddrs;
    }

    /**
     * Gets node last update time. Used for logging purposes only.<br/>
     * Note that this method tries to convert {@code nanoTime} internal JVM time format into a regular timestamp.
     * This might lead to errors if there was GC between measuring of current timestamp and current nano time,
     * but generally it might be ignored.
     *
     * @return Time of the last metrics update.
     * @see System#currentTimeMillis()
     * @see System#nanoTime()
     */
    public long lastUpdateTime() {
        return System.currentTimeMillis() - U.nanosToMillis(System.nanoTime() - lastUpdateTimeNanos);
    }

    /**
     * Gets node last update time.
     *
     * @return Time of the last metrics update as returned by {@link System#nanoTime()}.
     */
    public long lastUpdateTimeNanos() {
        return lastUpdateTimeNanos;
    }

    /**
     * Sets node last update.
     *
     * @param lastUpdateTimeNanos Time of last metrics update.
     */
    public void lastUpdateTimeNanos(long lastUpdateTimeNanos) {
        this.lastUpdateTimeNanos = lastUpdateTimeNanos;
    }

    /**
     * Gets the last time a node exchanged a message with a remote node.
     *
     * @return Time in nanoseconds as returned by {@link System#nanoTime()}.
     */
    public long lastExchangeTimeNanos() {
        return lastExchangeTimeNanos;
    }

    /**
     * Sets the last time a node exchanged a message with a remote node.
     *
     * @param lastExchangeTimeNanos Time in nanoseconds.
     */
    public void lastExchangeTime(long lastExchangeTimeNanos) {
        this.lastExchangeTimeNanos = lastExchangeTimeNanos;
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
        if (!cacheCliInit) {
            Boolean clientModeAttr = attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE);

            clusterNodeMsg.client(cacheCli = clientModeAttr != null && clientModeAttr);

            cacheCliInit = true;
        }

        return cacheCli;
    }

    /**
     * Test alive check time value.
     *
     * @return {@code True} if client alive, {@code False} otherwise.
     */
    public boolean isClientAlive() {
        assert isClient() : this;

        return (aliveCheckTimeNanos - System.nanoTime()) >= 0;
    }

    /**
     * Set client alive time.
     *
     * @param aliveTime Alive time interval.
     */
    public void clientAliveTime(long aliveTime) {
        assert isClient() : this;

        aliveCheckTimeNanos = System.nanoTime() + U.millisToNanos(aliveTime);
    }

    /**
     * @return {@code true} if client alive check time initialized.
     */
    public boolean clientAliveTimeSet() {
        return aliveCheckTimeNanos != 0;
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

    /**
     * @param newId New node ID.
     */
    public void onClientDisconnected(UUID newId) {
        id(newId);
    }

    /**
     * @param nodeAttrs Current node attributes.
     * @return Copy of local node for client reconnect request.
     */
    public TcpDiscoveryNode clientReconnectNode(Map<String, Object> nodeAttrs) {
        TcpDiscoveryNode node = new TcpDiscoveryNode(id(), clusterNodeMsg.addresses(), hostNames(), discPort,
            metricsProvider, version(), null);

        node.clusterNodeMsg.attributes(Collections.unmodifiableMap(new HashMap<>(nodeAttrs)));
        node.clientRouterNodeId = clientRouterNodeId;

        return node;
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
        U.writeUuid(out, id());
        U.writeMap(out, clusterNodeMsg.attributes());
        U.writeCollection(out, clusterNodeMsg.addresses());
        U.writeCollection(out, clusterNodeMsg.hostNames());
        out.writeInt(discPort);

        // Cluster metrics
        byte[] mtr = null;

        ClusterMetrics metrics = metrics();

        if (metrics != null)
            mtr = ClusterMetricsSnapshot.serialize(metrics);

        U.writeByteArray(out, mtr);

        // Legacy: Number of cache metrics
        out.writeInt(0);

        out.writeLong(order());
        out.writeLong(intOrder);
        out.writeObject(version());
        U.writeUuid(out, clientRouterNodeId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert clusterNodeMsg != null;

        id(U.readUuid(in));

        clusterNodeMsg.attributes(U.sealMap(U.readMap(in)));
        clusterNodeMsg.addresses(U.readCollection(in));
        clusterNodeMsg.hostNames(U.readCollection(in));
        discPort = in.readInt();

        Object consistentIdAttr = clusterNodeMsg.attributes().get(ATTR_NODE_CONSISTENT_ID);

        // Cluster metrics
        byte[] mtr = U.readByteArray(in);

        if (mtr != null)
            setMetrics(ClusterMetricsSnapshot.deserialize(mtr, 0));

        // Legacy: Cache metrics
        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            in.readInt();
            in.readObject();
        }

        order(in.readLong());
        intOrder = in.readLong();
        version((IgniteProductVersion)in.readObject());
        clientRouterNodeId = U.readUuid(in);

        if (clientRouterNodeId() != null)
            clusterNodeMsg.consistentId(consistentIdAttr != null ? consistentIdAttr : id());
        else {
            clusterNodeMsg.consistentId(consistentIdAttr != null ? consistentIdAttr
                : U.consistentId(clusterNodeMsg.addresses(), discPort));
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id().hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return eqNodes(this, o);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNode.class, this, "isClient", isClient(), "dataCenterId", dataCenterId());
    }

    /**
     * IMPORTANT!
     * Only purpose of this constructor is creating node which contains necessary data to store on disc only
     * @param node to copy data from
     */
    public TcpDiscoveryNode(ClusterNode node) {
        clusterNodeMsg = new ClusterNodeMessage();

        id(node.id());
        clusterNodeMsg.consistentId(node.consistentId());
        clusterNodeMsg.addresses(node.addresses());
        clusterNodeMsg.hostNames(node.hostNames());
        order(node.order());
        version(node.version());
        clusterNodeMsg.attributes(Collections.singletonMap(ATTR_NODE_CONSISTENT_ID, clusterNodeMsg.consistentId()));

        clientRouterNodeId = node.isClient() ? node.id() : null;
    }
}
