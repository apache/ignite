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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

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

    /** Node ID. */
    private volatile UUID id;

    /** Consistent ID. */
    @GridToStringInclude
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
    private volatile Collection<InetSocketAddress> sockAddrs;

    /** */
    @GridToStringInclude
    private int discPort;

    /** Node metrics. */
    @GridToStringExclude
    private volatile ClusterMetrics metrics;

    /** Node cache metrics. */
    @GridToStringExclude
    private volatile Map<Integer, CacheMetrics> cacheMetrics;

    /** Node order in the topology. */
    private volatile long order;

    /** Node order in the topology (internal). */
    private volatile long intOrder;

    /** The most recent time when metrics update message was received from the node. */
    @GridToStringExclude
    private volatile long lastUpdateTimeNanos = System.nanoTime();

    /** The most recent time when node exchanged a message with a remote node. */
    private volatile long lastExchangeTime = U.currentTimeMillis();

    /** Same as {@link #lastExchangeTime} but as returned by {@link System#nanoTime()} */
    @GridToStringExclude
    private volatile long lastExchangeTimeNanos = System.nanoTime();

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
     * @param consistentId Node consistent ID.
     */
    public TcpDiscoveryNode(UUID id,
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

        this.id = id;

        List<String> sortedAddrs = new ArrayList<>(addrs);

        Collections.sort(sortedAddrs);

        this.addrs = sortedAddrs;
        this.hostNames = hostNames;
        this.discPort = discPort;
        this.metricsProvider = metricsProvider;
        this.ver = ver;

        this.consistentId = consistentId != null ? consistentId : U.consistentId(sortedAddrs, discPort);

        metrics = metricsProvider.metrics();
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
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /**
     * Sets consistent globally unique node ID which survives node restarts.
     *
     * @param consistentId Consistent globally unique node ID.
     */
    @Override public void setConsistentId(Serializable consistentId) {
        this.consistentId = consistentId;

        final Map<String, Object> map = new HashMap<>(attrs);

        map.put(ATTR_NODE_CONSISTENT_ID, consistentId);

        attrs = Collections.unmodifiableMap(map);
    }

    /** {@inheritDoc} */
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
        if (metricsProvider != null) {
            ClusterMetrics metrics0 = metricsProvider.metrics();

            metrics = metrics0;

            return metrics0;
        }

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public void setMetrics(ClusterMetrics metrics) {
        assert metrics != null;

        this.metrics = metrics;
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
    @Override public long order() {
        return order;
    }

    /**
     * @param order Order of the node.
     */
    public void order(long order) {
        assert order > 0 : "Order is invalid: " + this;

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
     * @param lastExchangeTime Time in milliseconds.
     * @param lastExchangeTimeNanos Time in nanoseconds.
     */
    public void lastExchangeTime(long lastExchangeTime, long lastExchangeTimeNanos) {
        this.lastExchangeTime = lastExchangeTime;
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
            Boolean clientModeAttr = ((ClusterNode)this).attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE);

            cacheCli = clientModeAttr != null && clientModeAttr;

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
        id = newId;
    }

    /**
     * @param nodeAttrs Current node attributes.
     * @return Copy of local node for client reconnect request.
     */
    public TcpDiscoveryNode clientReconnectNode(Map<String, Object> nodeAttrs) {
        TcpDiscoveryNode node = new TcpDiscoveryNode(
            id, addrs, hostNames, discPort, metricsProvider, ver, null
        );

        node.attrs = Collections.unmodifiableMap(new HashMap<>(nodeAttrs));
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
        U.writeUuid(out, id);
        U.writeMap(out, attrs);
        U.writeCollection(out, addrs);
        U.writeCollection(out, hostNames);
        out.writeInt(discPort);

        // Cluster metrics
        byte[] mtr = null;

        ClusterMetrics metrics = this.metrics;

        if (metrics != null)
            mtr = ClusterMetricsSnapshot.serialize(metrics);

        U.writeByteArray(out, mtr);

        // Legacy: Number of cache metrics
        out.writeInt(0);

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

        Object consistentIdAttr = attrs.get(ATTR_NODE_CONSISTENT_ID);

        // Cluster metrics
        byte[] mtr = U.readByteArray(in);

        if (mtr != null)
            metrics = ClusterMetricsSnapshot.deserialize(mtr, 0);

        // Legacy: Cache metrics
        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            in.readInt();
            in.readObject();
        }

        order = in.readLong();
        intOrder = in.readLong();
        ver = (IgniteProductVersion)in.readObject();
        clientRouterNodeId = U.readUuid(in);

        if (clientRouterNodeId() != null)
            consistentId = consistentIdAttr != null ? consistentIdAttr : id;
        else
            consistentId = consistentIdAttr != null ? consistentIdAttr : U.consistentId(addrs, discPort);
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

    /**
     * IMPORTANT!
     * Only purpose of this constructor is creating node which contains necessary data to store on disc only
     * @param node to copy data from
     */
    public TcpDiscoveryNode(ClusterNode node) {
        this.id = node.id();
        this.consistentId = node.consistentId();
        this.addrs = node.addresses();
        this.hostNames = node.hostNames();
        this.order = node.order();
        this.ver = node.version();
        this.clientRouterNodeId = node.isClient() ? node.id() : null;

        attrs = Collections.singletonMap(ATTR_NODE_CONSISTENT_ID, consistentId);
    }
}
