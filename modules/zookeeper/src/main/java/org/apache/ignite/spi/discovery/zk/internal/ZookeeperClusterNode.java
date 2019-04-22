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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.SecurityCredentialsAttrFilterPredicate;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DAEMON;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

/**
 * Zookeeper Cluster Node.
 */
public class ZookeeperClusterNode implements IgniteClusterNode, Externalizable, Comparable<ZookeeperClusterNode> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final byte CLIENT_NODE_MASK = 0x01;

    /** */
    private UUID id;

    /** */
    private Serializable consistentId;

    /** */
    private long internalId;

    /** */
    private long order;

    /** */
    private IgniteProductVersion ver;

    /** Node attributes. */
    private Map<String, Object> attrs;

    /** Internal discovery addresses as strings. */
    private Collection<String> addrs;

    /** Internal discovery host names as strings. */
    private Collection<String> hostNames;

    /** */
    private long sesTimeout;

    /** Metrics provider. */
    private transient DiscoveryMetricsProvider metricsProvider;

    /** */
    private transient boolean loc;

    /** */
    private transient volatile ClusterMetrics metrics;

    /** Node cache metrics. */
    @GridToStringExclude
    private transient volatile Map<Integer, CacheMetrics> cacheMetrics;

    /** */
    private byte flags;

    /** Daemon node flag. */
    @GridToStringExclude
    private transient boolean daemon;

    /** Daemon node initialization flag. */
    @GridToStringExclude
    private transient volatile boolean daemonInit;

    /** */
    public ZookeeperClusterNode() {
        //No-op
    }

    /**
     * @param id Node ID.
     * @param addrs Node addresses.
     * @param hostNames Node host names.
     * @param ver Node version.
     * @param attrs Node attributes.
     * @param consistentId Consistent ID.
     * @param sesTimeout Zookeeper session timeout.
     * @param client Client node flag.
     * @param metricsProvider Metrics provider.
     */
    public ZookeeperClusterNode(
        UUID id,
        Collection<String> addrs,
        Collection<String> hostNames,
        IgniteProductVersion ver,
        Map<String, Object> attrs,
        Serializable consistentId,
        long sesTimeout,
        boolean client,
        DiscoveryMetricsProvider metricsProvider
    ) {
        assert id != null;
        assert consistentId != null;

        this.id = id;
        this.ver = ver;
        this.attrs = Collections.unmodifiableMap(attrs);
        this.addrs = addrs;
        this.hostNames = hostNames;
        this.consistentId = consistentId;
        this.sesTimeout = sesTimeout;
        this.metricsProvider = metricsProvider;

        if (client)
            flags |= CLIENT_NODE_MASK;
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
    @Override public void setConsistentId(Serializable consistentId) {
        this.consistentId = consistentId;

        final Map<String, Object> map = new HashMap<>(attrs);

        map.put(ATTR_NODE_CONSISTENT_ID, consistentId);

        attrs = Collections.unmodifiableMap(map);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T attribute(String name) {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        if (IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(name))
            return null;

        return (T)attrs.get(name);
    }

    /**
     * Sets node attributes.
     *
     * @param attrs Node attributes.
     */
    void setAttributes(Map<String, Object> attrs) {
        this.attrs = U.sealMap(attrs);
    }

    /**
     * Gets node attributes without filtering.
     *
     * @return Node attributes without filtering.
     */
    Map<String, Object> getAttributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        if (metricsProvider != null) {
            ClusterMetrics metrics0 = metricsProvider.metrics();

            assert metrics0 != null;

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

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        return F.view(attrs, new SecurityCredentialsAttrFilterPredicate());
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

    /**
     * @return Internal ID corresponds to Zookeeper sequential node.
     */
    long internalId() {
        return internalId;
    }

    /**
     * @param internalId Internal ID corresponds to Zookeeper sequential node.
     */
    void internalId(long internalId) {
        this.internalId = internalId;
    }

    /**
     * @param order Node order.
     */
    void order(long order) {
        assert order > 0 : order;

        this.order = order;
    }

    /**
     * @param newId New node ID.
     */
    public void onClientDisconnected(UUID newId) {
        id = newId;
    }

    /**
     * @return Session timeout.
     */
    long sessionTimeout() {
        return sesTimeout;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return ver;
    }

    /**
     * @param loc Local node flag.
     */
    public void local(boolean loc) {
        this.loc = loc;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        if (!daemonInit) {
            daemon = "true".equalsIgnoreCase((String)attribute(ATTR_DAEMON));

            daemonInit = true;
        }

        return daemon;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return (CLIENT_NODE_MASK & flags) != 0;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, id);
        out.writeObject(consistentId);
        out.writeLong(internalId);
        out.writeLong(order);
        out.writeObject(ver);
        U.writeMap(out, attrs);
        U.writeCollection(out, addrs);
        U.writeCollection(out, hostNames);
        out.writeLong(sesTimeout);
        out.writeByte(flags);

        // Cluster metrics
        byte[] mtr = null;

        ClusterMetrics metrics = this.metrics;

        if (metrics != null)
            mtr = ClusterMetricsSnapshot.serialize(metrics);

        U.writeByteArray(out, mtr);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readUuid(in);
        consistentId = (Serializable) in.readObject();
        internalId = in.readLong();
        order = in.readLong();
        ver = (IgniteProductVersion) in.readObject();
        attrs = U.sealMap(U.readMap(in));
        addrs = U.readCollection(in);
        hostNames = U.readCollection(in);
        sesTimeout = in.readLong();
        flags = in.readByte();

        // Cluster metrics
        byte[] mtr = U.readByteArray(in);

        if (mtr != null)
            metrics = ClusterMetricsSnapshot.deserialize(mtr, 0);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@Nullable ZookeeperClusterNode node) {
        if (node == null)
            return 1;

        int res = Long.compare(order, node.order);

        if (res == 0) {
            assert id().equals(node.id()) : "Duplicate order [this=" + this + ", other=" + node + ']';

            res = id().compareTo(node.id());
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return F.eqNodes(this, obj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ZookeeperClusterNode [id=" + id +
            ", addrs=" + addrs +
            ", order=" + order +
            ", loc=" + loc +
            ", client=" + isClient() + ']';
    }
}
