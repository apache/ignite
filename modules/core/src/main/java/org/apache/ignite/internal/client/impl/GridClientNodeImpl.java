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

package org.apache.ignite.internal.client.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientNodeMetrics;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Client node implementation.
 */
public class GridClientNodeImpl implements GridClientNode {
    /** Node id. */
    private UUID nodeId;

    /** Consistent ID. */
    private Object consistentId;

    /** REST TCP server addresses. */
    private List<String> tcpAddrs = Collections.emptyList();

    /** REST TCP server host names. */
    private List<String> tcpHostNames = Collections.emptyList();

    /** Port for TCP rest binary protocol. */
    private int tcpPort;

    /** Node attributes. */
    private Map<String, Object> attrs = Collections.emptyMap();

    /** Node metrics. */
    private GridClientNodeMetrics metrics;

    /** Node caches. */
    private Map<String, GridClientCacheMode> caches = Collections.emptyMap();

    /** Connectable property. */
    private boolean connectable;

    /** Cache for REST TCP socket addresses. */
    private final AtomicReference<Collection<InetSocketAddress>> tcpSockAddrs = new AtomicReference<>();

    /**
     * Default constructor (private).
     */
    private GridClientNodeImpl() {
        // No-op.
    }

    /**
     * Creates and returns a builder for a new instance
     * of this class.
     *
     * @return Builder for new instance.
     */
    public static Builder builder() {
        return new Builder(new GridClientNodeImpl());
    }

    /**
     * Creates and returns a builder for a new instance
     * of this class, copying data from an input instance.
     *
     * @param from Instance to copy data from.
     * @param skipAttrs Whether to skip attributes.
     * @param skipMetrics Whether to skip metrics.
     * @return Builder for new instance.
     */
    public static Builder builder(GridClientNode from, boolean skipAttrs, boolean skipMetrics) {
        Builder b = new Builder(new GridClientNodeImpl())
            .nodeId(from.nodeId())
            .consistentId(from.consistentId())
            .tcpAddresses(from.tcpAddresses())
            .tcpPort(from.tcpPort())
            .caches(from.caches())
            .connectable(from.connectable());

        if (!skipAttrs)
            b.attributes(from.attributes());

        if (!skipMetrics)
            b.metrics(from.metrics());

        return b;
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public List<String> tcpAddresses() {
        return tcpAddrs;
    }

    /** {@inheritDoc} */
    @Override public List<String> tcpHostNames() {
        return tcpHostNames;
    }

    /** {@inheritDoc} */
    @Override public int tcpPort() {
        return tcpPort;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return Collections.unmodifiableMap(attrs);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T attribute(String name) {
        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public GridClientNodeMetrics metrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Map<String, GridClientCacheMode> caches() {
        return caches;
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> availableAddresses(GridClientProtocol proto,
        boolean filterResolved) {
        Collection<String> addrs;
        Collection<String> hostNames;
        AtomicReference<Collection<InetSocketAddress>> addrsCache;
        final int port;

        if (proto == GridClientProtocol.TCP) {
            addrsCache = tcpSockAddrs;
            addrs = tcpAddrs;
            hostNames = tcpHostNames;
            port = tcpPort;
        }
        else
            throw new AssertionError("Unknown protocol: " + proto);

        Collection<InetSocketAddress> addrs0 = addrsCache.get();

        if (addrs0 != null)
            return filterIfNecessary(addrs0, filterResolved);

        addrs0 = U.toSocketAddresses(addrs, hostNames, port);

        if (!addrsCache.compareAndSet(null, addrs0))
            return filterIfNecessary(addrsCache.get(), filterResolved);

        return filterIfNecessary(addrs0, filterResolved);
    }

    /**
     * Filters sockets with resolved addresses.
     *
     * @param addrs Addresses to filter.
     * @param filter Flag indicating whether filter should be applied or not.
     * @return Collection copy without unresolved addresses if flag is set and collection itself otherwise.
     */
    private Collection<InetSocketAddress> filterIfNecessary(Collection<InetSocketAddress> addrs, boolean filter) {
        if (!filter)
            return addrs;

        List<InetSocketAddress> res = new ArrayList<>(addrs.size());

        for (InetSocketAddress addr : addrs)
            if (!addr.isUnresolved())
                res.add(addr);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean connectable() {
        return connectable;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof GridClientNodeImpl)) return false;

        GridClientNodeImpl that = (GridClientNodeImpl)o;

        return nodeId.equals(that.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nodeId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientNodeImpl [nodeId=" + nodeId +
            ", consistentId=" + consistentId +
            ", tcpAddrs=" + tcpAddrs +
            ", tcpHostNames=" + tcpHostNames +
            ", binaryPort=" + tcpPort +
            ']';
    }

    /**
     * Builder for instances of this class.
     */
    @SuppressWarnings("PublicInnerClass")
    public static final class Builder {
        /** */
        private GridClientNodeImpl impl;

        /** */
        private boolean built;

        /**
         * @param impl Implementation reference to build.
         */
        private Builder(GridClientNodeImpl impl) {
            this.impl = impl;
        }

        /**
         * Finishes instance construction and returns a
         * newly-built instance.
         *
         * @return A newly-built instance.
         */
        public GridClientNodeImpl build() {
            if (built)
                throw new AssertionError("Instance already built.");

            built = true;

            return impl;
        }

        /**
         * Sets node ID.
         *
         * @param nodeId Node ID.
         * @return This for chaining.
         */
        public Builder nodeId(UUID nodeId) {
            impl.nodeId = nodeId;

            return this;
        }

        /**
         * Sets node consistent ID.
         *
         * @param consistentId New consistent ID.
         * @return This for chaining.
         */
        public Builder consistentId(Object consistentId) {
            impl.consistentId = consistentId;

            return this;
        }

        /**
         * Sets list of REST TCP server addresses.
         *
         * @param tcpAddrs List of address strings.
         * @return This for chaining.
         */
        public Builder tcpAddresses(Collection<String> tcpAddrs) {
            impl.tcpAddrs = U.sealList(tcpAddrs);

            return this;
        }

        /**
         * Sets list of REST TCP server host names.
         *
         * @param tcpHostNames List of host names.
         * @return This for chaining.
         */
        public Builder tcpHostNames(Collection<String> tcpHostNames) {
            impl.tcpHostNames = U.sealList(tcpHostNames);

            return this;
        }

        /**
         * Sets remote TCP port value.
         *
         * @param tcpPort Sets remote port value.
         * @return This for chaining.
         */
        public Builder tcpPort(int tcpPort) {
            impl.tcpPort = tcpPort;

            return this;
        }

        /**
         * Sets node attributes.
         *
         * @param attrs Node attributes.
         * @return This for chaining.
         */
        public Builder attributes(Map<String, Object> attrs) {
            impl.attrs = U.sealMap(attrs);

            return this;
        }

        /**
         * Sets node metrics.
         *
         * @param metrics Metrics.
         * @return This for chaining.
         */
        public Builder metrics(GridClientNodeMetrics metrics) {
            impl.metrics = metrics;

            return this;
        }

        /**
         * Sets caches available on remote node.
         *
         * @param caches Cache map.
         * @return This for chaining.
         */
        public Builder caches(Map<String, GridClientCacheMode> caches) {
            impl.caches = U.sealMap(caches);

            return this;
        }

        /**
         * Sets connectable property.
         *
         * @param connectable Connectable value.
         * @return This for chaining.
         */
        public Builder connectable(boolean connectable) {
            impl.connectable = connectable;

            return this;
        }
    }
}