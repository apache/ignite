/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl;

import org.gridgain.client.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

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

    /** REST HTTP server addresses. */
    private List<String> jettyAddrs = Collections.emptyList();

    /** REST HTTP server host names. */
    private List<String> jettyHostNames = Collections.emptyList();

    /** Port for TCP rest binary protocol. */
    private int tcpPort;

    /** Port for HTTP(S) rest binary protocol. */
    private int httpPort;

    /** Node attributes. */
    private Map<String, Object> attrs = Collections.emptyMap();

    /** Node metrics. */
    private GridClientNodeMetrics metrics;

    /** Node caches. */
    private Map<String, GridClientCacheMode> caches = Collections.emptyMap();

    /** Replica count for partitioned cache. */
    private int replicaCnt;

    /** Connectable property. */
    private boolean connectable;

    /** Cache for REST TCP socket addresses. */
    private final AtomicReference<Collection<InetSocketAddress>> tcpSockAddrs = new AtomicReference<>();

    /** Cache for REST HTTP socket addresses. */
    private final AtomicReference<Collection<InetSocketAddress>> jettySockAddrs = new AtomicReference<>();

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
            .jettyAddresses(from.jettyAddresses())
            .tcpPort(from.tcpPort())
            .httpPort(from.httpPort())
            .caches(from.caches())
            .replicaCount(from.replicaCount())
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
    @Override public List<String> jettyAddresses() {
        return jettyAddrs;
    }

    /** {@inheritDoc} */
    @Override public List<String> jettyHostNames() {
        return jettyHostNames;
    }

    /** {@inheritDoc} */
    @Override public int tcpPort() {
        return tcpPort;
    }

    /** {@inheritDoc} */
    @Override public int httpPort() {
        return httpPort;
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
    @Override public int replicaCount() {
        return replicaCnt;
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> availableAddresses(GridClientProtocol proto) {
        Collection<String> addrs;
        Collection<String> hostNames;
        AtomicReference<Collection<InetSocketAddress>> addrsCache;
        final int port;

        switch (proto) {
            case TCP:
                addrsCache = tcpSockAddrs;
                addrs = tcpAddrs;
                hostNames = tcpHostNames;
                port = tcpPort;

                break;

            case HTTP:
                addrsCache = jettySockAddrs;
                addrs = jettyAddrs;
                hostNames = jettyHostNames;
                port = httpPort;

                break;

            default:
                throw new AssertionError("Unknown protocol: " + proto);
        }

        Collection<InetSocketAddress> addrs0 = addrsCache.get();

        if (addrs0 != null)
            return addrs0;

        addrs0 = U.toSocketAddresses(addrs, hostNames, port);

        if (!addrsCache.compareAndSet(null, addrs0))
            return addrsCache.get();

        return addrs0;
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
            ", jettyAddrs=" + jettyAddrs +
            ", jettyHostNames=" + jettyHostNames +
            ", binaryPort=" + tcpPort +
            ", httpPort=" + httpPort +
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
         * Sets list of REST HTTP server addresses.
         *
         * @param jettyAddrs List of address strings.
         * @return This for chaining.
         */
        public Builder jettyAddresses(Collection<String> jettyAddrs) {
            impl.jettyAddrs = U.sealList(jettyAddrs);

            return this;
        }

        /**
         * Sets list of REST HTTP server host names.
         *
         * @param jettyHostNames List of host names.
         * @return This for chaining.
         */
        public Builder jettyHostNames(Collection<String> jettyHostNames) {
            impl.jettyHostNames = U.sealList(jettyHostNames);

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
         * Sets remote http port value.
         *
         * @param httpPort Http(s) port value.
         * @return This for chaining.
         */
        public Builder httpPort(int httpPort) {
            impl.httpPort = httpPort;

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
         * Sets replica count for node on consistent hash ring.
         *
         * @param replicaCnt Replica count.
         * @return This for chaining.
         */
        public Builder replicaCount(int replicaCnt) {
            impl.replicaCnt = replicaCnt;

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
