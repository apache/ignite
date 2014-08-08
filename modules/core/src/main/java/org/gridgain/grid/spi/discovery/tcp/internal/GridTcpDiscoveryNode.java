/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.internal;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Node for {@link GridTcpDiscoverySpi}.
 * <p>
 * <strong>This class is not intended for public use</strong> and has been made
 * <tt>public</tt> due to certain limitations of Java technology.
 */
public class GridTcpDiscoveryNode extends GridMetadataAwareAdapter implements GridNode,
    Comparable<GridTcpDiscoveryNode>, Externalizable {
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
    private volatile GridNodeMetrics metrics;

    /** Node order in the topology. */
    private volatile long order;

    /** Node order in the topology (internal). */
    @GridToStringExclude
    private volatile long intOrder;

    /** The most recent time when heartbeat message was received from the node. */
    @GridToStringExclude
    private volatile long lastUpdateTime = U.currentTimeMillis();

    /** Metrics provider (transient). */
    @GridToStringExclude
    private GridDiscoveryMetricsProvider metricsProvider;

    /** Metrics store (transient). */
    @GridToStringExclude
    private GridTcpDiscoveryMetricsStore metricsStore;

    /** Grid logger (transient). */
    @GridToStringExclude
    private GridLogger log;

    /** Visible flag (transient). */
    @GridToStringExclude
    private boolean visible;

    /** Grid local node flag (transient). */
    private boolean loc;

    /** Version. */
    private GridProductVersion ver;

    /**
     * Public default no-arg constructor for {@link Externalizable} interface.
     */
    public GridTcpDiscoveryNode() {
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
    public GridTcpDiscoveryNode(UUID id, Collection<String> addrs, Collection<String> hostNames, int discPort,
        GridDiscoveryMetricsProvider metricsProvider, GridProductVersion ver) {
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

        metrics = metricsProvider.getMetrics();
        sockAddrs = U.toSocketAddresses(this, discPort);
    }

    /**
     * Sets metrics store.
     *
     * @param metricsStore Metrics store.
     */
    public void metricsStore(GridTcpDiscoveryMetricsStore metricsStore) {
        assert metricsStore != null;

        this.metricsStore = metricsStore;
    }

    /**
     * Sets log.
     *
     * @param log Grid logger.
     */
    public void logger(GridLogger log) {
        this.log = log;
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
        if (GridNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(name))
            return null;

        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        return F.view(attrs, new GridPredicate<String>() {
            @Override public boolean apply(String s) {
                return !GridNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(s);
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
    @Override public GridNodeMetrics metrics() {
        if (metricsProvider != null)
            metrics = metricsProvider.getMetrics();
        else if (metricsStore != null)
            try {
                GridNodeMetrics metrics = metricsStore.metrics(Collections.singletonList(id)).get(id);

                if (metrics != null)
                    this.metrics = metrics;
            }
            catch (GridSpiException e) {
                LT.error(log, e, "Failed to get metrics from metrics store for node: " + this);
            }

        return metrics;
    }

    /**
     * Sets node metrics.
     *
     * @param metrics Node metrics.
     */
    public void setMetrics(GridNodeMetrics metrics) {
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
        assert intOrder >= 0;

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
    @Override public GridProductVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(GridProductVersion ver) {
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
    @Override public int compareTo(@Nullable GridTcpDiscoveryNode node) {
        if (node == null)
            return 1;

        if (internalOrder() == node.internalOrder())
            assert id().equals(node.id()) : "Duplicate order [this=" + this + ", other=" + node + ']';

        return internalOrder() < node.internalOrder() ? -1 : internalOrder() > node.internalOrder() ? 1 :
            id().compareTo(node.id());
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
            mtr = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

            GridDiscoveryMetricsHelper.serialize(mtr, 0, metrics);
        }

        U.writeByteArray(out, mtr);

        out.writeLong(order);
        out.writeLong(intOrder);
        out.writeObject(ver);
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
            metrics = GridDiscoveryMetricsHelper.deserialize(mtr, 0);

        order = in.readLong();
        intOrder = in.readLong();
        ver = (GridProductVersion)in.readObject();
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
        return S.toString(GridTcpDiscoveryNode.class, this);
    }
}
