/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.apache.ignite.portables.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Node bean.
 */
public class GridClientNodeBean implements Externalizable, PortableMarshalAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID */
    private UUID nodeId;

    /** Consistent ID. */
    private Object consistentId;

    /** REST TCP server addresses. */
    private Collection<String> tcpAddrs;

    /** REST TCP server host names. */
    private Collection<String> tcpHostNames;

    /** Rest binary port. */
    private int tcpPort;

    /** Metrics. */
    private GridClientNodeMetricsBean metrics;

    /** Node attributes. */
    private Map<String, Object> attrs;

    /** Mode for cache with {@code null} name. */
    private String dfltCacheMode;

    /** Node caches. */
    private Map<String, String> caches;

    /** Default replica count for partitioned cache. */
    private int replicaCnt;

    /**
     * Gets node ID.
     *
     * @return Node Id.
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * Sets node ID.
     *
     * @param nodeId Node ID.
     */
    public void setNodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Consistent ID.
     */
    public Object getConsistentId() {
        return consistentId;
    }

    /**
     * @param consistentId New consistent ID.
     */
    public void setConsistentId(Object consistentId) {
        this.consistentId = consistentId;
    }

    /**
     * Gets REST TCP server addresses.
     *
     * @return REST TCP server addresses.
     */
    public Collection<String> getTcpAddresses() {
        return tcpAddrs;
    }

    /**
     * Gets REST TCP server host names.
     *
     * @return REST TCP server host names.
     */
    public Collection<String> getTcpHostNames() {
        return tcpHostNames;
    }

    /**
     * Sets REST TCP server addresses.
     *
     * @param tcpAddrs REST TCP server addresses.
     */
    public void setTcpAddresses(Collection<String> tcpAddrs) {
        this.tcpAddrs = tcpAddrs;
    }

    /**
     * Sets REST TCP server host names.
     *
     * @param tcpHostNames REST TCP server host names.
     */
    public void setTcpHostNames(Collection<String> tcpHostNames) {
        this.tcpHostNames = tcpHostNames;
    }

    /**
     * Gets metrics.
     *
     * @return Metrics.
     */
    public GridClientNodeMetricsBean getMetrics() {
        return metrics;
    }

    /**
     * Sets metrics.
     *
     * @param metrics Metrics.
     */
    public void setMetrics(GridClientNodeMetricsBean metrics) {
        this.metrics = metrics;
    }

    /**
     * Gets attributes.
     *
     * @return Attributes.
     */
    public Map<String, Object> getAttributes() {
        return attrs;
    }

    /**
     * Sets attributes.
     *
     * @param attrs Attributes.
     */
    public void setAttributes(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    /**
     * Gets REST binary protocol port.
     *
     * @return Port on which REST binary protocol is bound.
     */
    public int getTcpPort() {
        return tcpPort;
    }

    /**
     * Gets configured node caches.
     *
     * @return Map where key is cache name and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").
     */
    public Map<String, String> getCaches() {
        return caches;
    }

    /**
     * Sets configured node caches.
     *
     * @param caches Map where key is cache name and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").
     */
    public void setCaches(Map<String, String> caches) {
        this.caches = caches;
    }

    /**
     * Gets mode for cache with null name.
     *
     * @return Default cache mode.
     */
    public String getDefaultCacheMode() {
        return dfltCacheMode;
    }

    /**
     * Sets mode for default cache.
     *
     * @param dfltCacheMode Default cache mode.
     */
    public void setDefaultCacheMode(String dfltCacheMode) {
        this.dfltCacheMode = dfltCacheMode;
    }

    /**
     * Gets node replica count on consistent hash ring.
     *
     * @return Node replica count.
     */
    public int getReplicaCount() {
        return replicaCnt;
    }

    /**
     * Sets node replica count on consistent hash ring.
     *
     * @param replicaCnt Node replica count.
     */
    public void setReplicaCount(int replicaCnt) {
        this.replicaCnt = replicaCnt;
    }

    /**
     * Sets REST binary protocol port.
     *
     * @param tcpPort Port on which REST binary protocol is bound.
     */
    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nodeId != null ? nodeId.hashCode() : 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        GridClientNodeBean other = (GridClientNodeBean)obj;

        return nodeId == null ? other.nodeId == null : nodeId.equals(other.nodeId);
    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws PortableException {
        GridPortableRawWriter raw = writer.rawWriter();

        raw.writeInt(tcpPort);
        raw.writeInt(replicaCnt);
        raw.writeString(dfltCacheMode);
        raw.writeMap(attrs);
        raw.writeMap(caches);
        raw.writeCollection(tcpAddrs);
        raw.writeCollection(tcpHostNames);
        raw.writeUuid(nodeId);
        raw.writeObject(consistentId);
        raw.writeObject(metrics);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws PortableException {
        GridPortableRawReader raw = reader.rawReader();

        tcpPort = raw.readInt();
        replicaCnt = raw.readInt();

        dfltCacheMode = raw.readString();

        attrs = raw.readMap();
        caches = raw.readMap();

        tcpAddrs = raw.readCollection();
        tcpHostNames = raw.readCollection();

        nodeId = raw.readUuid();

        consistentId = raw.readObject();
        metrics = (GridClientNodeMetricsBean)raw.readObject();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(tcpPort);
        out.writeInt(0); // Jetty port.
        out.writeInt(replicaCnt);

        U.writeString(out, dfltCacheMode);

        U.writeMap(out, attrs);
        U.writeMap(out, caches);

        U.writeCollection(out, tcpAddrs);
        U.writeCollection(out, tcpHostNames);
        U.writeCollection(out, Collections.emptyList()); // Jetty addresses.
        U.writeCollection(out, Collections.emptyList()); // Jetty host names.

        U.writeUuid(out, nodeId);

        out.writeObject(consistentId);
        out.writeObject(metrics);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tcpPort = in.readInt();
        in.readInt(); // Jetty port.
        replicaCnt = in.readInt();

        dfltCacheMode = U.readString(in);

        attrs = U.readMap(in);
        caches = U.readMap(in);

        tcpAddrs = U.readCollection(in);
        tcpHostNames = U.readCollection(in);
        U.readCollection(in); // Jetty addresses.
        U.readCollection(in); // Jetty host names.

        nodeId = U.readUuid(in);

        consistentId = in.readObject();
        metrics = (GridClientNodeMetricsBean)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientNodeBean [id=" + nodeId + ']';
    }
}
