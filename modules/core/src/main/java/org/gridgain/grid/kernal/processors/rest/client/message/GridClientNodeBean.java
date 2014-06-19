/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Node bean.
 */
public class GridClientNodeBean implements Externalizable, GridPortable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int PORTABLE_TYPE_ID = GridClientAbstractMessage.nextSystemTypeId();

    /** Node ID */
    private UUID nodeId;

    /** Consistent ID. */
    private Object consistentId;

    /** REST TCP server addresses. */
    private Collection<String> tcpAddrs;

    /** REST TCP server host names. */
    private Collection<String> tcpHostNames;

    /** REST HTTP server addresses. */
    private Collection<String> jettyAddrs;

    /** REST HTTP server host names. */
    private Collection<String> jettyHostNames;

    /** Rest binary port. */
    private int tcpPort;

    /** Rest HTTP(S) port. */
    private int jettyPort;

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
     * Gets REST HTTP server addresses.
     *
     * @return REST HTTP server addresses.
     */
    public Collection<String> getJettyAddresses() {
        return jettyAddrs;
    }

    /**
     * Gets REST HTTP server host names.
     *
     * @return REST HTTP server host names.
     */
    public Collection<String> getJettyHostNames() {
        return jettyHostNames;
    }

    /**
     * Sets REST HTTP server addresses.
     *
     * @param jettyAddrs REST HTTP server addresses.
     */
    public void setJettyAddresses(Collection<String> jettyAddrs) {
        this.jettyAddrs = jettyAddrs;
    }

    /**
     * Sets REST HTTP server host names.
     *
     * @param jettyHostNames REST HTTP server host names.
     */
    public void setJettyHostNames(Collection<String> jettyHostNames) {
        this.jettyHostNames = jettyHostNames;
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
     * Gets REST http protocol port.
     *
     * @return Http port.
     */
    public int getJettyPort() {
        return jettyPort;
    }

    /**
     * Sets REST http port.
     *
     * @param jettyPort REST http port.
     */
    public void setJettyPort(int jettyPort) {
        this.jettyPort = jettyPort;
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

//    /** {@inheritDoc} */
//    @Override public int typeId() {
//        return PORTABLE_TYPE_ID;
//    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws GridPortableException {
        writer.writeInt("tcpPort", tcpPort);
        writer.writeInt("jettyPort", jettyPort);
        writer.writeInt("replicaCnt", replicaCnt);

        writer.writeString("dfltCacheMode", dfltCacheMode);

        writer.writeMap("attrs", attrs);
        writer.writeMap("caches", caches);

        writer.writeCollection("tcpAddrs", tcpAddrs);
        writer.writeCollection("tcpHostNames", tcpHostNames);
        writer.writeCollection("jettyAddrs", jettyAddrs);
        writer.writeCollection("jettyHostNames", jettyHostNames);

        writer.writeUuid("nodeId", nodeId);

        writer.writeObject("consistentId", consistentId);
        writer.writeObject("metrics", metrics);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws GridPortableException {
        tcpPort = reader.readInt("tcpPort");
        jettyPort = reader.readInt("jettyPort");
        replicaCnt = reader.readInt("replicaCnt");

        dfltCacheMode = reader.readString("dfltCacheMode");

        attrs = reader.readMap("attrs");
        caches = reader.readMap("caches");

        tcpAddrs = reader.readCollection("tcpAddrs");
        tcpHostNames = reader.readCollection("tcpHostNames");
        jettyAddrs = reader.readCollection("jettyAddrs");
        jettyHostNames = reader.readCollection("jettyHostNames");

        nodeId = reader.readUuid("nodeId");

        consistentId = reader.readObject("consistentId");
        metrics = reader.readObject("metrics");
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(tcpPort);
        out.writeInt(jettyPort);
        out.writeInt(replicaCnt);

        U.writeString(out, dfltCacheMode);

        U.writeMap(out, attrs);
        U.writeMap(out, caches);

        U.writeCollection(out, tcpAddrs);
        U.writeCollection(out, tcpHostNames);
        U.writeCollection(out, jettyAddrs);
        U.writeCollection(out, jettyHostNames);

        U.writeUuid(out, nodeId);

        out.writeObject(consistentId);
        out.writeObject(metrics);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tcpPort = in.readInt();
        jettyPort = in.readInt();
        replicaCnt = in.readInt();

        dfltCacheMode = U.readString(in);

        attrs = U.readMap(in);
        caches = U.readMap(in);

        tcpAddrs = U.readCollection(in);
        tcpHostNames = U.readCollection(in);
        jettyAddrs = U.readCollection(in);
        jettyHostNames = U.readCollection(in);

        nodeId = U.readUuid(in);

        consistentId = in.readObject();
        metrics = (GridClientNodeMetricsBean)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientNodeBean [id=" + nodeId + ']';
    }
}
