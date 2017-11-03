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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Node bean.
 */
public class GridClientNodeBean implements Externalizable {
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

    /** Node caches. */
    private Collection<GridClientCacheBean> caches;

    /** Node order within grid topology. */
    private long order;

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
     * @return Configured node caches.
     */
    public Collection<GridClientCacheBean> getCaches() {
        return caches;
    }

    /**
     * Sets configured node caches.
     *
     * @param caches Configured node caches.
     */
    public void setCaches(Collection<GridClientCacheBean> caches) {
        this.caches = caches;
    }

    /**
     * Sets REST binary protocol port.
     *
     * @param tcpPort Port on which REST binary protocol is bound.
     */
    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /**
     * Node order within grid topology.
     *
     * @return Node startup order.
     */
    public long getOrder() {
        return order;
    }

    /**
     * Set node order within grid topology
     *
     * @param order Node order within grid topology
     */
    public void setOrder(long order) {
        this.order = order;
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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(tcpPort);
        out.writeInt(0); // Jetty port.

        String dfltCacheMode = null;

        Map<String, String> cacheMap = null;

        if (caches != null) {
            cacheMap = U.newHashMap(caches.size());

            for (GridClientCacheBean cacheBean : caches) {
                if (cacheBean.getName() == null)
                    dfltCacheMode = cacheBean.getMode().toString();

                cacheMap.put(cacheBean.getName(), cacheBean.getMode().toString());
            }
        }

        U.writeString(out, dfltCacheMode);

        U.writeMap(out, attrs);
        U.writeMap(out, cacheMap);

        U.writeCollection(out, tcpAddrs);
        U.writeCollection(out, tcpHostNames);
        U.writeCollection(out, Collections.emptyList()); // Jetty addresses.
        U.writeCollection(out, Collections.emptyList()); // Jetty host names.

        U.writeUuid(out, nodeId);

        out.writeObject(String.valueOf(consistentId));
        out.writeObject(metrics);

        out.writeLong(order);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tcpPort = in.readInt();
        in.readInt(); // Jetty port.

        String dfltCacheMode = U.readString(in);

        attrs = U.readMap(in);

        Map<String, String> cacheMap = U.readMap(in);

        if (cacheMap == null && dfltCacheMode != null) {
            cacheMap = U.newHashMap(1);

            cacheMap.put(null, dfltCacheMode);
        }

        if (cacheMap != null) {
            caches = new ArrayList<>(cacheMap.size());

            for (Map.Entry<String, String> e : cacheMap.entrySet())
                caches.add(new GridClientCacheBean(e.getKey(), GridClientCacheMode.valueOf(e.getValue()), null));
        }

        tcpAddrs = U.readCollection(in);
        tcpHostNames = U.readCollection(in);
        U.readCollection(in); // Jetty addresses.
        U.readCollection(in); // Jetty host names.

        nodeId = U.readUuid(in);

        consistentId = in.readObject();
        metrics = (GridClientNodeMetricsBean)in.readObject();

        order = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientNodeBean [id=" + nodeId + ']';
    }
}
