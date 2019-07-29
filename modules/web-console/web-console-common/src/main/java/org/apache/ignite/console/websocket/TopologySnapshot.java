/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.websocket;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.console.utils.Utils.attribute;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IPS;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.sortAddresses;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.splitAddresses;
import static org.apache.ignite.lang.IgniteProductVersion.fromString;

/**
 * Topology snapshot POJO.
 */
public class TopologySnapshot {
    /** Optional Ignite cluster ID. */
    public static final String IGNITE_CLUSTER_ID = "IGNITE_CLUSTER_ID";

    /**
     * Cluster ID.
     */
    private String id;

    /**
     * Cluster name.
     */
    private String name;

    /**
     * Cluster active flag.
     */
    private boolean active;

    /**
     * Configured security flag.
     */
    private boolean secured;

    /**
     * Cluster demo flag.
     */
    private boolean demo;

    /**
     * Cluster version.
     */
    private String clusterVer;

    /**
     * Cluster nodes.
     */
    private Map<UUID, NodeBean> nodes;

    /** */
    private final long creationTime = U.currentTimeMillis();

    /**
     * Default constructor for serialization.
     */
    public TopologySnapshot() {
        // No-op.
    }

    /**
     * Constructor from list of nodes.
     *
     * @param nodes Nodes.
     */
    public TopologySnapshot(Collection<GridClientNodeBean> nodes) {
        int sz = nodes.size();

        this.nodes = U.newHashMap(sz);
        active = false;
        secured = false;

        IgniteProductVersion minNodeVer = null;

        for (GridClientNodeBean node : nodes) {
            UUID nid = node.getNodeId();

            Map<String, Object> attrs = node.getAttributes();

            if (F.isEmpty(id))
                id = attribute(attrs, IGNITE_CLUSTER_ID);

            if (F.isEmpty(name))
                name = attribute(attrs, IGNITE_CLUSTER_NAME);

            String nodeVerAttr = attribute(attrs, ATTR_BUILD_VER);
            IgniteProductVersion nodeVer = fromString(nodeVerAttr);

            if (minNodeVer == null || minNodeVer.compareTo(nodeVer) > 0) {
                minNodeVer = nodeVer;
                clusterVer = nodeVerAttr;
            }

            Boolean client = attribute(attrs, ATTR_CLIENT_MODE);

            Collection<String> nodeAddrs = client
                ? splitAddresses(attribute(attrs, ATTR_IPS))
                : node.getTcpAddresses();

            String firstIP = F.first(sortAddresses(nodeAddrs));

            this.nodes.put(nid, new NodeBean(client, firstIP));
        }
    }

    /**
     * @return Cluster id.
     */
    public String getId() {
        return id;
    }

    /**
     * @param id Cluster id.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return Cluster name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Cluster name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Cluster version.
     */
    public String getClusterVersion() {
        return clusterVer;
    }

    /**
     * @param clusterVer Cluster version.
     */
    public void setClusterVersion(String clusterVer) {
        this.clusterVer = clusterVer;
    }

    /**
     * @return Cluster active flag.
     */
    public boolean isActive() {
        return active;
    }

    /**
     * @param active New cluster active state.
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return {@code true} If cluster has configured security.
     */
    public boolean isSecured() {
        return secured;
    }

    /**
     * @param secured Configured security flag.
     */
    public void setSecured(boolean secured) {
        this.secured = secured;
    }

    /**
     * @return If demo cluster.
     */
    public boolean isDemo() {
        return demo;
    }

    /**
     * @param demo Cluster demo flag.
     */
    public void setDemo(boolean demo) {
        this.demo = demo;
    }

    /**
     * @return Cluster nodes.
     */
    public Map<UUID, NodeBean> getNodes() {
        return nodes;
    }

    /**
     * @param nodes Cluster nodes.
     */
    public void setNodes(Map<UUID, NodeBean> nodes) {
        this.nodes = nodes;
    }

    /**
     * @return Cluster nodes IDs.
     */
    public Collection<UUID> nids() {
        return nodes.keySet();
    }

    /**
     * @param other Other topology.
     * @return {@code true} in case if current topology is a same cluster.
     */
    public boolean sameNodes(TopologySnapshot other) {
        return !(other == null || F.isEmpty(other.nids()) || Collections.disjoint(nids(), other.nids()));
    }

    /**
     * @param other Other topology.
     * @return {@code true} in case if current topology changed.
     */
    public boolean changed(TopologySnapshot other) {
        if (other == null)
            return true;

        if (!id.equals(other.getId()))
            return true;

        if (!Objects.equals(nids(), other.nids()))
            return true;

        return active != other.active;
    }

    /**
     * Returns true if the snapshot is expired.
     *
     * @param maxInactiveInterval The maximum inactive interval.
     * @return true if the snapshot is expired, else false.
     */
    public boolean isExpired(long maxInactiveInterval) {
        return U.currentTimeMillis() - maxInactiveInterval >= creationTime;
    }

    /**
     * Node bean.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class NodeBean {
        /** Is client node. */
        private boolean client;

        /** Canonical ip address. */
        private String addr;

        /**
         * @param client Is client node.
         * @param addr Canonical ip address.
         */
        @JsonCreator
        public NodeBean(@JsonProperty("client") boolean client, @JsonProperty("address") String addr) {
            this.client = client;
            this.addr = addr;
        }

        /**
         * @return Is client node.
         */
        public boolean isClient() {
            return client;
        }

        /**
         * @return Canonical ip address.
         */
        public String getAddress() {
            return addr;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TopologySnapshot.class, this);
    }
}
