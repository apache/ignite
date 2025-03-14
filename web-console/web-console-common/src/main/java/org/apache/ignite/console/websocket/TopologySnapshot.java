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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.console.utils.Utils.attribute;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IPS;


/**
 * Topology snapshot POJO.
 */
public class TopologySnapshot {
    /** Optional Ignite cluster ID. */
    private static final String IGNITE_CLUSTER_ID = "IGNITE_CLUSTER_ID";

    /** Optional GridGain plugin attribute. */
    private static final String GRIDGAIN_PLUGIN = "plugins.gg.node";
    
    /** Optional GridGain Ultimate plugin attribute. */
    private static final String ULTIMATE_CLUSTER = "plugins.gg.ultimate";

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
     * GridGain plugin flag.
     */
    private boolean gridgain;

    /**
     * GridGain ultimate plugin flag.
     */
    private boolean ultimate;

    /**
     * Cluster version.
     */
    private String clusterVer;

    /**
     * Cluster nodes.
     */
    private Map<UUID, NodeBean> nodes;

    /** Feature set that is supported by nodes. */
    private byte[] supportedFeatures;

    /** */
    private final long creationTime = U.currentTimeMillis();

    /**
     * Default constructor for serialization.
     */
    public TopologySnapshot() {
        nodes = Collections.emptyMap();
    }

    /**
     * Constructor from list of nodes.
     *
     * @param nodes Nodes.
     */
    public TopologySnapshot(Collection<GridClientNodeBean> nodes) {
        Collection<GridClientNodeBean> srvs = forServers(nodes);

        id = firstNonNullAttribute(srvs, IGNITE_CLUSTER_ID);
        name = firstNonNullAttribute(srvs, IGNITE_CLUSTER_NAME);
        gridgain = allHasAttribute(srvs, GRIDGAIN_PLUGIN, true);
        ultimate = allHasAttribute(srvs, ULTIMATE_CLUSTER, true);

        //supportedFeatures = supportedFeatures(nodes);
        clusterVer = clusterVersion(nodes);

        this.nodes = nodeMap(nodes);
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
     * @return value of gridgain
     */
    public boolean isGridgain() {
        return gridgain;
    }

    /**
     * @return value of ultimate
     */
    public boolean isUltimate() {
        return ultimate;
    }

    /**
     * @param nodes Cluster nodes.
     * @return Cluster nodes map.
     */
    private Map<UUID, NodeBean> nodeMap(Collection<GridClientNodeBean> nodes) {
        return nodes.stream()
            .collect(Collectors.toMap(GridClientNodeBean::getNodeId, (node) -> {
                Map<String, Object> attrs = node.getAttributes();

                Boolean client = attribute(attrs, ATTR_CLIENT_MODE);
                if(client==null) client = false;
                Collection<String> nodeAddrs = client
                    ? splitAddresses(attribute(attrs, ATTR_IPS))
                    : node.getTcpAddresses();

                String firstIP = F.first(nodeAddrs);

                return new NodeBean(client, firstIP);
            }));
    }

    private List<String> splitAddresses(String ips){
    	String[] ipsV = ips.split(", ");
    	return Arrays.asList(ipsV);
    }
    /**
     * @return Cluster nodes IDs.
     */
    public Set<UUID> nids() {
        return nodes.keySet();
    }

    /**
     * @return Features supported by the current grid.
     */
    public byte[] getSupportedFeatures() {
        return supportedFeatures;
    }

   

    /**
     * @param nodes Cluster nodes.
     * @param key Attribute name.
     * @return First non null value of attribute.
     */
    private String firstNonNullAttribute(Collection<GridClientNodeBean> nodes, String key) {
        return nodes.stream()
            .map(n -> n.getAttributes().get(key))
            .filter(Objects::nonNull)
            .findFirst()
            .map(Object::toString)
            .orElse(null);
    }

    /**
     * @param nodes Cluster nodes.
     * @param key Attribute name.
     * @param val Target attribute value.
     * @return First non null value of attribute.
     */
    private boolean allHasAttribute(Collection<GridClientNodeBean> nodes, String key, Object val) {
        return nodes.stream()
            .allMatch(n -> val.equals(n.getAttributes().get(key)));
    }

    /**
     * @param nodes Cluster nodes.
     * @return All server nodes.
     */
    private Collection<GridClientNodeBean> forServers(Collection<GridClientNodeBean> nodes) {
        return nodes.stream()
            .filter(n -> n.getAttributes().get(ATTR_CLIENT_MODE)==null || !(Boolean)n.getAttributes().get(ATTR_CLIENT_MODE))
            .collect(toList());
    }

    /**
     * @param nodes Nodes.
     */
    private String clusterVersion(Collection<GridClientNodeBean> nodes) {
        return nodes.stream()
            .map(n -> n.getAttributes().get(ATTR_BUILD_VER))
            .filter(Objects::nonNull)
            .map(Object::toString)
            .min(comparing(IgniteProductVersion::fromString))
            .orElse(null);
    }

    /**
     * @param other Other topology.
     * @return {@code true} in case if current topology is a same cluster.
     */
    public boolean sameNodes(TopologySnapshot other) {
        return !(other == null || F.isEmpty(other.nids()) || Collections.disjoint(nids(), other.nids()));
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TopologySnapshot snapshot = (TopologySnapshot)o;

        return id.equals(snapshot.id) &&
            nids().equals(snapshot.nids()) &&
            active == snapshot.active &&
            Objects.equals(name, snapshot.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id, nids(), active, name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TopologySnapshot.class, this);
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
}
