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

package org.apache.ignite.internal.managers.discovery;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DiscoCache {
    /** */
    private final DiscoveryDataClusterState state;

    /** Local node. */
    private final ClusterNode loc;

    /** Remote nodes. */
    private final List<ClusterNode> rmtNodes;

    /** All nodes. */
    private final List<ClusterNode> allNodes;

    /** All server nodes. */
    private final List<ClusterNode> srvNodes;

    /** Daemon nodes. */
    private final List<ClusterNode> daemonNodes;

    /** All remote nodes with at least one cache configured. */
    @GridToStringInclude
    private final List<ClusterNode> rmtNodesWithCaches;

    /** Cache nodes by cache name. */
    @GridToStringInclude
    private final Map<Integer, List<ClusterNode>> allCacheNodes;

    /** Affinity cache nodes by cache name. */
    @GridToStringInclude
    private final Map<Integer, List<ClusterNode>> cacheGrpAffNodes;

    /** Node map. */
    final Map<UUID, ClusterNode> nodeMap;

    /** Alive nodes. */
    final Set<UUID> alives = new GridConcurrentHashSet<>();

    /** */
    private final IgniteProductVersion minNodeVer;

    /** */
    private final AffinityTopologyVersion topVer;

    /**
     * @param topVer Topology version.
     * @param state Current cluster state.
     * @param loc Local node.
     * @param rmtNodes Remote nodes.
     * @param allNodes All nodes.
     * @param srvNodes Server nodes.
     * @param daemonNodes Daemon nodes.
     * @param rmtNodesWithCaches Remote nodes with at least one cache configured.
     * @param allCacheNodes Cache nodes by cache name.
     * @param cacheGrpAffNodes Affinity nodes by cache group ID.
     * @param nodeMap Node map.
     * @param alives Alive nodes.
     * @param minNodeVer Minimum node version.
     */
    DiscoCache(
        AffinityTopologyVersion topVer,
        DiscoveryDataClusterState state,
        ClusterNode loc,
        List<ClusterNode> rmtNodes,
        List<ClusterNode> allNodes,
        List<ClusterNode> srvNodes,
        List<ClusterNode> daemonNodes,
        List<ClusterNode> rmtNodesWithCaches,
        Map<Integer, List<ClusterNode>> allCacheNodes,
        Map<Integer, List<ClusterNode>> cacheGrpAffNodes,
        Map<UUID, ClusterNode> nodeMap,
        Set<UUID> alives,
        IgniteProductVersion minNodeVer) {
        this.topVer = topVer;
        this.state = state;
        this.loc = loc;
        this.rmtNodes = rmtNodes;
        this.allNodes = allNodes;
        this.srvNodes = srvNodes;
        this.daemonNodes = daemonNodes;
        this.rmtNodesWithCaches = rmtNodesWithCaches;
        this.allCacheNodes = allCacheNodes;
        this.cacheGrpAffNodes = cacheGrpAffNodes;
        this.nodeMap = nodeMap;
        this.alives.addAll(alives);
        this.minNodeVer = minNodeVer;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion version() {
        return topVer;
    }

    /**
     * @return Minimum node version.
     */
    public IgniteProductVersion minimumNodeVersion() {
        return minNodeVer;
    }

    /**
     * @return Current cluster state.
     */
    public DiscoveryDataClusterState state() {
        return state;
    }

    /**
     *
     */
    public List<ClusterNode> baselineView() {
        BaselineTopology baselineTop = state().baselineTopology();

        if (baselineTop == null)
            return allNodes();

        HashSet<ClusterNode> res = new HashSet<>();

        return baselineTop.createBaselineView(allNodes());
    }

    /** @return Local node. */
    public ClusterNode localNode() {
        return loc;
    }

    /** @return Remote nodes. */
    public List<ClusterNode> remoteNodes() {
        return rmtNodes;
    }

    /** @return All nodes. */
    public List<ClusterNode> allNodes() {
        return allNodes;
    }

    /** @return Server nodes. */
    public List<ClusterNode> serverNodes() {
        return srvNodes;
    }

    /** @return Daemon nodes. */
    public List<ClusterNode> daemonNodes() {
        return daemonNodes;
    }

    /**
     * Gets all alive remote nodes that have at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public Collection<ClusterNode> remoteAliveNodesWithCaches() {
        return F.view(rmtNodesWithCaches, new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return alives.contains(node.id());
            }
        });
    }

    /**
     * Gets collection of server nodes with at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public Collection<ClusterNode> aliveServerNodes() {
        return F.view(serverNodes(), new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return alives.contains(node.id());
            }
        });
    }

    /**
     * @return Oldest alive server node.
     */
    public @Nullable ClusterNode oldestAliveServerNode(){
        for (int i = 0; i < srvNodes.size(); i++) {
            ClusterNode srv = srvNodes.get(i);

            if (alives.contains(srv.id()))
                return srv;
        }

        return null;
    }

    /**
     * Gets all nodes that have cache with given name.
     *
     * @param cacheName Cache name.
     * @return Collection of nodes.
     */
    public List<ClusterNode> cacheNodes(@Nullable String cacheName) {
        return cacheNodes(CU.cacheId(cacheName));
    }

    /**
     * Gets all nodes that have cache with given ID.
     *
     * @param cacheId Cache ID.
     * @return Collection of nodes.
     */
    public List<ClusterNode> cacheNodes(Integer cacheId) {
        return emptyIfNull(allCacheNodes.get(cacheId));
    }

    /**
     * @param grpId Cache group ID.
     * @return All nodes that participate in affinity calculation.
     */
    public List<ClusterNode> cacheGroupAffinityNodes(int grpId) {
        return emptyIfNull(cacheGrpAffNodes.get(grpId));
    }

    /**
     * @param id Node ID.
     * @return Node.
     */
    @Nullable public ClusterNode node(UUID id) {
        return nodeMap.get(id);
    }

    /**
     * Removes left node from alives lists.
     *
     * @param rmvd Removed node.
     */
    public void updateAlives(ClusterNode rmvd) {
        alives.remove(rmvd.id());
    }

    /**
     * Removes left nodes from cached alives lists.
     *
     * @param discovery Discovery manager.
     */
    public void updateAlives(GridDiscoveryManager discovery) {
        for (UUID alive : alives) {
            if (!discovery.alive(alive))
                alives.remove(alive);
        }
    }

    /**
     * @param order Order.
     * @return Server node instance.
     */
    @Nullable public ClusterNode serverNodeByOrder(long order) {
        int idx = serverNodeBinarySearch(order);

        if (idx >= 0)
            return srvNodes.get(idx);

        return null;
    }

    /**
     * @param order Node order.
     * @return Node index.
     */
    private int serverNodeBinarySearch(long order) {
        int low = 0;
        int high = srvNodes.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            ClusterNode midVal = srvNodes.get(mid);

            int cmp = Long.compare(midVal.order(), order);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }

        return -(low + 1);
    }

    /**
     * @param nodes Cluster nodes.
     * @return Empty collection if nodes list is {@code null}
     */
    private List<ClusterNode> emptyIfNull(List<ClusterNode> nodes) {
        return nodes == null ? Collections.<ClusterNode>emptyList() : nodes;
    }

    /**
     * @param ver Topology version.
     * @param state Not {@code null} state if need override state, otherwise current state is used.
     * @return Copy of discovery cache with new version.
     */
    public DiscoCache copy(AffinityTopologyVersion ver, @Nullable DiscoveryDataClusterState state) {
        return new DiscoCache(
            ver,
            state == null ? this.state : state,
            loc,
            rmtNodes,
            allNodes,
            srvNodes,
            daemonNodes,
            rmtNodesWithCaches,
            allCacheNodes,
            cacheGrpAffNodes,
            nodeMap,
            alives,
            minNodeVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoCache.class, this);
    }
}
