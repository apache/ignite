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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
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
    private static final C1<BaselineNode, ClusterNode> BASELINE_TO_CLUSTER = new C1<BaselineNode, ClusterNode>() {
        @Override public ClusterNode apply(BaselineNode baselineNode) {
            return (ClusterNode)baselineNode;
        }
    };

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

    /** Baseline nodes. */
    private final List<? extends BaselineNode> baselineNodes;

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

    /** Minimum {@link IgniteProductVersion} across all nodes including client nodes. */
    private final IgniteProductVersion minNodeVer;

    /** Minimum {@link IgniteProductVersion} across alive server nodes. */
    private final IgniteProductVersion minSrvNodeVer;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    final Map<UUID, Short> nodeIdToConsIdx;

    /** */
    final Map<Short, UUID> consIdxToNodeId;

    /** */
    private final P1<BaselineNode> aliveBaselineNodePred;

    /** */
    private final P1<ClusterNode> aliveNodePred;

    /**
     * @param topVer Topology version.
     * @param state Current cluster state.
     * @param loc Local node.
     * @param rmtNodes Remote nodes.
     * @param allNodes All nodes.
     * @param srvNodes Server nodes.
     * @param daemonNodes Daemon nodes.
     * @param rmtNodesWithCaches Remote nodes with at least one cache configured.
     * @param baselineNodes Baseline nodes or {@code null} if baseline was not set.
     * @param allCacheNodes Cache nodes by cache name.
     * @param cacheGrpAffNodes Affinity nodes by cache group ID.
     * @param nodeMap Node map.
     * @param alives0 Alive nodes.
     * @param nodeIdToConsIdx Node ID to consistent ID mapping.
     * @param consIdxToNodeId Consistent ID to node ID mapping.
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
        @Nullable List<? extends BaselineNode> baselineNodes,
        Map<Integer, List<ClusterNode>> allCacheNodes,
        Map<Integer, List<ClusterNode>> cacheGrpAffNodes,
        Map<UUID, ClusterNode> nodeMap,
        Set<UUID> alives0,
        @Nullable Map<UUID, Short> nodeIdToConsIdx,
        @Nullable Map<Short, UUID> consIdxToNodeId,
        IgniteProductVersion minNodeVer,
        IgniteProductVersion minSrvNodeVer
    ) {
        this.topVer = topVer;
        this.state = state;
        this.loc = loc;
        this.rmtNodes = rmtNodes;
        this.allNodes = allNodes;
        this.srvNodes = srvNodes;
        this.daemonNodes = daemonNodes;
        this.rmtNodesWithCaches = rmtNodesWithCaches;
        this.baselineNodes = baselineNodes;
        this.allCacheNodes = allCacheNodes;
        this.cacheGrpAffNodes = cacheGrpAffNodes;
        this.nodeMap = nodeMap;
        this.alives.addAll(alives0);
        this.minNodeVer = minNodeVer;
        this.minSrvNodeVer = minSrvNodeVer;
        this.nodeIdToConsIdx = nodeIdToConsIdx;
        this.consIdxToNodeId = consIdxToNodeId;

        aliveBaselineNodePred = new P1<BaselineNode>() {
            @Override public boolean apply(BaselineNode node) {
                return node instanceof ClusterNode && alives.contains(((ClusterNode)node).id());
            }
        };

        aliveNodePred = new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return alives.contains(node.id());
            }
        };
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
     * @return Minimum server node version.
     */
    public IgniteProductVersion minimumServerNodeVersion() {
        return minSrvNodeVer;
    }

    /**
     * @return Current cluster state.
     */
    public DiscoveryDataClusterState state() {
        return state;
    }

    /** @return Local node. */
    public ClusterNode localNode() {
        return loc;
    }

    /** @return Remote nodes. */
    public List<ClusterNode> remoteNodes() {
        return rmtNodes;
    }

    /**
     * Returns a collection of baseline nodes.
     *
     * @return A collection of baseline nodes or {@code null} if baseline topology was not set.
     */
    @Nullable public List<? extends BaselineNode> baselineNodes() {
        return baselineNodes;
    }

    /**
     * @param nodeId Node ID to check.
     * @return {@code True} if baseline is not set or the node is in the baseline topology.
     */
    public boolean baselineNode(UUID nodeId) {
        return nodeIdToConsIdx == null || nodeIdToConsIdx.containsKey(nodeId);
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

    /** @return Consistent id map UUID -> Short (compacted consistent id). */
    public Map<UUID, Short> consistentIdMap() {
        return nodeIdToConsIdx;
    }

    /** @return Consistent id map Short (compacted consistent id) -> UUID. */
    public Map<Short, UUID> nodeIdMap() {
        return consIdxToNodeId;
    }

    /**
     * Gets all alive remote nodes that have at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public Collection<ClusterNode> remoteAliveNodesWithCaches() {
        return F.view(rmtNodesWithCaches, aliveNodePred);
    }

    /**
     * Gets collection of server nodes with at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public Collection<ClusterNode> aliveServerNodes() {
        return F.view(serverNodes(), aliveNodePred);
    }

    /**
     * Returns a collection of live baseline nodes.
     *
     * @return A view of baseline nodes that are currently present in the cluster or {@code null} if baseline
     *      topology was not set.
     */
    @Nullable public Collection<ClusterNode> aliveBaselineNodes() {
        return baselineNodes == null ? null : F.viewReadOnly(baselineNodes, BASELINE_TO_CLUSTER, aliveBaselineNodePred);

    }

    /**
     * @param node Node to check.
     * @return {@code True} if the node is in baseline or if baseline is not set.
     */
    public boolean baselineNode(ClusterNode node) {
        return nodeIdToConsIdx == null || nodeIdToConsIdx.get(node.id()) != null;
    }

    /**
     * @return Oldest alive server node.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Nullable public ClusterNode oldestAliveServerNode() {
        // Avoid iterator allocation.
        for (int i = 0; i < srvNodes.size(); i++) {
            ClusterNode srv = srvNodes.get(i);

            if (alives.contains(srv.id()))
                return srv;
        }

        return null;
    }

    /**
     * @return Oldest server node.
     */
    @Nullable public ClusterNode oldestServerNode() {
        if (!srvNodes.isEmpty())
            return srvNodes.get(0);

        return null;
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if node is in alives list.
     */
    public boolean alive(UUID nodeId) {
        return alives.contains(nodeId);
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
     *
     * Returns {@code True} if all nodes has the given attribute and its value equals to {@code expVal}.
     *
     * @param <T> Attribute Type.
     * @param name Attribute name.
     * @param expVal Expected value.
     * @return {@code True} if all the given nodes has the given attribute and its value equals to {@code expVal}.
     */
    public <T> boolean checkAttribute(String name, T expVal) {
        for (ClusterNode node : allNodes) {
            T attr = node.attribute(name);

            if (attr == null || !expVal.equals(attr))
                return false;
        }

        return true;
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
            baselineNodes,
            allCacheNodes,
            cacheGrpAffNodes,
            nodeMap,
            alives,
            nodeIdToConsIdx,
            consIdxToNodeId,
            minNodeVer,
            minSrvNodeVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoCache.class, this);
    }
}
