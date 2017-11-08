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
 *
 */

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class BaselineTopology implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key - node consistent ID, value - node attribute map. */
    private final Map<Object, Map<String, Object>> nodeMap;

    /** */
    private long activationHash;

    /** */
    private final List<Long> activationHist;

    /**
     * @param nodeMap Map of node consistent ID to it's attributes.
     */
    public BaselineTopology(Map<Object, Map<String, Object>> nodeMap) {
        this.nodeMap = nodeMap;

        for (Object o : nodeMap.keySet())
            activationHash += (long) o.hashCode();

        activationHist = new ArrayList<>();

        activationHist.add(activationHash);
    }

    /**
     * @return Set of consistent IDs.
     */
    public Set<Object> consistentIds() {
        return nodeMap.keySet();
    }

    /**
     * @param consId Node consistent ID.
     * @return Node attributes map.
     */
    public Map<String, Object> attributes(Object consId) {
        return nodeMap.get(consId);
    }

    /**
     * @param aliveNodes Sorted list of currently alive nodes.
     * @param nodeFilter Node filter.
     * @return Sorted list of baseline topology nodes.
     */
    public List<ClusterNode> createBaselineView(
        List<ClusterNode> aliveNodes,
        @Nullable IgnitePredicate<ClusterNode> nodeFilter)
    {
        List<ClusterNode> res = new ArrayList<>(nodeMap.size());

        for (ClusterNode node : aliveNodes) {
            if (nodeMap.containsKey(node.consistentId()) && (nodeFilter == null || CU.affinityNode(node, nodeFilter)))
                res.add(node);
        }

        assert res.size() <= nodeMap.size();

        if (res.size() == nodeMap.size())
            return res;

        Map<Object, ClusterNode> consIdMap = new HashMap<>();

        for (ClusterNode node : aliveNodes) {
            if (nodeMap.containsKey(node.consistentId()) && (nodeFilter == null || CU.affinityNode(node, nodeFilter)))
                consIdMap.put(node.consistentId(), node);
        }

        for (Map.Entry<Object, Map<String, Object>> e : nodeMap.entrySet()) {
            Object consId = e.getKey();

            if (!consIdMap.containsKey(consId)) {
                DetachedClusterNode node = new DetachedClusterNode(consId, e.getValue());

                if (nodeFilter == null || CU.affinityNode(node, nodeFilter))
                    consIdMap.put(consId, node);
            }
        }

        res = new ArrayList<>();

        res.addAll(consIdMap.values());

        Collections.sort(res, NodeOrderComparator.getInstance());

        return res;
    }

    /**
     * @param presentedNodes Nodes present in cluster.
     * @return {@code True} if current topology satisfies baseline.
     */
    public boolean isSatisfied(@NotNull Collection<ClusterNode> presentedNodes) {
        if (presentedNodes.size() < nodeMap.size())
            return false;

        Set<Object> presentedNodeIds = new HashSet<>();

        for (ClusterNode node : presentedNodes)
            presentedNodeIds.add(node.consistentId());

        return presentedNodeIds.containsAll(nodeMap.keySet());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BaselineTopology topology = (BaselineTopology)o;

        return nodeMap != null ? nodeMap.keySet().equals(topology.nodeMap.keySet()) : topology.nodeMap == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nodeMap != null ? nodeMap.hashCode() : 0;
    }

    /**
     * @param blt1 Baseline topology instance.
     * @param blt2 Baseline topology instance.
     * @return {@code True} if equals.
     */
    public static boolean equals(BaselineTopology blt1, BaselineTopology blt2) {
        if (blt1 == null && blt2 == null)
            return true;

        if (blt1 == null ^ blt2 == null)
            return false;

        return blt1.equals(blt2);
    }

    /**
     * @param nodes Nodes.
     * @return Baseline topology consisting of given nodes.
     */
    @Nullable public static BaselineTopology build(Collection<ClusterNode> nodes) {
        if (nodes == null)
            return null;

        Map<Object, Map<String, Object>> nodeMap = new HashMap<>();

        for (ClusterNode node : nodes)
            nodeMap.put(node.consistentId(), node.attributes());

        return new BaselineTopology(nodeMap);
    }

    /**
     * @param blt BaselineTopology to check.
     * @return {@code True} if current BaselineTopology is compatible (the same or a newer one) with passed in Blt.
     */
    boolean isCompatibleWith(BaselineTopology blt) {
        return blt == null || (activationHash == blt.activationHash) || activationHist.contains(blt.activationHash);
    }

    boolean isSuccessorOf(BaselineTopology blt) {
        return blt == null || (activationHist.contains(blt.activationHash) && activationHash != blt.activationHash);
    }

    /**
     * @param nodes Nodes.
     */
    boolean updateHistory(Collection<ClusterNode> nodes) {
        long newTopHash = calculateTopologyHash(nodes);

        if (activationHash != newTopHash) {
            activationHash = newTopHash;

            activationHist.add(newTopHash);

            return true;
        }

        return false;
    }

    /**
     * @param nodes Nodes.
     */
    private long calculateTopologyHash(Collection<ClusterNode> nodes) {
        long res = 0;

        for (ClusterNode node : nodes)
            res += (long) node.consistentId().hashCode();

        return res;
    }
}
