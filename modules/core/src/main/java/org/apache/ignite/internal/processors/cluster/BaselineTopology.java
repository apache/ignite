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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.cluster.BaselineNode;
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

    /** Consistent ID comparator. */
    private static final Comparator<Object> CONSISTENT_ID_COMPARATOR = new Comparator<Object>() {
        @Override public int compare(Object o1, Object o2) {
            return o1.toString().compareTo(o2.toString());
        }
    };

    /** */
    private final int id;

    /** Key - node consistent ID, value - node attribute map. */
    private final Map<Object, Map<String, Object>> nodeMap;

    /** Compact ID to consistent ID mapping. */
    private final Map<Short, Object> compactIdMapping;

    /** Consistent ID to compact ID mapping. */
    private final Map<Object, Short> consistentIdMapping;

    /** */
    private long branchingPointHash;

    /** */
    private final List<Long> branchingHist;

    /**
     * @param nodeMap Map of node consistent ID to it's attributes.
     */
    private BaselineTopology(Map<Object, Map<String, Object>> nodeMap, int id) {
        this.id = id;
        this.compactIdMapping = new HashMap<>();
        this.consistentIdMapping = new HashMap<>();

        this.nodeMap = nodeMap;

        Set<Object> consistentIds = new TreeSet<>(CONSISTENT_ID_COMPARATOR);

        for (Object o : nodeMap.keySet()){
            branchingPointHash += (long) o.hashCode();

            consistentIds.add(o);
        }

        short compactId = 0;

        for (Object consistentId : consistentIds) {
            compactIdMapping.put(compactId, consistentId);

            consistentIdMapping.put(consistentId, compactId++);
        }

        branchingHist = new ArrayList<>();

        branchingHist.add(branchingPointHash);
    }

    /**
     * @return id of this BaselineTopology.
     */
    public int id() {
        return id;
    }

    /**
     * @return Set of consistent IDs.
     */
    public Set<Object> consistentIds() {
        return nodeMap.keySet();
    }

    /**
     * @return Activation history.
     */
    public List<Long> branchingHistory() {
        return branchingHist;
    }

    /**
     * @return Compact IDs mapping.
     */
    public Map<Short, Object> compactIdMapping() {
        return compactIdMapping;
    }

    /**
     * @return Consistent IDs mapping.
     */
    public Map<Object, Short> consistentIdMapping() {
        return consistentIdMapping;
    }

    /**
     * @return Short consistent Id.
     */
    public Short resolveShortConsistentId(Object constId){
        return consistentIdMapping.get(constId);
    }


    /**
     * @return Activation hash.
     */
    public long branchingPointHash() {
        return branchingPointHash;
    }

    /**
     * @param consId Node consistent ID.
     * @return Node attributes map.
     */
    public Map<String, Object> attributes(Object consId) {
        return nodeMap.get(consId);
    }

    /**
     *
     */
    public List<BaselineNode> currentBaseline() {
        List<BaselineNode> res = new ArrayList<>();

        for (Map.Entry<Object, Map<String, Object>> consIdAttrsEntry : nodeMap.entrySet())
            res.add(new DetachedClusterNode(consIdAttrsEntry.getKey(), consIdAttrsEntry.getValue()));

        return res;
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
     * @param id ID of BaselineTopology to build.
     * @return Baseline topology consisting of given nodes.
     */
    @Nullable public static BaselineTopology build(Collection<BaselineNode> nodes, int id) {
        if (nodes == null)
            return null;

        Map<Object, Map<String, Object>> nodeMap = new HashMap<>();

        for (BaselineNode node : nodes)
            nodeMap.put(node.consistentId(), node.attributes());

        return new BaselineTopology(nodeMap, id);
    }

    /**
     * @param blt BaselineTopology to check.
     * @return {@code True} if current BaselineTopology is compatible (the same or a newer one) with passed in Blt.
     */
    boolean isCompatibleWith(BaselineTopology blt) {
        return blt == null || (branchingPointHash == blt.branchingPointHash) || branchingHist.contains(blt.branchingPointHash);
    }

    /**
     * @param nodes Nodes.
     */
    boolean updateHistory(Collection<BaselineNode> nodes) {
        long newTopHash = calculateTopologyHash(nodes);

        if (branchingPointHash != newTopHash) {
            branchingPointHash = newTopHash;

            branchingHist.add(newTopHash);

            return true;
        }

        return false;
    }

    /**
     * @param nodes Nodes.
     */
    private long calculateTopologyHash(Collection<BaselineNode> nodes) {
        long res = 0;

        for (BaselineNode node : nodes)
            res += (long) node.consistentId().hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "BaselineTopology[id=" + id + ", branchingHash=" + branchingPointHash + ", baselineNodes=" + nodeMap.keySet() + "]";
    }
}
