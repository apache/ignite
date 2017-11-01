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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class BaselineTopology implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key - node consistent ID, value - node attribute map. */
    private final Map<Object, Map<String, Object>> nodeMap;

    /**
     * @param nodeMap Map of node consistent ID to it's attrubites.
     */
    public BaselineTopology(Map<Object, Map<String, Object>> nodeMap) {
        this.nodeMap = nodeMap;
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
     * @return Sorted list of baseline topology nodes.
     */
    public List<ClusterNode> createBaselineView(List<ClusterNode> aliveNodes) {
        return createBaselineView(aliveNodes, null);
    }

    /**
     * @param aliveNodes Sorted list of currently alive nodes.
     * @return Sorted list of baseline topology nodes.
     */
    public List<ClusterNode> createBaselineView(List<ClusterNode> aliveNodes, IgnitePredicate<ClusterNode> nodeFilter)  {
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BaselineTopology topology = (BaselineTopology)o;

        return nodeMap != null ? nodeMap.equals(topology.nodeMap) : topology.nodeMap == null;
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
}
