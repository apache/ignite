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

package org.apache.ignite.internal.processors.affinity;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.BitSetIntSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class HistoryAffinityAssignment implements AffinityAssignment {
    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final List<List<ClusterNode>> assignment;

    /** */
    private final List<List<ClusterNode>> idealAssignment;

    /** */
    private final ClusterNode[] nodes;

    /** Assignments are stored as sequences of indexes in nodes array. */
    private final int[] parts;

    /** */
    private final Map<Integer, int[]> idealAssignmentDiff;

    /**
     * @param assign Assignment.
     */
    public HistoryAffinityAssignment(GridAffinityAssignment assign) {
        topVer = assign.topologyVersion();

        int tmp = assign.idealAssignment().get(0).size();

        int cpys = tmp == 1 ? 2 : tmp; // Special case for late affinity with zero backups.

        if (IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION || cpys > IGNITE_AFFINITY_BACKUPS_THRESHOLD) {
            assignment = Collections.unmodifiableList(assign.assignment());

            idealAssignment = Collections.unmodifiableList(assign.idealAssignment());

            nodes = null;

            parts = null;

            idealAssignmentDiff = null;

            return;
        }

        List<List<ClusterNode>> assignment = assign.assignment();
        List<List<ClusterNode>> idealAssignment = assign.idealAssignment();

        int partsCnt = assignment.size();

        parts = new int[partsCnt * cpys];

        Map<ClusterNode, Integer> orderMap = new HashMap<>();

        int order = 1; // Zero order is reserved for empty slot case.

        idealAssignmentDiff = new HashMap<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);
            List<ClusterNode> nodes0 = idealAssignment.get(p);

            for (int i = 0; i < nodes.size(); i++) {
                ClusterNode node = nodes.get(i);

                Integer nodeOrder = orderMap.get(node);

                if (nodeOrder == null)
                    orderMap.put(node, (nodeOrder = order++));

                parts[p * cpys + i] = nodeOrder;
            }

            if (!nodes.equals(nodes0)) {
                int[] idx = new int[nodes0.size()];

                idealAssignmentDiff.put(p, idx);

                for (int i = 0; i < nodes0.size(); i++) {
                    ClusterNode node = nodes0.get(i);

                    Integer nodeOrder = orderMap.get(node);

                    if (nodeOrder == null)
                        orderMap.put(node, (nodeOrder = order++));

                    idx[i] = nodeOrder;
                }
            }
        }

        // Rewrite according to order.
        nodes = orderMap.keySet().stream().toArray(ClusterNode[]::new);

        Arrays.sort(nodes, (o1, o2) -> orderMap.get(o1).compareTo(orderMap.get(o2)));

        this.assignment = new AbstractList<List<ClusterNode>>() {
            @Override public List<ClusterNode> get(int idx) {
                return partitionNodes(idx, false, cpys);
            }

            @Override public int size() {
                return partsCnt;
            }
        };

        this.idealAssignment = new AbstractList<List<ClusterNode>>() {
            @Override public List<ClusterNode> get(int idx) {
                return partitionNodes(idx, true, cpys);
            }

            @Override public int size() {
                return partsCnt;
            }
        };
    }

    /**
     * @param p Partion.
     * @param ideal {@code True} for ideal assignment.
     */
    private List<ClusterNode> partitionNodes(int p, boolean ideal, int cpys) {
        int[] order = idealAssignmentDiff.get(p);

        if (ideal && order != null) {
            List<ClusterNode> ret = new ArrayList<>(order.length);

            for (int i = 0; i < order.length; i++)
                ret.add(nodes[order[i] - 1]);

            return ret;
        }

        List<ClusterNode> ret = new ArrayList<>(cpys);

        for (int i = 0; i < cpys; i++) {
            int ord = parts[p * cpys + i];

            if (ord == 0)
                break;

            ret.add(nodes[ord - 1]);
        }

        return ret;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<List<ClusterNode>> idealAssignment() {
        return idealAssignment;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignment() {
        return assignment;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> get(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        return assignment.get(part);
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> getIds(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        if (IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION)
            return assignments2ids(assignment.get(part));
        else {
            List<ClusterNode> nodes = assignment.get(part);

            return nodes.size() > AffinityAssignment.IGNITE_AFFINITY_BACKUPS_THRESHOLD
                    ? assignments2ids(nodes)
                    : F.viewReadOnly(nodes, F.node2id());
        }
     }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> nodes() {
        Set<ClusterNode> res = new HashSet<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);

            if (!F.isEmpty(nodes))
                res.addAll(nodes);
        }

        return Collections.unmodifiableSet(res);
    }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> primaryPartitionNodes() {
        Set<ClusterNode> res = new HashSet<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);

            if (!F.isEmpty(nodes))
                res.add(nodes.get(0));
        }

        return Collections.unmodifiableSet(res);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> primaryPartitions(UUID nodeId) {
        Set<Integer> res = IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION ? new HashSet<>() : new BitSetIntSet();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);

            if (!F.isEmpty(nodes) && nodes.get(0).id().equals(nodeId))
                res.add(p);
        }

        return Collections.unmodifiableSet(res);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> backupPartitions(UUID nodeId) {
        Set<Integer> res = IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION ? new HashSet<>() : new BitSetIntSet();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);

            for (int i = 1; i < nodes.size(); i++) {
                ClusterNode node = nodes.get(i);

                if (node.id().equals(nodeId)) {
                    res.add(p);

                    break;
                }
            }
        }

        return Collections.unmodifiableSet(res);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return topVer.hashCode();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || !(o instanceof AffinityAssignment))
            return false;

        return topVer.equals(((AffinityAssignment)o).topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HistoryAffinityAssignment.class, this);
    }
}
