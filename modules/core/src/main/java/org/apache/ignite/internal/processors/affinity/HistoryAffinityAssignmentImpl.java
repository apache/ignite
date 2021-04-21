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
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.ImmutableIntSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Heap-space optimized version of calculated affinity assignment.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class HistoryAffinityAssignmentImpl implements HistoryAffinityAssignment {
    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final List<List<ClusterNode>> assignment;

    /** */
    private final List<List<ClusterNode>> idealAssignment;

    /** */
    private final ClusterNode[] nodes;

    /** Ideal assignments are stored as sequences of indexes in nodes array. */
    private final char[] idealParts;

    /** Diff with ideal. */
    private final Map<Integer, char[]> assignmentDiff;

    /** Partition primaries different to ideal. */
    private final Set<Integer> partitionPrimariesDifferentToIdeal;

    /**
     * @param assign Assignment.
     * @param backups Backups.
     */
    public HistoryAffinityAssignmentImpl(AffinityAssignment assign, int backups) {
        topVer = assign.topologyVersion();

        partitionPrimariesDifferentToIdeal = assign.partitionPrimariesDifferentToIdeal();

        if (IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION || backups > IGNITE_AFFINITY_BACKUPS_THRESHOLD) {
            assignment = assign.assignment();

            idealAssignment = assign.idealAssignment();

            nodes = null;

            idealParts = null;

            assignmentDiff = null;

            return;
        }

        List<List<ClusterNode>> assignment = assign.assignment();
        List<List<ClusterNode>> idealAssignment = assign.idealAssignment();

        int min = Integer.MAX_VALUE;
        int max = 0;

        for (List<ClusterNode> nodes : idealAssignment) { // Estimate required size.
            int size = nodes.size();

            if (size > max)
                max = size;

            if (size < min)
                min = size;
        }

        if (max != min) {
            this.assignment = assign.assignment();

            this.idealAssignment = assign.idealAssignment();

            nodes = null;

            idealParts = null;

            assignmentDiff = null;

            return;
        }

        int cpys = max;

        boolean same = assignment == idealAssignment;

        int partsCnt = assignment.size();

        idealParts = new char[partsCnt * cpys];

        Map<ClusterNode, Character> orderMap = new HashMap<>();

        char order = 1; // Char type is used as unsigned short to avoid conversions.

        assignmentDiff = new HashMap<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> curr = assignment.get(p);
            List<ClusterNode> ideal = idealAssignment.get(p);

            for (int i = 0; i < ideal.size(); i++) {
                ClusterNode node = ideal.get(i);

                Character nodeOrder = orderMap.get(node);

                if (nodeOrder == null)
                    orderMap.put(node, (nodeOrder = order++));

                idealParts[p * cpys + i] = nodeOrder;
            }

            if (!same && !curr.equals(ideal)) {
                char[] idx = new char[curr.size()];

                assignmentDiff.put(p, idx);

                for (int i = 0; i < curr.size(); i++) {
                    ClusterNode node = curr.get(i);

                    Character nodeOrder = orderMap.get(node);

                    if (nodeOrder == null)
                        orderMap.put(node, (nodeOrder = order++));

                    idx[i] = nodeOrder;
                }
            }
        }

        // Fill array according to assigned order.
        nodes = orderMap.keySet().stream().toArray(ClusterNode[]::new);

        Arrays.sort(nodes, (o1, o2) -> orderMap.get(o1).compareTo(orderMap.get(o2)));

        this.idealAssignment = new AbstractList<List<ClusterNode>>() {
            @Override public List<ClusterNode> get(int idx) {
                return partitionNodes(idx, true, cpys);
            }

            @Override public int size() {
                return partsCnt;
            }
        };

        this.assignment = same ? this.idealAssignment : new AbstractList<List<ClusterNode>>() {
            @Override public List<ClusterNode> get(int idx) {
                return partitionNodes(idx, false, cpys);
            }

            @Override public int size() {
                return partsCnt;
            }
        };

        assert this.assignment.equals(assign.assignment()) : "new=" + this.assignment + ", old=" + assign.assignment();

        assert this.idealAssignment.equals(assign.idealAssignment()) :
            "new=" + this.idealAssignment + ", old=" + assign.idealAssignment();
    }

    /**
     * @param p Partion.
     * @param ideal {@code True} for ideal assignment.
     * @param cpys Copies.
     */
    private List<ClusterNode> partitionNodes(int p, boolean ideal, int cpys) {
        char[] order;

        if (!ideal && (order = assignmentDiff.get(p)) != null) {
            List<ClusterNode> ret = new ArrayList<>(order.length);

            for (int i = 0; i < order.length; i++)
                ret.add(nodes[order[i] - 1]);

            return ret;
        }

        List<ClusterNode> ret = new ArrayList<>(cpys);

        for (int i = 0; i < cpys; i++) {
            char ord = idealParts[p * cpys + i];

            if (ord == 0) // Zero
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

        return ImmutableIntSet.wrap(res);
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

        return ImmutableIntSet.wrap(res);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> partitionPrimariesDifferentToIdeal() {
        return Collections.unmodifiableSet(partitionPrimariesDifferentToIdeal);
    }

    /** {@inheritDoc} */
    @Override public boolean requiresHistoryCleanup() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public HistoryAffinityAssignment origin() {
        return this;
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
        return S.toString(HistoryAffinityAssignmentImpl.class, this);
    }
}
