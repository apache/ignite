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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

    /**
     * @param assign Assignment.
     */
    HistoryAffinityAssignment(AffinityAssignment assign) {
        topVer = assign.topologyVersion();
        assignment = assign.assignment();
        idealAssignment = assign.idealAssignment();
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> idealAssignment() {
        return Collections.unmodifiableList(idealAssignment);
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignment() {
        return Collections.unmodifiableList(assignment);
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
