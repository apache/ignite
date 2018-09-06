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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
    private final MvccCoordinator mvccCrd;

    /**
     * @param assign Assignment.
     */
    HistoryAffinityAssignment(GridAffinityAssignment assign) {
        this.topVer = assign.topologyVersion();
        this.assignment = assign.assignment();
        this.idealAssignment = assign.idealAssignment();
        this.mvccCrd = assign.mvccCoordinator();
    }

    /** {@inheritDoc} */
    @Override public MvccCoordinator mvccCoordinator() {
        return mvccCrd;
    }

    /** {@inheritDoc} */
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
    @Override public HashSet<UUID> getIds(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        List<ClusterNode> nodes = assignment.get(part);

        HashSet<UUID> ids = U.newHashSet(nodes.size());

        for (int i = 0; i < nodes.size(); i++)
            ids.add(nodes.get(i).id());

        return ids;
    }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> nodes() {
        Set<ClusterNode> res = new HashSet<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);

            if (!F.isEmpty(nodes))
                res.addAll(nodes);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> primaryPartitionNodes() {
        Set<ClusterNode> res = new HashSet<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);

            if (!F.isEmpty(nodes))
                res.add(nodes.get(0));
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> primaryPartitions(UUID nodeId) {
        Set<Integer> res = new HashSet<>();

        for (int p = 0; p < assignment.size(); p++) {
            List<ClusterNode> nodes = assignment.get(p);

            if (!F.isEmpty(nodes) && nodes.get(0).id().equals(nodeId))
                res.add(p);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> backupPartitions(UUID nodeId) {
        Set<Integer> res = new HashSet<>();

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

        return res;
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
