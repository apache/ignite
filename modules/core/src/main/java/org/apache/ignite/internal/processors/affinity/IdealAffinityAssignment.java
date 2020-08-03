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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IdealAffinityAssignment {
    /** Topology version. */
    private final AffinityTopologyVersion topologyVersion;

    /** Assignment. */
    private final List<List<ClusterNode>> assignment;

    /** Ideal primaries. */
    private final Map<Object, Set<Integer>> idealPrimaries;

    /**
     * @param topologyVersion Topology version.
     * @param assignment Assignment.
     * @param idealPrimaries Ideal primaries.
     */
    private IdealAffinityAssignment(
        AffinityTopologyVersion topologyVersion,
        List<List<ClusterNode>> assignment,
        Map<Object, Set<Integer>> idealPrimaries
    ) {
        this.topologyVersion = topologyVersion;
        this.assignment = assignment;
        this.idealPrimaries = idealPrimaries;
    }

    /**
     * @param clusterNode Cluster node.
     */
    public Set<Integer> idealPrimaries(ClusterNode clusterNode) {
        Object consistentId = clusterNode.consistentId();

        assert consistentId != null : clusterNode;

        return idealPrimaries.getOrDefault(consistentId, Collections.emptySet());
    }

    /**
     * @param partition Partition.
     */
    public ClusterNode currentPrimary(int partition) {
        return assignment.get(partition).get(0);
    }

    /**
     *
     */
    public List<List<ClusterNode>> assignment() {
        return assignment;
    }

    /**
     *
     */
    public AffinityTopologyVersion topologyVersion() {
        return topologyVersion;
    }

    /**
     * @param nodes Nodes.
     * @param assignment Assignment.
     */
    public static Map<Object, Set<Integer>> calculatePrimaries(
        @Nullable List<ClusterNode> nodes,
        List<List<ClusterNode>> assignment
    ) {
        int nodesSize = nodes != null ? nodes.size() : 100;

        Map<Object, Set<Integer>> primaryPartitions = U.newHashMap(nodesSize);

        for (int size = assignment.size(), p = 0; p < size; p++) {
            List<ClusterNode> affinityNodes = assignment.get(p);

            if (!affinityNodes.isEmpty()) {
                ClusterNode primary = affinityNodes.get(0);

                primaryPartitions.computeIfAbsent(primary.consistentId(),
                    id -> new HashSet<>(U.capacity(size / nodesSize * 2))).add(p);
            }
        }

        return primaryPartitions;
    }

    /**
     * @param topVer Topology version.
     * @param assignment Assignment.
     */
    public static IdealAffinityAssignment create(AffinityTopologyVersion topVer, List<List<ClusterNode>> assignment) {
        return create(topVer, null, assignment);
    }

    /**
     * @param topVer Topology version.
     * @param nodes Nodes.
     * @param assignment Assignment.
     */
    public static IdealAffinityAssignment create(
        AffinityTopologyVersion topVer,
        @Nullable List<ClusterNode> nodes,
        List<List<ClusterNode>> assignment
    ) {
        return new IdealAffinityAssignment(topVer, assignment, calculatePrimaries(nodes, assignment));
    }

    /**
     * @param topVer Topology version.
     * @param assignment Assignment.
     * @param previousAssignment Previous assignment.
     */
    public static IdealAffinityAssignment createWithPreservedPrimaries(
        AffinityTopologyVersion topVer,
        List<List<ClusterNode>> assignment,
        IdealAffinityAssignment previousAssignment
    ) {
        return new IdealAffinityAssignment(topVer, assignment, previousAssignment.idealPrimaries);
    }
}
