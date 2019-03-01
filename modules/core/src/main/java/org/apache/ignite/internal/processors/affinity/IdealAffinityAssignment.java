package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

public class IdealAffinityAssignment {
    private final AffinityTopologyVersion topologyVersion;

    private final List<List<ClusterNode>> assignment;

    private final int partitions;

    private final Map<Object, List<Integer>> idealPrimaries;

    private IdealAffinityAssignment(
        AffinityTopologyVersion topologyVersion,
        List<List<ClusterNode>> assignment,
        Map<Object, List<Integer>> idealPrimaries
    ) {
        this.topologyVersion = topologyVersion;
        this.assignment = assignment;
        this.partitions = assignment.size();
        this.idealPrimaries = idealPrimaries;
    }

    public List<Integer> idealPrimaries(ClusterNode clusterNode) {
        Object consistentId = clusterNode.consistentId();

        assert consistentId != null : clusterNode;

        return idealPrimaries.get(consistentId);
    }

    public ClusterNode currentPrimary(int partition) {
        return assignment.get(partition).get(0);
    }

    public List<List<ClusterNode>> assignment() {
        return assignment;
    }

    public AffinityTopologyVersion topologyVersion() {
        return topologyVersion;
    }

    private static Map<Object, List<Integer>> calculatePrimaries(
        @Nullable List<ClusterNode> nodes,
        List<List<ClusterNode>> assignment
    ) {
        int nodesSize = nodes != null ? nodes.size() : 100;

        Map<Object, List<Integer>> primaryPartitions = U.newHashMap(nodesSize);

        for (int size = assignment.size(), p = 0; p < size; p++) {
            List<ClusterNode> affinityNodes = assignment.get(p);

            if (!affinityNodes.isEmpty()) {
                ClusterNode primary = affinityNodes.get(0);

                primaryPartitions.computeIfAbsent(primary.consistentId(), id -> new ArrayList<>(size / nodesSize * 2)).add(p);
            }
        }

        return primaryPartitions;
    }

    public static IdealAffinityAssignment create(AffinityTopologyVersion topVer, List<List<ClusterNode>> assignment) {
        return create(topVer, null, assignment);
    }

    public static IdealAffinityAssignment create(
        AffinityTopologyVersion topVer,
        @Nullable List<ClusterNode> nodes,
        List<List<ClusterNode>> assignment
    ) {
        return new IdealAffinityAssignment(topVer, assignment, calculatePrimaries(nodes, assignment));
    }

    public static IdealAffinityAssignment createWithPreservedPrimaries(
        AffinityTopologyVersion topVer,
        List<List<ClusterNode>> assignment,
        IdealAffinityAssignment previousAssignment
    ) {
        return new IdealAffinityAssignment(topVer, assignment, previousAssignment.idealPrimaries);
    }
}
