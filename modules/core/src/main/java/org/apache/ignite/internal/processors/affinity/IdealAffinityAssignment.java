package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;

public class IdealAffinityAssignment {

    private final List<List<ClusterNode>> assignment;

    private final Map<Object, List<Integer>> primaryPartitions;

    private IdealAffinityAssignment(List<List<ClusterNode>> assignment, Map<Object, List<Integer>> primaryPartitions) {
        this.assignment = assignment;
        this.primaryPartitions = primaryPartitions;
    }

    public List<Integer> primaryPartitions(ClusterNode clusterNode) {
        Object consistentId = clusterNode.consistentId();

        assert consistentId != null : clusterNode;

        return primaryPartitions.get(consistentId);
    }

    public List<List<ClusterNode>> assignment() {
        return assignment;
    }

    private static Map<Object, List<Integer>> calculatePrimaryPartitions(List<List<ClusterNode>> assignment) {
        Map<Object, List<Integer>> primaryPartitions = new HashMap<>();

        for (int size = assignment.size(), p = 0; p < size; p++) {
            List<ClusterNode> affinityNodes = assignment.get(p);

            if (!affinityNodes.isEmpty()) {
                ClusterNode primary = affinityNodes.get(0);

                primaryPartitions.computeIfAbsent(primary.consistentId(), id -> new ArrayList<>()).add(p);
            }
        }

        return primaryPartitions;
    }

    public static IdealAffinityAssignment create(List<List<ClusterNode>> assignment) {
        return new IdealAffinityAssignment(assignment, calculatePrimaryPartitions(assignment));
    }

    public static IdealAffinityAssignment createWithPreservedPrimaries(
        IdealAffinityAssignment previous,
        List<List<ClusterNode>> assignment
    ) {
        return new IdealAffinityAssignment(assignment, previous.primaryPartitions);
    }
}
