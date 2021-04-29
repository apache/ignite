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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.ImmutableIntSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cached affinity calculations V2.
 * It supports adaptive usage of BitSets instead of HashSets.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class GridAffinityAssignmentV2 extends IgniteDataTransferObject implements AffinityAssignment {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Collection of calculated affinity nodes. */
    private List<List<ClusterNode>> assignment;

    /** Map of primary node partitions. */
    private Map<UUID, Set<Integer>> primary;

    /** Map of backup node partitions. */
    private Map<UUID, Set<Integer>> backup;

    /** Set of partitions which primary is different than in ideal assignment. */
    private Set<Integer> primariesDifferentToIdeal;

    /** Assignment node IDs */
    private transient volatile List<Collection<UUID>> assignmentIds;

    /** Nodes having primary or backup partition assignments. */
    private transient volatile Set<ClusterNode> nodes;

    /** Nodes having primary partitions assignments. */
    private transient volatile Set<ClusterNode> primaryPartsNodes;

    /** */
    private transient List<List<ClusterNode>> idealAssignment;

    /**
     * Default constructor for deserialization.
     */
    public GridAffinityAssignmentV2() {
        //No-op
    }

    /**
     * Constructs cached affinity calculations item.
     *
     * @param topVer Topology version.
     */
    GridAffinityAssignmentV2(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
        primary = Collections.emptyMap();
        backup = Collections.emptyMap();
    }

    /**
     * @param topVer Topology version.
     * @param assignment Assignment.
     * @param idealAssignment Ideal assignment.
     */
    public GridAffinityAssignmentV2(AffinityTopologyVersion topVer,
        List<List<ClusterNode>> assignment,
        List<List<ClusterNode>> idealAssignment
    ) {
        assert topVer != null;
        assert assignment != null;
        assert idealAssignment != null;

        this.topVer = topVer;
        this.assignment = Collections.unmodifiableList(assignment); // It's important to keep equal references.
        this.idealAssignment =
            idealAssignment.equals(assignment) ? this.assignment : Collections.unmodifiableList(idealAssignment);

        // Temporary mirrors with modifiable partition's collections.
        Map<UUID, Set<Integer>> tmpPrimary = new HashMap<>();
        Map<UUID, Set<Integer>> tmpBackup = new HashMap<>();
        Set<Integer> primariesDifferentToIdeal = new HashSet<>();
        boolean isPrimary;

        for (int partsCnt = assignment.size(), p = 0; p < partsCnt; p++) {
            isPrimary = true;

            List<ClusterNode> currOwners = assignment.get(p);

            for (ClusterNode node : currOwners) {
                UUID id = node.id();

                Map<UUID, Set<Integer>> tmp = isPrimary ? tmpPrimary : tmpBackup;

                /*
                    https://issues.apache.org/jira/browse/IGNITE-4554 BitSet performs better than HashSet at most cases.
                    However with 65k partition and high number of nodes (700+) BitSet is losing HashSet.
                    We need to replace it with sparse bitsets.
                 */
                tmp.computeIfAbsent(id, uuid ->
                    !IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION ? new BitSetIntSet() : new HashSet<>()
                ).add(p);

                isPrimary = false;
            }

            List<ClusterNode> idealOwners = p < idealAssignment.size() ? idealAssignment.get(p) : Collections.emptyList();

            ClusterNode curPrimary = !currOwners.isEmpty() ? currOwners.get(0) : null;
            ClusterNode idealPrimary = !idealOwners.isEmpty() ? idealOwners.get(0) : null;

            if (curPrimary != null && !curPrimary.equals(idealPrimary))
                primariesDifferentToIdeal.add(p);
        }

        primary = Collections.unmodifiableMap(tmpPrimary);
        backup = Collections.unmodifiableMap(tmpBackup);
        this.primariesDifferentToIdeal = Collections.unmodifiableSet(primariesDifferentToIdeal);
    }

    /**
     * @param topVer Topology version.
     * @param aff Assignment to copy from.
     */
    GridAffinityAssignmentV2(AffinityTopologyVersion topVer, GridAffinityAssignmentV2 aff) {
        this.topVer = topVer;

        assignment = aff.assignment;
        idealAssignment = aff.idealAssignment;
        primary = aff.primary;
        backup = aff.backup;
        primariesDifferentToIdeal = aff.primariesDifferentToIdeal;
    }

    /**
     * @return Unmodifiable ideal affinity assignment computed by affinity function.
     */
    @Override public List<List<ClusterNode>> idealAssignment() {
        return idealAssignment;
    }

    /**
     * @return Unmodifiable affinity assignment.
     */
    @Override public List<List<ClusterNode>> assignment() {
        return assignment;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Get affinity nodes for partition.
     *
     * @param part Partition.
     * @return Affinity nodes.
     */
    @Override public List<ClusterNode> get(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        return assignment.get(part);
    }

    /**
     * Get affinity node IDs for partition as unmodifiable collection.
     * Depending on AFFINITY_BACKUPS_THRESHOLD we returned newly allocated HashSet or view on List.
     * @param part Partition.
     * @return Affinity nodes IDs.
     */
    @Override public Collection<UUID> getIds(int part) {
        assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
            " [part=" + part + ", partitions=" + assignment.size() + ']';

        if (IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION)
            return getOrCreateAssignmentsIds(part);
        else {
            List<ClusterNode> nodes = assignment.get(part);

            return nodes.size() > GridAffinityAssignmentV2.IGNITE_AFFINITY_BACKUPS_THRESHOLD
                    ? getOrCreateAssignmentsIds(part)
                    : F.viewReadOnly(nodes, F.node2id());
        }
    }

    /**
     *
     * @param part Partition ID.
     * @return Collection of UUIDs.
     */
    private Collection<UUID> getOrCreateAssignmentsIds(int part) {
        List<Collection<UUID>> assignmentIds0 = assignmentIds;

        if (assignmentIds0 == null) {
            assignmentIds0 = new ArrayList<>(assignment.size());

            for (List<ClusterNode> assignmentPart : assignment)
                assignmentIds0.add(assignments2ids(assignmentPart));

            assignmentIds = assignmentIds0;
        }

        return assignmentIds0.get(part);
    }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> nodes() {
        Set<ClusterNode> res = nodes;

        if (res == null) {
            res = new HashSet<>();

            for (int p = 0; p < assignment.size(); p++) {
                List<ClusterNode> nodes = assignment.get(p);

                if (!nodes.isEmpty())
                    res.addAll(nodes);
            }

            res = Collections.unmodifiableSet(res);

            this.nodes = res;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Set<ClusterNode> primaryPartitionNodes() {
        Set<ClusterNode> res = primaryPartsNodes;

        if (res == null) {
            res = new HashSet<>();

            for (int p = 0; p < assignment.size(); p++) {
                List<ClusterNode> nodes = assignment.get(p);

                if (!nodes.isEmpty())
                    res.add(nodes.get(0));
            }

            res = Collections.unmodifiableSet(res);

            primaryPartsNodes = res;
        }

        return res;
    }

    /**
     * Get primary partitions for specified node ID.
     *
     * @param nodeId Node ID to get primary partitions for.
     * @return Primary partitions for specified node ID.
     */
    @Override public Set<Integer> primaryPartitions(UUID nodeId) {
        Set<Integer> set = primary.get(nodeId);

        return set == null ? ImmutableIntSet.emptySet() : ImmutableIntSet.wrap(set);
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @return Backup partitions for specified node ID.
     */
    @Override public Set<Integer> backupPartitions(UUID nodeId) {
        Set<Integer> set = backup.get(nodeId);

        return set == null ? ImmutableIntSet.emptySet() : ImmutableIntSet.wrap(set);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> partitionPrimariesDifferentToIdeal() {
        return Collections.unmodifiableSet(primariesDifferentToIdeal);
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

        if (!(o instanceof AffinityAssignment))
            return false;

        return topVer.equals(((AffinityAssignment)o).topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAffinityAssignmentV2.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(topVer);

        U.writeCollection(out, assignment);

        U.writeMap(out, primary);

        U.writeMap(out, backup);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        topVer = (AffinityTopologyVersion)in.readObject();

        assignment = U.readList(in);

        primary = U.readMap(in);

        backup = U.readMap(in);
    }
}
