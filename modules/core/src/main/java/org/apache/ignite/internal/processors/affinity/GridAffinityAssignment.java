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

import java.io.Serializable;
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
import org.apache.ignite.internal.util.BitSetIntSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cached affinity calculations.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class GridAffinityAssignment implements AffinityAssignment, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Number of backups . */
    private final int backups;

    /** Collection of calculated affinity nodes. */
    private List<List<ClusterNode>> assignment;

    /** Map of primary node partitions. */
    private final Map<UUID, Set<Integer>> primary;

    /** Map of backup node partitions. */
    private final Map<UUID, Set<Integer>> backup;

    /** Assignment node IDs */
    private transient volatile List<Collection<UUID>> assignmentIds;

    /** Nodes having primary or backup partition assignments. */
    private transient volatile Set<ClusterNode> nodes;

    /** Nodes having primary partitions assignments. */
    private transient volatile Set<ClusterNode> primaryPartsNodes;

    /** */
    private transient List<List<ClusterNode>> idealAssignment;

    /**
     * Constructs cached affinity calculations item.
     *
     * @param topVer Topology version.
     */
    GridAffinityAssignment(AffinityTopologyVersion topVer, int backups) {
        this.topVer = topVer;
        primary = Collections.emptyMap();
        backup = Collections.emptyMap();
        this.backups = backups;
    }

    /**
     * @param topVer Topology version.
     * @param assignment Assignment.
     * @param idealAssignment Ideal assignment.
     */
    GridAffinityAssignment(AffinityTopologyVersion topVer,
        List<List<ClusterNode>> assignment,
        List<List<ClusterNode>> idealAssignment,
        int backups
    ) {
        assert topVer != null;
        assert assignment != null;
        assert idealAssignment != null;

        this.topVer = topVer;
        this.assignment = assignment;
        this.idealAssignment = idealAssignment.equals(assignment) ? assignment : idealAssignment;

        // Temporary mirrors with modifiable partition's collections.
        Map<UUID, Set<Integer>> tmpPrimary = new HashMap<>();
        Map<UUID, Set<Integer>> tmpBackup = new HashMap<>();
        boolean isPrimary;

        for (int partsCnt = assignment.size(), p = 0; p < partsCnt; p++) {
            isPrimary = true;

            for (ClusterNode node : assignment.get(p)) {
                UUID id = node.id();

                Map<UUID, Set<Integer>> tmp = isPrimary ? tmpPrimary : tmpBackup;

                tmp.computeIfAbsent(id, uuid -> new BitSetIntSet()).add(p);

                isPrimary =  false;
            }
        }

        primary = Collections.unmodifiableMap(tmpPrimary);
        backup = Collections.unmodifiableMap(tmpBackup);

        this.backups = backups;
    }

    /**
     * @param topVer Topology version.
     * @param aff Assignment to copy from.
     */
    GridAffinityAssignment(AffinityTopologyVersion topVer, GridAffinityAssignment aff, int backups) {
        this.topVer = topVer;

        assignment = aff.assignment;
        idealAssignment = aff.idealAssignment;
        primary = aff.primary;
        backup = aff.backup;

        this.backups = backups;
    }

    /**
     * @return Affinity assignment computed by affinity function.
     */
    @Override public List<List<ClusterNode>> idealAssignment() {
        return Collections.unmodifiableList(idealAssignment);
    }

    /**
     * @return Affinity assignment.
     */
    @Override public List<List<ClusterNode>> assignment() {
        return Collections.unmodifiableList(assignment);
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

        if (backups > AFFINITY_BACKUPS_THRESHOLD) {
            List<Collection<UUID>> assignmentIds0 = assignmentIds;

            if (assignmentIds0 == null) {
                assignmentIds0 = new ArrayList<>(assignment.size());

                for (List<ClusterNode> assignmentPart : assignment)
                    assignmentIds0.add(assignments2ids(assignmentPart));

                assignmentIds = assignmentIds0;
            }

            return assignmentIds0.get(part);
        } else
            return F.viewReadOnly(assignment.get(part), F.node2id());
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

            nodes = Collections.unmodifiableSet(res);
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

            primaryPartsNodes = Collections.unmodifiableSet(res);
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

        return set == null ? Collections.emptySet() : Collections.unmodifiableSet(set);
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @return Backup partitions for specified node ID.
     */
    @Override public Set<Integer> backupPartitions(UUID nodeId) {
        Set<Integer> set = backup.get(nodeId);

        return set == null ? Collections.emptySet() : Collections.unmodifiableSet(set);
    }

    /** {@inheritDoc} */
    @Override public int backups() {
        return backups;
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
        return S.toString(GridAffinityAssignment.class, this, super.toString());
    }
}
