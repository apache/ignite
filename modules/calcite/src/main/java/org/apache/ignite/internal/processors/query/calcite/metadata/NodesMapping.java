/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class NodesMapping implements Serializable {
    public static final byte HAS_MOVING_PARTITIONS = 1;
    public static final byte HAS_REPLICATED_CACHES = 1 << 1;
    public static final byte HAS_PARTITIONED_CACHES = 1 << 2;
    public static final byte PARTIALLY_REPLICATED = 1 << 3;
    public static final byte DEDUPLICATED = 1 << 4;

    private final List<ClusterNode> nodes;
    private final List<List<ClusterNode>> assignments;
    private final byte flags;

    public NodesMapping(List<ClusterNode> nodes, List<List<ClusterNode>> assignments, byte flags) {
        this.nodes = nodes;
        this.assignments = assignments;
        this.flags = flags;
    }

    public List<ClusterNode> nodes() {
        return nodes;
    }

    public List<List<ClusterNode>> assignments() {
        return assignments;
    }

    public NodesMapping mergeWith(NodesMapping other) throws LocationMappingException {
        byte flags = (byte) (this.flags | other.flags);

        if ((flags & PARTIALLY_REPLICATED) == 0)
            return new NodesMapping(U.firstNotNull(nodes, other.nodes), mergeAssignments(other, null), flags);

        List<ClusterNode> nodes;

        if (this.nodes == null)
            nodes = other.nodes;
        else if (other.nodes == null)
            nodes = this.nodes;
        else
            nodes = Commons.intersect(this.nodes, other.nodes);

        if (nodes != null && nodes.isEmpty())
            throw new LocationMappingException("Failed to map fragment to location.");

        return new NodesMapping(nodes, mergeAssignments(other, nodes), flags);
    }

    public NodesMapping deduplicate() throws LocationMappingException {
        if (assignments == null || !excessive())
            return this;

        HashSet<ClusterNode> nodes0 = new HashSet<>();
        List<List<ClusterNode>> assignments0 = new ArrayList<>(assignments.size());

        for (List<ClusterNode> partNodes : assignments) {
            ClusterNode node = F.first(partNodes);

            if (node == null)
                throw new LocationMappingException("Failed to map fragment to location.");

            assignments0.add(Collections.singletonList(node));
            nodes0.add(node);
        }

        return new NodesMapping(new ArrayList<>(nodes0), assignments0, (byte)(flags | DEDUPLICATED));
    }

    public int[] partitions(ClusterNode node) {
        if (assignments == null)
            return null;

        GridIntList parts = new GridIntList(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            List<ClusterNode> assignment = assignments.get(i);
            if (Objects.equals(node, F.first(assignment)))
                parts.add(i);
        }

        return parts.array();
    }

    public boolean excessive() {
        return (flags & DEDUPLICATED) == 0;
    }

    public boolean hasMovingPartitions() {
        return (flags & HAS_MOVING_PARTITIONS) == HAS_MOVING_PARTITIONS;
    }

    public boolean hasReplicatedCaches() {
        return (flags & HAS_REPLICATED_CACHES) == HAS_REPLICATED_CACHES;
    }

    public boolean hasPartitionedCaches() {
        return (flags & HAS_PARTITIONED_CACHES) == HAS_PARTITIONED_CACHES;
    }

    public boolean partiallyReplicated() {
        return (flags & PARTIALLY_REPLICATED) == PARTIALLY_REPLICATED;
    }

    private List<List<ClusterNode>> mergeAssignments(NodesMapping other, List<ClusterNode> nodes) throws LocationMappingException {
        byte flags = (byte) (this.flags | other.flags); List<List<ClusterNode>> left = assignments, right = other.assignments;

        if (left == null && right == null)
            return null; // nothing to intersect;

        if (left == null || right == null || (flags & HAS_MOVING_PARTITIONS) == 0) {
            List<List<ClusterNode>> assignments = U.firstNotNull(left, right);

            if (nodes == null || (flags & PARTIALLY_REPLICATED) == 0)
                return assignments;

            List<List<ClusterNode>> assignments0 = new ArrayList<>(assignments.size());
            HashSet<ClusterNode> nodesSet = new HashSet<>(nodes);

            for (List<ClusterNode> partNodes : assignments) {
                List<ClusterNode> partNodes0 = new ArrayList<>(partNodes.size());

                for (ClusterNode partNode : partNodes) {
                    if (nodesSet.contains(partNode))
                        partNodes0.add(partNode);
                }

                if (partNodes0.isEmpty())
                    throw new LocationMappingException("Failed to map fragment to location.");

                assignments0.add(partNodes0);
            }

            return assignments0;
        }

        List<List<ClusterNode>> assignments = new ArrayList<>(left.size());
        HashSet<ClusterNode> nodesSet = nodes != null ? new HashSet<>(nodes) : null;

        for (int i = 0; i < left.size(); i++) {
            List<ClusterNode> leftNodes = left.get(i), partNodes = new ArrayList<>(leftNodes.size());
            HashSet<ClusterNode> rightNodesSet = new HashSet<>(right.get(i));

            for (ClusterNode partNode : leftNodes) {
                if (rightNodesSet.contains(partNode) && (nodesSet == null || nodesSet.contains(partNode)))
                    partNodes.add(partNode);
            }

            if (partNodes.isEmpty())
                throw new LocationMappingException("Failed to map fragment to location.");

            assignments.add(partNodes);
        }

        return assignments;
    }
}
