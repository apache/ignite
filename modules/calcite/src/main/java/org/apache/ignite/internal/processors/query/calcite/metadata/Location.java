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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class Location {
    public static byte HAS_MOVING_PARTITIONS = 0x1;
    public static byte HAS_REPLICATED_CACHES = 0x2;
    public static byte HAS_PARTITIONED_CACHES = 0x4;
    public static byte PARTIALLY_REPLICATED = 0x8;
    public static byte DEDUPLICATED = 0x16;

    private final List<ClusterNode> nodes;
    private final List<List<ClusterNode>> assignments;
    private final byte flags;

    public Location(List<ClusterNode> nodes, List<List<ClusterNode>> assignments, byte flags) {
        this.nodes = nodes;
        this.assignments = assignments;
        this.flags = flags;
    }

    public List<ClusterNode> nodes() {
        return nodes;
    }

    public List<ClusterNode> nodes(int part) {
        return assignments.get(part % assignments.size());
    }

    public Location mergeWith(Location other) throws LocationMappingException {
        byte flags = (byte) (this.flags | other.flags);

        if ((flags & PARTIALLY_REPLICATED) == 0)
            return new Location(U.firstNotNull(nodes, other.nodes), mergeAssignments(other, null), flags);

        List<ClusterNode> nodes;

        if (this.nodes == null)
            nodes = other.nodes;
        else if (other.nodes == null)
            nodes = this.nodes;
        else
            nodes = Commons.intersect(this.nodes, other.nodes);

        if (nodes != null && nodes.isEmpty())
            throw new LocationMappingException("Failed to map fragment to location.");

        return new Location(nodes, mergeAssignments(other, nodes), flags);
    }

    private List<List<ClusterNode>> mergeAssignments(Location other, List<ClusterNode> nodes) throws LocationMappingException {
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

    public Location deduplicate() throws LocationMappingException {
        if (assignments == null || (flags & DEDUPLICATED) == DEDUPLICATED)
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

        return new Location(new ArrayList<>(nodes0), assignments0, (byte)(flags | DEDUPLICATED));
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
}
