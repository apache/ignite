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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cached affinity calculations.
 */
public interface AffinityAssignment {
    /** Size threshold to use Map instead of List view. */
    int IGNITE_AFFINITY_BACKUPS_THRESHOLD = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.IGNITE_AFFINITY_BACKUPS_THRESHOLD,
        5
    );

    /** Disable memory affinity optimizations. */
    boolean IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION,
        false
    );

    /**
     * @return Affinity assignment computed by affinity function.
     */
    public List<List<ClusterNode>> idealAssignment();

    /**
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> assignment();

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion();

    /**
     * Get affinity nodes for partition.
     *
     * @param part Partition.
     * @return Affinity nodes.
     */
    public List<ClusterNode> get(int part);

    /**
     * Get affinity node IDs for partition.
     *
     * @param part Partition.
     * @return Affinity nodes IDs.
     */
    public Collection<UUID> getIds(int part);

    /**
     * @return Nodes having primary and backup assignments.
     */
    public Set<ClusterNode> nodes();

    /**
     * @return Nodes having primary partitions assignments.
     */
    public Set<ClusterNode> primaryPartitionNodes();

    /**
     * Get primary partitions for specified node ID.
     *
     * @param nodeId Node ID to get primary partitions for.
     * @return Primary partitions for specified node ID.
     */
    public Set<Integer> primaryPartitions(UUID nodeId);

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @return Backup partitions for specified node ID.
     */
    public Set<Integer> backupPartitions(UUID nodeId);

    /**
     * @return Set of partitions which primary is different to primary in ideal assignment.
     */
    public Set<Integer> partitionPrimariesDifferentToIdeal();

    /**
     * Converts List of Cluster Nodes to HashSet of UUIDs wrapped as unmodifiable collection.
     * @param assignmentPart Source assignment per partition.
     * @return List of deduplicated collections if ClusterNode's ids.
     */
    public default Collection<UUID> assignments2ids(List<ClusterNode> assignmentPart) {
        Collection<UUID> partIds = U.newHashSet(assignmentPart.size());

        for (ClusterNode node : assignmentPart)
            partIds.add(node.id());

        return Collections.unmodifiableCollection(partIds);
    }
}
