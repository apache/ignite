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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Cached affinity calculations.
 */
public interface AffinityAssignment {
    /**
     * Backups threshold to use HashMap instead of views on list.
     */
    int AFFINITY_BACKUPS_THRESHOLD = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.IGNITE_AFFINITY_BACKUPS_THRESHOLD,
        6
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
     * @return Nodes having parimary and backup assignments.
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
     * Number of backups of assignment.
     *
     * @return Number of backups.
     */
    public int backups();

    /**
     * Converts List of Cluster Nodes to Set of UUIDs.
     * @param assignment Source assignment.
     * @return List of deduplicated collections if ClusterNode's ids.
     */
    public default List<Collection<UUID>> assignments2ids(List<List<ClusterNode>> assignment) {
        List<Collection<UUID>> assignmentIds0 = new ArrayList<>(assignment.size());

        for (List<ClusterNode> assignmentPart : assignment) {
            Collection<UUID> partIds = new HashSet<>(assignmentPart.size());

            for (ClusterNode node : assignmentPart)
                partIds.add(node.id());

            assignmentIds0.add(partIds);
        }

        return assignmentIds0;
    }

}
