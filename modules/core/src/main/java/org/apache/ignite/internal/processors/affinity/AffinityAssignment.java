/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
