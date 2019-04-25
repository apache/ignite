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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;

/**
 * Cache partition mapping.
 */
public class ClientCachePartitionMapping {
    /** Partitions map for caches. */
    private final HashMap<UUID, Set<Integer>> partitionMap;

    /**
     * @param cacheId Cache ID.
     * @param assignment Affinity assignment.
     */
    public ClientCachePartitionMapping(int cacheId, AffinityAssignment assignment) {
        Set<ClusterNode> nodes = assignment.primaryPartitionNodes();

        partitionMap = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            UUID nodeId = node.id();
            Set<Integer> parts = assignment.primaryPartitions(nodeId);

            partitionMap.put(nodeId, parts);
        }
    }

    /**
     * Write mapping using binary writer.
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer) {
        writer.writeInt(partitionMap.size());

        for (HashMap.Entry<UUID, Set<Integer>> nodeParts: partitionMap.entrySet()) {
            UUID nodeUuid = nodeParts.getKey();
            Set<Integer> parts = nodeParts.getValue();

            writer.writeUuid(nodeUuid);

            writer.writeInt(parts.size());
            for (int part : parts)
                writer.writeInt(part);
        }
    }

    /**
     * Check if the mapping is compatible to another one.
     * @param another Another mapping.
     * @return True if compatible.
     */
    public boolean isCompatible(ClientCachePartitionMapping another) {
        return partitionMap.equals(another.partitionMap);
    }
}
