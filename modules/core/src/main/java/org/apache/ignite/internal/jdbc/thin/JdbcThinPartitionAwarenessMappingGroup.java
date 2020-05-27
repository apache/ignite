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

package org.apache.ignite.internal.jdbc.thin;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcProtocolContext;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRawBinarylizable;
import org.jetbrains.annotations.NotNull;

/**
 * Contains cache partitions distributions with corresponding set of cache ids.
 */
public class JdbcThinPartitionAwarenessMappingGroup implements JdbcRawBinarylizable {
    /** Set of cache Ids. */
    private final Set<Integer> cacheIds = new HashSet<>();

    /** Partitions mappings. */
    private final Map<UUID, Set<Integer>> partitionsMappings;

    /**
     * Constructor.
     */
    private JdbcThinPartitionAwarenessMappingGroup() {
        partitionsMappings = new HashMap<>();
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache id.
     * @param partitionsMappings Partitions mappings.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public JdbcThinPartitionAwarenessMappingGroup(@NotNull Integer cacheId, Map<UUID,
        @NotNull Set<Integer>> partitionsMappings) {
        cacheIds.add(cacheId);
        this.partitionsMappings = partitionsMappings;
    }

    /**
     * Tries to merge given partitions mappings and corresponding cache id with already existing mappings.
     *
     * @param cacheId Cache id.
     * @param partitionsMappings Partitions mappings.
     * @return True if merged successfully, false otherwise.
     */
    public boolean merge(int cacheId, Map<UUID, Set<Integer>> partitionsMappings) {
        if (cacheIds.contains(cacheId))
            return true;
        else if (this.partitionsMappings.equals(partitionsMappings)) {
            cacheIds.add(cacheId);

            return true;
        }
        else
            return false;
    }

    /**
     * Reverts partitions mappings from the form 'node id -> set of partition ids' to the form 'partition id -> node
     * id'. First form is more compact, so it's preferred in case of data transferring, second form is easier to use on
     * client side, cause we mainly retrieve data using partition is as key.
     *
     * @param partsCnt Partitions count.
     * @return Reverted form of partitions mapping: partition id -> node id.
     */
    public UUID[] revertMappings(int partsCnt) {
        if (partitionsMappings == null)
            return null;

        UUID[] reverted = new UUID[partsCnt];

        for (UUID nodeId : partitionsMappings.keySet()) {
            for (Integer partition : partitionsMappings.get(nodeId))
                reverted[partition] = nodeId;
        }

        return reverted;
    }

    /**
     * @return Cache ids.
     */
    public Set<Integer> cacheIds() {
        return Collections.unmodifiableSet(cacheIds);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    )
        throws BinaryObjectException {
        writer.writeInt(cacheIds.size());

        for (int cacheId : cacheIds)
            writer.writeInt(cacheId);

        writer.writeInt(partitionsMappings == null ? 0 : partitionsMappings.size());

        if (partitionsMappings != null) {
            for (UUID nodeId : partitionsMappings.keySet()) {
                writer.writeUuid(nodeId);

                Set<Integer> nodePartitions = partitionsMappings.get(nodeId);
                writer.writeInt(nodePartitions == null ? 0 : nodePartitions.size());

                if (nodePartitions != null) {
                    for (int partition : nodePartitions)
                        writer.writeInt(partition);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    )
        throws BinaryObjectException {
        // No-op.
    }

    /**
     * Reads <code>JdbcThinPartitionAwarenessMappingGroup</code> from provided reader.
     *
     * @param reader Binary object reader.
     * @param binCtx Binary context.
     * @return Deserialized instance of <code>JdbcThinPartitionAwarenessMappingGroup</code>.
     * @throws BinaryObjectException In case of error.
     */
    public static JdbcThinPartitionAwarenessMappingGroup readGroup(
        BinaryReaderExImpl reader,
        JdbcProtocolContext binCtx
    ) throws BinaryObjectException {
        JdbcThinPartitionAwarenessMappingGroup res = new JdbcThinPartitionAwarenessMappingGroup();

        int cacheIdsSize = reader.readInt();

        for (int i = 0; i < cacheIdsSize; i++)
            res.cacheIds.add(reader.readInt());

        int partitionsMappingsSize = reader.readInt();

        for (int i = 0; i < partitionsMappingsSize; i++) {
            UUID nodeId = reader.readUuid();

            int partPerNodeSize = reader.readInt();

            Set<Integer> partitionsPerNode = new HashSet<>();

            for (int j = 0; j < partPerNodeSize; j++)
                partitionsPerNode.add(reader.readInt());

            res.partitionsMappings.put(nodeId, partitionsPerNode);
        }

        return res;
    }
}
