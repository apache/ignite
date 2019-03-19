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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRawBinarylizable;
import org.jetbrains.annotations.NotNull;

public class JdbcThinAffinityAwarenessMappingGroup implements JdbcRawBinarylizable {

    private final Set<Integer> cacheIds = new HashSet<>();

    private final Map<UUID, Set<Integer>> partitionsMappings;

    private JdbcThinAffinityAwarenessMappingGroup() {
        partitionsMappings = new HashMap<>();
    }

    public JdbcThinAffinityAwarenessMappingGroup(@NotNull Integer cacheId, Map<UUID,
        @NotNull Set<Integer>> partitionsMappings) {
        cacheIds.add(cacheId);
        this.partitionsMappings = partitionsMappings;
    }

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

    public Map<Integer, UUID> revertMappings() {
        if (partitionsMappings == null)
            return null;

        Map<Integer, UUID> reverted = new HashMap<>();

        for (UUID nodeId : partitionsMappings.keySet()) {
            for (Integer partition : partitionsMappings.get(nodeId)) {
                reverted.put(partition, nodeId);
            }
        }

        return reverted;
    }

    /**
     * @return Cache ids.
     */
    public Set<Integer> cacheIds() {
        return cacheIds;
    }

    @Override
    public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
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

    @Override
    public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        // No-op.
    }

    public static JdbcThinAffinityAwarenessMappingGroup readGroup(BinaryReaderExImpl reader,
        ClientListenerProtocolVersion ver) throws BinaryObjectException {
        JdbcThinAffinityAwarenessMappingGroup res = new JdbcThinAffinityAwarenessMappingGroup();

        int cacheIdsSize = reader.readInt();

        for (int i = 0; i < cacheIdsSize; i++)
            res.cacheIds.add(reader.readInt());

        int partitionsMappingsSize = reader.readInt();

        for (int i = 0; i < partitionsMappingsSize; i++) {
            UUID nodeId = reader.readUuid();

            int partitionPerNodeSize = reader.readInt();

            Set<Integer> partitionsPerNode = new HashSet<>();

            for (int j = 0; j < partitionPerNodeSize; j++)
                partitionsPerNode.add(reader.readInt());

            res.partitionsMappings.put(nodeId, partitionsPerNode);
        }

        return res;
    }
}
