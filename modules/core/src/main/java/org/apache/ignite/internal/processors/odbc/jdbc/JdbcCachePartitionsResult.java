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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

public class JdbcCachePartitionsResult extends JdbcResult {

    private Map<UUID, Set<Integer>> partitionsMap;

    /**
     * Default constructor.
     */
    public JdbcCachePartitionsResult() {
        super(CACHE_PARTITIONS);
    }

    public JdbcCachePartitionsResult(Map<UUID, Set<Integer>> partitionsMap) {
        super(CACHE_PARTITIONS);

        this.partitionsMap = partitionsMap;
    }

    public Map<UUID, Set<Integer>> getPartitionsMap() {
        return partitionsMap;
    }

    @Override
    public void writeBinary(BinaryWriterExImpl writer, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.writeBinary(writer, ver);
        writer.writeInt(partitionsMap == null ? 0 : partitionsMap.size());

        if (partitionsMap != null) {
            for (UUID nodeId: partitionsMap.keySet()) {
                writer.writeUuid(nodeId);

                Set<Integer> partitionsPerNode = partitionsMap.get(nodeId);

                writer.writeInt(partitionsPerNode == null ? 0 : partitionsPerNode.size());

                for (Integer partition: partitionsPerNode)
                    writer.writeInt(partition);
            }
        }
    }

    @Override
    public void readBinary(BinaryReaderExImpl reader, ClientListenerProtocolVersion ver) throws BinaryObjectException {
        super.readBinary(reader, ver);
        int partitionsMapSize = reader.readInt();

        partitionsMap = new HashMap<>(partitionsMapSize);

        for (int i = 0; i < partitionsMapSize; i++) {
            UUID nodeId = reader.readUuid();

            int partitionsPerNodeSize = reader.readInt();

            Set<Integer> partitionsPerNode = new HashSet<>(partitionsPerNodeSize);

            for (int j = 0; j < partitionsPerNodeSize; j++)
                partitionsPerNode.add(reader.readInt());

            partitionsMap.put(nodeId, partitionsPerNode);
        }
    }
}
