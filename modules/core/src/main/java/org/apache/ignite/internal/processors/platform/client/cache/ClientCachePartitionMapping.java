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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.jetbrains.annotations.Nullable;

/**
 * Cache partition mapping.
 */
public class ClientCachePartitionMapping {
    /** Primary partitions map for caches. */
    private final Map<UUID, Set<Integer>> primaryPartitionMap;

    /** Backups partitions map, located in current data center for caches. */
    @Nullable private final Map<UUID, Set<Integer>> dcBackupPartitionMap;

    /**
     * @param primaryPartitionMap Primary partition mapping.
     * @param dcBackupPartitionMap Backup partition mapping, located in current data center.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClientCachePartitionMapping(
        Map<UUID, Set<Integer>> primaryPartitionMap,
        @Nullable Map<UUID, Set<Integer>> dcBackupPartitionMap
    ) {
        this.primaryPartitionMap = primaryPartitionMap;
        this.dcBackupPartitionMap = dcBackupPartitionMap;
    }

    /**
     * Write mapping using binary writer.
     * @param writer Writer.
     */
    public void write(ClientProtocolContext ctx, BinaryRawWriter writer) {
        writePartitionMap(writer, primaryPartitionMap);

        if (ctx.isFeatureSupported(ClientBitmaskFeature.DC_AWARE))
            writePartitionMap(writer, dcBackupPartitionMap);
    }

    /** */
    private static void writePartitionMap(BinaryRawWriter writer, @Nullable Map<UUID, Set<Integer>> partitionMap) {
        if (partitionMap == null) {
            writer.writeInt(0);

            return;
        }

        writer.writeInt(partitionMap.size());

        for (Map.Entry<UUID, Set<Integer>> nodeParts: partitionMap.entrySet()) {
            UUID nodeUuid = nodeParts.getKey();
            Set<Integer> parts = nodeParts.getValue();

            writer.writeUuid(nodeUuid);

            writer.writeInt(parts.size());
            for (int part : parts)
                writer.writeInt(part);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ClientCachePartitionMapping mapping = (ClientCachePartitionMapping)o;

        return Objects.equals(primaryPartitionMap, mapping.primaryPartitionMap)
            && Objects.equals(dcBackupPartitionMap, mapping.dcBackupPartitionMap);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(primaryPartitionMap, dcBackupPartitionMap);
    }
}
