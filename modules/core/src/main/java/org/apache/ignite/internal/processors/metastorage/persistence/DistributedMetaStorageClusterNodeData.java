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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Externalizable;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Distributed metastorage data that cluster sends to joining node. To reduce messages number, contains plain representation
 * of {@link DistributedMetaStorageVersion}, arrays of plain representations of Distributed MetaStorage's key-value pairs.
 * And wrapped {@link DistributedMetaStorageHistoryItem}s. The version and the full data holders are {@link Externalizable}s
 * persistent by {@link MetaStorage} with the dedicated code-generated serializers. Thus, we do not make them directly a {@link Message}.
 *
 * @see DmsDataWriter#write(String, byte[])
 * @see MetaStorage#write(String, Serializable)
 */
public class DistributedMetaStorageClusterNodeData implements Message {
    /** @see DistributedMetaStorageVersion#id */
    @Order(0)
    @GridToStringInclude
    long dVerId;

    /** @see DistributedMetaStorageVersion#hash */
    @Order(1)
    @GridToStringInclude
    long dVerHash;

    /** Array of the full data keys. */
    @GridToStringInclude
    @Order(2)
    @Nullable String[] fullDataKeys;

    /** Arrays of the full data bytes. */
    @GridToStringInclude
    @Order(3)
    @Nullable byte[][] fullDataValsBytes;

    /** Required updates for joining nodes or full available history of local node if the full data is not {@code null}. */
    @Order(4)
    @Nullable DistributedMetaStorageHistoryItemMessage[] hist;

    /** Additional updates. Makes sense only if the full data is not {@code null}. */
    @Order(5)
    @Nullable DistributedMetaStorageHistoryItemMessage[] updates;

    /** Empty constructor for serialization purposes. */
    public DistributedMetaStorageClusterNodeData() {
        // No-op.
    }

    /** */
    public DistributedMetaStorageClusterNodeData(
        DistributedMetaStorageVersion ver,
        @Nullable Map<String, byte[]> fullData,
        @Nullable DistributedMetaStorageHistoryItem[] hist,
        @Nullable DistributedMetaStorageHistoryItem[] updates
    ) {
        assert ver != null;
        assert fullData == null || hist != null;

        dVerId = ver.id;
        dVerHash = ver.hash;

        if (fullData != null) {
            fullDataKeys = new String[fullData.size()];
            fullDataValsBytes = new byte[fullData.size()][];

            int i = 0;

            for (var e : fullData.entrySet()) {
                fullDataKeys[i] = e.getKey();
                fullDataValsBytes[i] = e.getValue();

                ++i;
            }
        }

        this.hist = DistributedMetaStorageHistoryItemMessage.toMessages(hist);
        this.updates = DistributedMetaStorageHistoryItemMessage.toMessages(updates);
    }
}
