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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Distributed metastorage data message that a joining node sends to a cluster. Contains unwrapped
 * {@link DistributedMetaStorageVersion} (to reduce the messages number) and {@link DistributedMetaStorageHistoryItemMessage}s.
 * They are a {@link Externalizable} and persistent by {@link MetaStorage} with the dedicated code-generated serializers.
 * Thus, we do not make them directly a {@link Message}.
 *
 * @see DmsDataWriter#write(String, byte[])
 * @see MetaStorage#write(String, Serializable)
 */
public class DistributedMetaStorageJoiningNodeData implements Message {
    /** Baseline topology id of node, {@code -1} if baseline topology is null. */
    @Order(0)
    int bltId;

    /** @see DistributedMetaStorageVersion#id */
    @Order(1)
    @GridToStringInclude
    long dVerId;

    /** @see DistributedMetaStorageVersion#hash */
    @Order(2)
    @GridToStringInclude
    long dVerHash;

    /** Available history of joining node. */
    @Order(3)
    @GridToStringInclude
    DistributedMetaStorageHistoryItemMessage[] hist;

    /** For serialization purposes. */
    public DistributedMetaStorageJoiningNodeData() {
        // No-op.
    }

    /** */
    public DistributedMetaStorageJoiningNodeData(
        int bltId,
        DistributedMetaStorageVersion ver,
        DistributedMetaStorageHistoryItem[] hist
    ) {
        assert ver != null;
        assert hist != null;

        this.bltId = bltId;

        dVerId = ver.id;
        dVerHash = ver.hash;

        this.hist = DistributedMetaStorageHistoryItemMessage.of(hist);
    }
}
