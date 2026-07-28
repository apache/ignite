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

import java.io.Serializable;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Message wrap for {@link DistributedMetaStorageHistoryItem} which is a persistent {@link IgniteDataTransferObject} stored
 * by {@link MetaStorage} using the dedicated code-generated DTO-serializer.
 *
 * @see DmsDataWriter#write(String, byte[])
 * @see MetaStorage#write(String, Serializable)
 */
public class DistributedMetaStorageHistoryItemMessage implements Message {
    /** */
    @Order(0)
    String[] keys;

    /** */
    @Order(1)
    byte[][] valBytes;

    /** Empty constructor for serialization purposes. */
    public DistributedMetaStorageHistoryItemMessage() {
        // No-op.
    }

    /** */
    DistributedMetaStorageHistoryItemMessage(String[] keys, byte[][] valBytes) {
        this.keys = keys;
        this.valBytes = valBytes;
    }

    /** @return {@link Message} wraps array for {@link DistributedMetaStorageHistoryItem} array. */
    static @Nullable DistributedMetaStorageHistoryItemMessage[] of(@Nullable DistributedMetaStorageHistoryItem[] hist) {
        if (hist == null)
            return null;

        var res = new DistributedMetaStorageHistoryItemMessage[hist.length];

        for (int i = 0; i < hist.length; ++i)
            res[i] = new DistributedMetaStorageHistoryItemMessage(hist[i].keys, hist[i].valBytesArr);

        return res;
    }
}
