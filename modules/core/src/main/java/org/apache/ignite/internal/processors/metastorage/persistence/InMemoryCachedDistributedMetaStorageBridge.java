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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.globalKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKeyPrefix;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.unmarshal;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.versionKey;

/** */
class InMemoryCachedDistributedMetaStorageBridge {
    /** */
    private final JdkMarshaller marshaller;

    /** */
    private final SortedMap<String, byte[]> cache = new TreeMap<>();

    /** */
    public InMemoryCachedDistributedMetaStorageBridge(JdkMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    /**
     * Get unmarshalled data by key.
     *
     * @param globalKey The key.
     * @return Value associated with the key.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public Serializable read(String globalKey) throws IgniteCheckedException {
        return unmarshal(marshaller, readMarshalled(globalKey));
    }

    /**
     * Get raw data by key.
     *
     * @param globalKey The key.
     * @return Value associated with the key.
     */
    public byte[] readMarshalled(String globalKey) {
        return cache.get(globalKey);
    }

    /**
     * Iterate over all values corresponding to the keys with given prefix. It is guaranteed that iteration will be
     * executed in ascending keys order.
     *
     * @param globalKeyPrefix Prefix for the keys that will be iterated.
     * @param cb Callback that will be applied to all {@code <key, value>} pairs.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb
    ) throws IgniteCheckedException {
        for (Map.Entry<String, byte[]> entry : cache.tailMap(globalKeyPrefix).entrySet()) {
            if (!entry.getKey().startsWith(globalKeyPrefix))
                break;

            cb.accept(entry.getKey(), unmarshal(marshaller, entry.getValue()));
        }
    }

    /**
     * Write data into storage.
     *
     * @param globalKey The key.
     * @param valBytes Value bytes.
     */
    public void write(String globalKey, @Nullable byte[] valBytes) {
        if (valBytes == null)
            cache.remove(globalKey);
        else
            cache.put(globalKey, valBytes);
    }

    /**
     * Returns all {@code <key, value>} pairs currently stored in distributed metastorage. Values are not unmarshalled.
     * All keys are sorted in ascending order.
     *
     * @return Array of all keys and values.
     */
    public DistributedMetaStorageKeyValuePair[] localFullData() {
        return cache.entrySet().stream().map(
            entry -> new DistributedMetaStorageKeyValuePair(entry.getKey(), entry.getValue())
        ).toArray(DistributedMetaStorageKeyValuePair[]::new);
    }

    /** */
    public void writeFullNodeData(DistributedMetaStorageClusterNodeData fullNodeData) {
        assert fullNodeData.fullData != null;

        cache.clear();

        for (DistributedMetaStorageKeyValuePair item : fullNodeData.fullData)
            cache.put(item.key, item.valBytes);
    }

    /** */
    public DistributedMetaStorageVersion readInitialData(
        ReadOnlyMetastorage metastorage
    ) throws IgniteCheckedException {
        if (metastorage.readRaw(cleanupGuardKey()) != null)
            return DistributedMetaStorageVersion.INITIAL_VERSION;

        DistributedMetaStorageVersion storedVer =
            (DistributedMetaStorageVersion)metastorage.read(versionKey());

        if (storedVer == null)
            return DistributedMetaStorageVersion.INITIAL_VERSION;
        else {
            DistributedMetaStorageVersion ver = storedVer;

            DistributedMetaStorageHistoryItem lastHistItem;

            DistributedMetaStorageHistoryItem histItem =
                (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id() + 1));

            if (histItem != null) {
                lastHistItem = histItem;

                ver = storedVer.nextVersion(histItem);
            }
            else
                lastHistItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id()));

            metastorage.iterate(
                localKeyPrefix(),
                (key, val) -> cache.put(globalKey(key), (byte[])val),
                false
            );

            // Last item rollover.
            if (lastHistItem != null) {
                for (int i = 0, len = lastHistItem.keys().length; i < len; i++) {
                    String key = lastHistItem.keys()[i];
                    byte[] valBytes = lastHistItem.valuesBytesArray()[i];

                    if (valBytes == null)
                        cache.remove(key);
                    else
                        cache.put(key, valBytes);
                }
            }

            return ver;
        }
    }
}
