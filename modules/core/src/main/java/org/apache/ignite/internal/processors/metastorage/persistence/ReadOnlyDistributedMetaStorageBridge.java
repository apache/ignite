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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageKeyValuePair.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.globalKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.versionKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKeyPrefix;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.unmarshal;

/** */
class ReadOnlyDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
    /** */
    private static final Comparator<DistributedMetaStorageKeyValuePair> KEY_COMPARATOR =
        Comparator.comparing(item -> item.key);

    /** */
    private DistributedMetaStorageKeyValuePair[] locFullData = EMPTY_ARRAY;

    /** */
    private final JdkMarshaller marshaller;

    /** */
    private DistributedMetaStorageVersion ver;

    /** */
    public ReadOnlyDistributedMetaStorageBridge(JdkMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    /** */
    public ReadOnlyDistributedMetaStorageBridge(
        JdkMarshaller marshaller, DistributedMetaStorageKeyValuePair[] locFullData
    ) {
        this.locFullData = locFullData;
        this.marshaller = marshaller;
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String globalKey, boolean unmarshal) throws IgniteCheckedException {
        int idx = Arrays.binarySearch(
            locFullData,
            new DistributedMetaStorageKeyValuePair(globalKey, null),
            KEY_COMPARATOR
        );

        if (idx >= 0)
            return unmarshal ? unmarshal(marshaller, locFullData[idx].valBytes) : locFullData[idx].valBytes;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException {
        int idx = Arrays.binarySearch(
            locFullData,
            new DistributedMetaStorageKeyValuePair(globalKeyPrefix, null),
            KEY_COMPARATOR
        );

        if (idx < 0)
            idx = -1 - idx;

        for (; idx < locFullData.length && locFullData[idx].key.startsWith(globalKeyPrefix); ++idx) {
            DistributedMetaStorageKeyValuePair item = locFullData[idx];

            cb.accept(item.key, unmarshal ? unmarshal(marshaller, item.valBytes) : item.valBytes);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(String globalKey, byte[] valBytes) {
        throw new UnsupportedOperationException("write");
    }

    /** {@inheritDoc} */
    @Override public void onUpdateMessage(
        DistributedMetaStorageHistoryItem histItem
    ) {
        throw new UnsupportedOperationException("onUpdateMessage");
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) {
        throw new UnsupportedOperationException("removeHistoryItem");
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorageKeyValuePair[] localFullData() {
        return locFullData;
    }

    /** */
    public DistributedMetaStorageVersion version() {
        return ver;
    }

    /** */
    public DistributedMetaStorageVersion readInitialData(
        ReadOnlyMetastorage metastorage,
        StartupExtras startupExtras
    ) throws IgniteCheckedException {
        if (metastorage.readRaw(cleanupGuardKey()) != null) {
            ver = DistributedMetaStorageVersion.INITIAL_VERSION;

            locFullData = EMPTY_ARRAY;

            return ver;
        }
        else {
            DistributedMetaStorageVersion storedVer =
                (DistributedMetaStorageVersion)metastorage.read(versionKey());

            if (storedVer == null) {
                ver = DistributedMetaStorageVersion.INITIAL_VERSION;

                locFullData = EMPTY_ARRAY;

                return ver;
            }
            else {
                ver = storedVer;

                DistributedMetaStorageHistoryItem histItem =
                    (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id + 1));

                DistributedMetaStorageHistoryItem incompletedHistItem = null;

                if (histItem != null) {
                    ver = storedVer.nextVersion(histItem);

                    startupExtras.deferredUpdates.add(histItem);
                }
                else {
                    histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id));

                    if (histItem != null) {
                        boolean equal = true;

                        for (int i = 0, len = histItem.keys.length; i < len; i++) {
                            byte[] valBytes = metastorage.readRaw(localKey(histItem.keys[i]));

                            if (!Arrays.equals(valBytes, histItem.valBytesArray[i])) {
                                equal = false;

                                break;
                            }
                        }

                        if (!equal)
                            incompletedHistItem = histItem;
                    }
                }

                SortedMap<String, byte[]> locFullDataMap = new TreeMap<>();

                metastorage.iterate(
                    localKeyPrefix(),
                    (key, val) -> locFullDataMap.put(globalKey(key), (byte[])val),
                    false
                );

                if (incompletedHistItem != null) {
                    for (int i = 0, len = incompletedHistItem.keys.length; i < len; i++) {
                        String key = incompletedHistItem.keys[i];
                        byte[] valBytes = incompletedHistItem.valBytesArray[i];

                        if (valBytes == null)
                            locFullDataMap.remove(key);
                        else
                            locFullDataMap.put(key, valBytes);
                    }
                }

                locFullData = new DistributedMetaStorageKeyValuePair[locFullDataMap.size()];

                int i = 0;
                for (Map.Entry<String, byte[]> entry : locFullDataMap.entrySet()) {
                    locFullData[i] = new DistributedMetaStorageKeyValuePair(entry.getKey(), entry.getValue());

                    ++i;
                }

                return storedVer;
            }
        }
    }
}
