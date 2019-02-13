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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryItem.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.globalKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyVersionKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKeyPrefix;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.unmarshal;

/** */
class ReadOnlyDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
    /** */
    private static final Comparator<DistributedMetaStorageHistoryItem> HISTORY_ITEM_KEY_COMPARATOR =
        Comparator.comparing(item -> item.key);

    /** */
    private DistributedMetaStorageHistoryItem[] locFullData = EMPTY_ARRAY;

    /** */
    private DistributedMetaStorageVersion ver;

    /** */
    public ReadOnlyDistributedMetaStorageBridge() {
    }

    /** */
    public ReadOnlyDistributedMetaStorageBridge(
        DistributedMetaStorageHistoryItem[] locFullData
    ) {
        this.locFullData = locFullData;
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String globalKey, boolean unmarshal) throws IgniteCheckedException {
        int idx = Arrays.binarySearch(
            locFullData,
            new DistributedMetaStorageHistoryItem(globalKey, null),
            HISTORY_ITEM_KEY_COMPARATOR
        );

        if (idx >= 0)
            return unmarshal ? unmarshal(locFullData[idx].valBytes) : locFullData[idx].valBytes;

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
            new DistributedMetaStorageHistoryItem(globalKeyPrefix, null),
            HISTORY_ITEM_KEY_COMPARATOR
        );

        if (idx < 0)
            idx = -1 - idx;

        for (; idx < locFullData.length && locFullData[idx].key.startsWith(globalKeyPrefix); ++idx) {
            DistributedMetaStorageHistoryItem item = locFullData[idx];

            cb.accept(item.key, unmarshal ? unmarshal(item.valBytes) : item.valBytes);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(String globalKey, byte[] valBytes) {
        throw new UnsupportedOperationException("write");
    }

    /** {@inheritDoc} */
    @Override public void onUpdateMessage(
        DistributedMetaStorageHistoryItem histItem,
        Serializable val,
        boolean notifyListeners
    ) {
        throw new UnsupportedOperationException("onUpdateMessage");
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) {
        throw new UnsupportedOperationException("removeHistoryItem");
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorageHistoryItem[] localFullData() {
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
                (DistributedMetaStorageVersion)metastorage.read(historyVersionKey());

            if (storedVer == null) {
                ver = DistributedMetaStorageVersion.INITIAL_VERSION;

                locFullData = EMPTY_ARRAY;

                return ver;
            }
            else {
                ver = storedVer;

                DistributedMetaStorageHistoryItem histItem =
                    (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id + 1));

                DistributedMetaStorageHistoryItem[] firstToWrite = {null};

                if (histItem != null) {
                    ver = storedVer.nextVersion(histItem);

                    startupExtras.deferredUpdates.add(histItem);
                }
                else {
                    histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id));

                    if (histItem != null) {
                        byte[] valBytes = metastorage.readRaw(localKey(histItem.key));

                        if (!Arrays.equals(valBytes, histItem.valBytes))
                            firstToWrite[0] = histItem;
                    }
                }

                List<DistributedMetaStorageHistoryItem> locFullDataList = new ArrayList<>();

                metastorage.iterate(
                    localKeyPrefix(),
                    (key, val) -> {
                        String globalKey = globalKey(key);

                        if (firstToWrite[0] != null && firstToWrite[0].key.equals(globalKey)) {
                            if (firstToWrite[0].valBytes != null)
                                locFullDataList.add(firstToWrite[0]);

                            firstToWrite[0] = null;
                        }
                        else if (firstToWrite[0] != null && firstToWrite[0].key.compareTo(globalKey) < 0) {
                            if (firstToWrite[0].valBytes != null)
                                locFullDataList.add(firstToWrite[0]);

                            firstToWrite[0] = null;

                            locFullDataList.add(new DistributedMetaStorageHistoryItem(globalKey, (byte[])val));
                        }
                        else
                            locFullDataList.add(new DistributedMetaStorageHistoryItem(globalKey, (byte[])val));
                    },
                    false
                );

                if (firstToWrite[0] != null && firstToWrite[0].valBytes != null) {
                    locFullDataList.add(
                        new DistributedMetaStorageHistoryItem(firstToWrite[0].key, firstToWrite[0].valBytes)
                    );
                }

                locFullData = locFullDataList.toArray(EMPTY_ARRAY);

                return storedVer;
            }
        }
    }
}
