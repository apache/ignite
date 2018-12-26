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
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemPrefix;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemVer;
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
    private DistributedMetaStorageImpl dms;

    /** */
    private DistributedMetaStorageHistoryItem[] locFullData;

    /** */
    public ReadOnlyDistributedMetaStorageBridge(DistributedMetaStorageImpl dms) {
        this.dms = dms;
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String globalKey) throws IgniteCheckedException {
        int idx = Arrays.binarySearch(
            locFullData,
            new DistributedMetaStorageHistoryItem(globalKey, null),
            HISTORY_ITEM_KEY_COMPARATOR
        );

        if (idx >= 0)
            return unmarshal(locFullData[idx].valBytes);

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

    /** */
    public void readInitialData(
        ReadOnlyMetastorage metastorage,
        StartupExtras startupExtras
    ) throws IgniteCheckedException {
        if (metastorage.getData(cleanupGuardKey()) != null) {
            startupExtras.clearLocData = true;

            startupExtras.verToSnd = dms.ver = DistributedMetaStorageVersion.INITIAL_VERSION;
        }
        else {
            DistributedMetaStorageVersion storedVer =
                (DistributedMetaStorageVersion)metastorage.read(historyVersionKey());

            if (storedVer == null) {
                startupExtras.verToSnd = dms.ver = DistributedMetaStorageVersion.INITIAL_VERSION;

                startupExtras.locFullData = EMPTY_ARRAY;
            }
            else {
                startupExtras.verToSnd = dms.ver = storedVer;

                Serializable guard = metastorage.read(historyGuardKey(storedVer.id));

                if (guard != null) {
                    // New value is already known, but listeners may not have been invoked.
                    DistributedMetaStorageHistoryItem histItem =
                        (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id));

                    assert histItem != null;

                    startupExtras.deferredUpdates.add(histItem);
                }
                else {
                    guard = metastorage.read(historyGuardKey(storedVer.id + 1));

                    if (guard != null) {
                        DistributedMetaStorageHistoryItem histItem =
                            (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id + 1));

                        if (histItem != null) {
                            startupExtras.verToSnd = storedVer.nextVersion(histItem);

                            startupExtras.deferredUpdates.add(histItem);
                        }
                    }
                    else {
                        DistributedMetaStorageHistoryItem histItem =
                            (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id));

                        if (histItem != null) {
                            byte[] valBytes = metastorage.getData(localKey(histItem.key));

                            if (!Arrays.equals(valBytes, histItem.valBytes))
                                startupExtras.firstToWrite = histItem;
                        }
                    }
                }

                List<DistributedMetaStorageHistoryItem> locFullDataList = new ArrayList<>();

                DistributedMetaStorageHistoryItem firstToWrite = startupExtras.firstToWrite;

                boolean[] ftwWritten = {false};

                metastorage.iterate(
                    localKeyPrefix(),
                    (key, val) -> {
                        String globalKey = globalKey(key);

                        if (firstToWrite != null && firstToWrite.key.equals(globalKey)) {
                            if (firstToWrite.valBytes != null)
                                locFullDataList.add(firstToWrite);

                            ftwWritten[0] = true;
                        }
                        else if (firstToWrite != null && ftwWritten[0] && firstToWrite.key.compareTo(globalKey) < 0) {
                            if (firstToWrite.valBytes != null)
                                locFullDataList.add(firstToWrite);

                            locFullDataList.add(new DistributedMetaStorageHistoryItem(globalKey, (byte[])val));

                            ftwWritten[0] = true;
                        }
                        else
                            locFullDataList.add(new DistributedMetaStorageHistoryItem(globalKey, (byte[])val));
                    },
                    false
                );

                if (firstToWrite != null && !ftwWritten[0])
                    locFullDataList.add(new DistributedMetaStorageHistoryItem(firstToWrite.key, firstToWrite.valBytes));

                locFullData = startupExtras.locFullData = locFullDataList.toArray(EMPTY_ARRAY);

                metastorage.iterate(
                    historyItemPrefix(),
                    (key, val) -> dms.addToHistoryCache(historyItemVer(key), (DistributedMetaStorageHistoryItem)val),
                    true
                );
            }
        }
    }
}
