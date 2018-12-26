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
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryItem.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
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
    private DistributedMetaStorageImpl dms;

    /** */
    private final ReadOnlyMetastorage metastorage;

    /** */
    private DistributedMetaStorageHistoryItem firstToWrite;

    /** */
    public ReadOnlyDistributedMetaStorageBridge(DistributedMetaStorageImpl dms, ReadOnlyMetastorage metastorage) {
        this.dms = dms;
        this.metastorage = metastorage;
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String globalKey) throws IgniteCheckedException {
        if (firstToWrite != null && firstToWrite.key.equals(globalKey))
            return unmarshal(firstToWrite.valBytes);

        return metastorage.read(localKey(globalKey));
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException {
        if (firstToWrite == null || !firstToWrite.key.startsWith(globalKeyPrefix))
            metastorage.iterate(
                localKeyPrefix() + globalKeyPrefix,
                cb,
                unmarshal
            );
        else {
            Serializable firstToWriteVal = unmarshal ? unmarshal(firstToWrite.valBytes) : firstToWrite.valBytes;

            metastorage.iterate(
                localKeyPrefix() + globalKeyPrefix,
                (key, val) -> {
                    if (firstToWrite.key.equals(key)) {
                        if (firstToWriteVal != null)
                            cb.accept(key, firstToWriteVal);
                    }
                    else
                        cb.accept(key, val);
                },
                unmarshal
            );
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
    public void readInitialData(StartupExtras startupExtras) throws IgniteCheckedException {
        if (metastorage.getData(cleanupGuardKey()) != null) {
            startupExtras.clearLocData = true;

            startupExtras.verToSnd = dms.ver = 0;
        }
        else {
            Long storedVer = (Long)metastorage.read(historyVersionKey());

            if (storedVer == null) {
                startupExtras.verToSnd = dms.ver = 0;

                startupExtras.locFullData = EMPTY_ARRAY;
            }
            else {
                startupExtras.verToSnd = dms.ver = storedVer;

                Serializable guard = metastorage.read(historyGuardKey(dms.ver));

                if (guard != null) {
                    // New value is already known, but listeners may not have been invoked.
                    DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(dms.ver));

                    assert histItem != null;

                    startupExtras.deferredUpdates.add(histItem);
                }
                else {
                    guard = metastorage.read(historyGuardKey(dms.ver + 1));

                    if (guard != null) {
                        DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(dms.ver + 1));

                        if (histItem != null) {
                            ++startupExtras.verToSnd;

                            startupExtras.deferredUpdates.add(histItem);
                        }
                    }
                    else {
                        DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(dms.ver));

                        if (histItem != null) {
                            byte[] valBytes = metastorage.getData(localKey(histItem.key));

                            if (!Arrays.equals(valBytes, histItem.valBytes))
                                startupExtras.firstToWrite = histItem;
                        }
                    }
                }

                List<DistributedMetaStorageHistoryItem> locFullData = new ArrayList<>();

                firstToWrite = startupExtras.firstToWrite;

                metastorage.iterate(
                    localKeyPrefix(),
                    (key, val) -> {
                        if (firstToWrite != null && firstToWrite.key.equals(key)) {
                            if (firstToWrite.valBytes != null)
                                locFullData.add(firstToWrite);
                        }
                        else
                            locFullData.add(new DistributedMetaStorageHistoryItem(key, (byte[])val));
                    },
                    false
                );

                startupExtras.locFullData = locFullData.toArray(EMPTY_ARRAY);

                metastorage.iterate(
                    historyItemPrefix(),
                    (key, val) -> dms.addToHistoryCache(historyItemVer(key), (DistributedMetaStorageHistoryItem)val),
                    true
                );
            }
        }
    }
}
