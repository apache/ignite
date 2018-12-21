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
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.COMMON_KEY_PREFIX;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyVersionKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKeyPrefix;

/** */
class WritableDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
    /** */
    private static final byte[] DUMMY_VALUE = {};

    /** */
    private final DistributedMetaStorageImpl dms;

    /** */
    private final ReadWriteMetastorage metastorage;

    /** */
    public WritableDistributedMetaStorageBridge(DistributedMetaStorageImpl dms, ReadWriteMetastorage metastorage) {
        this.dms = dms;
        this.metastorage = metastorage;
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String globalKey) throws IgniteCheckedException {
        return metastorage.read(localKey(globalKey));
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException {
        metastorage.iterate(
            localKeyPrefix() + globalKeyPrefix,
            cb,
            unmarshal
        );
    }

    /** {@inheritDoc} */
    @Override public void write(String globalKey, @Nullable byte[] valBytes) throws IgniteCheckedException {
        if (valBytes == null)
            metastorage.remove(localKey(globalKey));
        else
            metastorage.putData(localKey(globalKey), valBytes);
    }

    /** {@inheritDoc} */
    @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem, Serializable val) throws IgniteCheckedException {
        String histGuardKey = historyGuardKey(dms.ver + 1);

        metastorage.write(histGuardKey, DUMMY_VALUE);

        metastorage.write(historyItemKey(dms.ver + 1), histItem);

        ++dms.ver;

        metastorage.write(historyVersionKey(), dms.ver);

        dms.notifyListeners(histItem.key, val);

        metastorage.remove(histGuardKey);
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) throws IgniteCheckedException {
        metastorage.remove(historyItemKey(ver));
    }

    /** */
    public void restore(StartupExtras startupExtras) throws IgniteCheckedException {
        assert startupExtras != null;

        if (startupExtras.clearLocData || startupExtras.fullNodeData != null) {
            String cleanupKey = cleanupKey();

            metastorage.putData(cleanupKey, DUMMY_VALUE);

            if (startupExtras.clearLocData) {
                Set<String> allKeys = new HashSet<>();

                metastorage.iterate(COMMON_KEY_PREFIX, (key, val) -> allKeys.add(key), false);

                for (String key : allKeys)
                    metastorage.remove(key);
            }

            if (startupExtras.fullNodeData != null) {
                DistributedMetaStorageNodeData fullNodeData = startupExtras.fullNodeData;

                dms.ver = fullNodeData.ver;

                dms.clearHistoryCache();

                for (DistributedMetaStorageHistoryItem item : fullNodeData.fullData)
                    metastorage.putData(item.key, item.valBytes);

                for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                    DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                    long histItemVer = dms.ver + i + 1 - len;

                    metastorage.write(historyItemKey(histItemVer), histItem);

                    dms.addToHistoryCache(histItemVer, histItem);
                }

                metastorage.write(historyVersionKey(), dms.ver);

                for (DistributedMetastorageLifecycleListener lsnr : dms.subscrProcessor.getGlobalMetastorageSubscribers())
                    lsnr.onReInit(dms);
            }

            metastorage.remove(cleanupKey);
        }

        Long storedVer = (Long)metastorage.read(historyVersionKey());

        if (storedVer == null)
            metastorage.write(historyVersionKey(), 0L);
        else {
            Serializable guard = metastorage.read(historyGuardKey(dms.ver + 1));

            if (guard != null) {
                DistributedMetaStorageHistoryItem histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(dms.ver + 1));

                if (histItem == null)
                    metastorage.remove(historyGuardKey(dms.ver + 1));
            }
        }
    }
}
