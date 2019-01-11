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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryItem.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.COMMON_KEY_PREFIX;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.globalKey;
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
    @Override public Serializable read(String globalKey, boolean unmarshal) throws IgniteCheckedException {
        return unmarshal ? metastorage.read(localKey(globalKey)) : metastorage.readRaw(localKey(globalKey));
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException {
        metastorage.iterate(
            localKeyPrefix() + globalKeyPrefix,
            (key, val) -> cb.accept(globalKey(key), val),
            unmarshal
        );
    }

    /** {@inheritDoc} */
    @Override public void write(String globalKey, @Nullable byte[] valBytes) throws IgniteCheckedException {
        if (valBytes == null)
            metastorage.remove(localKey(globalKey));
        else
            metastorage.writeRaw(localKey(globalKey), valBytes);
    }

    /** {@inheritDoc} */
    @Override public void onUpdateMessage(
        DistributedMetaStorageHistoryItem histItem,
        Serializable val,
        boolean notifyListeners
    ) throws IgniteCheckedException {
        metastorage.write(historyItemKey(dms.ver.id + 1), histItem);

        dms.ver = dms.ver.nextVersion(histItem);

        metastorage.write(historyVersionKey(), dms.ver);

        if (notifyListeners)
            dms.notifyListeners(histItem.key, read(histItem.key, true), val);
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) throws IgniteCheckedException {
        metastorage.remove(historyItemKey(ver));
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorageHistoryItem[] localFullData() throws IgniteCheckedException {
        List<DistributedMetaStorageHistoryItem> locFullData = new ArrayList<>();

        metastorage.iterate(
            localKeyPrefix(),
            (key, val) -> locFullData.add(new DistributedMetaStorageHistoryItem(globalKey(key), (byte[])val)),
            false
        );

        return locFullData.toArray(EMPTY_ARRAY);
    }

    /** */
    public void restore(StartupExtras startupExtras) throws IgniteCheckedException {
        assert startupExtras != null;

        String cleanupGuardKey = cleanupGuardKey();

        if (metastorage.readRaw(cleanupGuardKey) != null || startupExtras.fullNodeData != null) {
            metastorage.writeRaw(cleanupGuardKey, DUMMY_VALUE);

            Set<String> allKeys = new HashSet<>();

            metastorage.iterate(COMMON_KEY_PREFIX, (key, val) -> allKeys.add(key), false);

            for (String key : allKeys)
                metastorage.remove(key);

            if (startupExtras.fullNodeData != null) {
                DistributedMetaStorageClusterNodeData fullNodeData = startupExtras.fullNodeData;

                dms.ver = fullNodeData.ver;

                dms.clearHistoryCache();

                for (DistributedMetaStorageHistoryItem item : fullNodeData.fullData)
                    metastorage.writeRaw(localKey(item.key), item.valBytes);

                for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                    DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                    long histItemVer = dms.ver.id + i + 1 - len;

                    metastorage.write(historyItemKey(histItemVer), histItem);

                    dms.addToHistoryCache(histItemVer, histItem);
                }

                metastorage.write(historyVersionKey(), dms.ver);
            }

            metastorage.remove(cleanupGuardKey);
        }

        DistributedMetaStorageVersion storedVer = (DistributedMetaStorageVersion)metastorage.read(historyVersionKey());

        if (storedVer == null)
            metastorage.write(historyVersionKey(), DistributedMetaStorageVersion.INITIAL_VERSION);
    }
}
