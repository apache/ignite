/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageKeyValuePair.EMPTY_ARRAY;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.COMMON_KEY_PREFIX;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.globalKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.versionKey;
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
        DistributedMetaStorageHistoryItem histItem
    ) throws IgniteCheckedException {
        metastorage.write(historyItemKey(dms.getVer().id + 1), histItem);

        dms.setVer(dms.getVer().nextVersion(histItem));

        metastorage.write(versionKey(), dms.getVer());
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) throws IgniteCheckedException {
        metastorage.remove(historyItemKey(ver));
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorageKeyValuePair[] localFullData() throws IgniteCheckedException {
        List<DistributedMetaStorageKeyValuePair> locFullData = new ArrayList<>();

        metastorage.iterate(
            localKeyPrefix(),
            (key, val) -> locFullData.add(new DistributedMetaStorageKeyValuePair(globalKey(key), (byte[])val)),
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

                dms.setVer(fullNodeData.ver);

                dms.clearHistoryCache();

                for (DistributedMetaStorageKeyValuePair item : fullNodeData.fullData)
                    metastorage.writeRaw(localKey(item.key), item.valBytes);

                for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                    DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                    long histItemVer = dms.getVer().id + i + 1 - len;

                    metastorage.write(historyItemKey(histItemVer), histItem);

                    dms.addToHistoryCache(histItemVer, histItem);
                }

                metastorage.write(versionKey(), dms.getVer());
            }

            metastorage.remove(cleanupGuardKey);
        }

        DistributedMetaStorageVersion storedVer = (DistributedMetaStorageVersion)metastorage.read(versionKey());

        if (storedVer == null)
            metastorage.write(versionKey(), DistributedMetaStorageVersion.INITIAL_VERSION);
    }
}
