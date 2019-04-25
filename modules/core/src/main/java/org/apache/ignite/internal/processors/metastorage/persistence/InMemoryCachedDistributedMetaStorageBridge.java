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
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.unmarshal;

/** */
class InMemoryCachedDistributedMetaStorageBridge implements DistributedMetaStorageBridge {
    /** */
    private DistributedMetaStorageImpl dms;

    /** */
    private final Map<String, byte[]> cache = new ConcurrentSkipListMap<>();

    /** */
    public InMemoryCachedDistributedMetaStorageBridge(DistributedMetaStorageImpl dms) {
        this.dms = dms;
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String globalKey, boolean unmarshal) throws IgniteCheckedException {
        byte[] valBytes = cache.get(globalKey);

        return unmarshal ? unmarshal(dms.marshaller, valBytes) : valBytes;
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException {
        for (Map.Entry<String, byte[]> entry : cache.entrySet()) {
            if (entry.getKey().startsWith(globalKeyPrefix))
                cb.accept(entry.getKey(), unmarshal ? unmarshal(dms.marshaller, entry.getValue()) : entry.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void write(String globalKey, @Nullable byte[] valBytes) {
        if (valBytes == null)
            cache.remove(globalKey);
        else
            cache.put(globalKey, valBytes);
    }

    /** {@inheritDoc} */
    @Override public void onUpdateMessage(DistributedMetaStorageHistoryItem histItem) {
        dms.setVer(dms.getVer().nextVersion(histItem));
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) {
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorageKeyValuePair[] localFullData() {
        return cache.entrySet().stream().map(
            entry -> new DistributedMetaStorageKeyValuePair(entry.getKey(), entry.getValue())
        ).toArray(DistributedMetaStorageKeyValuePair[]::new);
    }

    /** */
    public void restore(StartupExtras startupExtras) {
        if (startupExtras.fullNodeData != null) {
            DistributedMetaStorageClusterNodeData fullNodeData = startupExtras.fullNodeData;

            dms.setVer(fullNodeData.ver);

            for (DistributedMetaStorageKeyValuePair item : fullNodeData.fullData)
                cache.put(item.key, item.valBytes);

            for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                dms.addToHistoryCache(dms.getVer().id + i + 1 - len, histItem);
            }
        }
    }
}
