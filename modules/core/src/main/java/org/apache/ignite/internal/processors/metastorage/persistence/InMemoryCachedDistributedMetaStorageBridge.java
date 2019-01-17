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

        return unmarshal ? unmarshal(valBytes) : valBytes;
    }

    /** {@inheritDoc} */
    @Override public void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException {
        for (Map.Entry<String, byte[]> entry : cache.entrySet()) {
            if (entry.getKey().startsWith(globalKeyPrefix))
                cb.accept(entry.getKey(), unmarshal ? unmarshal(entry.getValue()) : entry.getValue());
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
    @Override public void onUpdateMessage(
        DistributedMetaStorageHistoryItem histItem,
        Serializable val,
        boolean notifyListeners
    ) throws IgniteCheckedException {
        dms.ver = dms.ver.nextVersion(histItem);

        if (notifyListeners)
            dms.notifyListeners(histItem.key, read(histItem.key, true), val);
    }

    /** {@inheritDoc} */
    @Override public void removeHistoryItem(long ver) {
    }

    /** {@inheritDoc} */
    @Override public DistributedMetaStorageHistoryItem[] localFullData() {
        return cache.entrySet().stream().map(
            entry -> new DistributedMetaStorageHistoryItem(entry.getKey(), entry.getValue())
        ).toArray(DistributedMetaStorageHistoryItem[]::new);
    }

    /** */
    public void restore(StartupExtras startupExtras) {
        if (startupExtras.fullNodeData != null) {
            DistributedMetaStorageClusterNodeData fullNodeData = startupExtras.fullNodeData;

            dms.ver = fullNodeData.ver;

            for (DistributedMetaStorageHistoryItem item : fullNodeData.fullData)
                cache.put(item.key, item.valBytes);

            for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                dms.addToHistoryCache(dms.ver.id + i + 1 - len, histItem);
            }
        }
    }
}
