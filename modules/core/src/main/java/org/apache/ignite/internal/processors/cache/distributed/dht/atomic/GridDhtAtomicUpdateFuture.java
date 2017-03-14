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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * DHT atomic cache backup update future.
 */
class GridDhtAtomicUpdateFuture extends GridDhtAtomicAbstractUpdateFuture {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future keys. */
    private final Collection<KeyCacheObject> keys;

    /** Entries with readers. */
    private Map<KeyCacheObject, GridDhtCacheEntry> nearReadersEntries;


    /**
     * @param cctx Cache context.
     * @param completionCb Callback to invoke when future is completed.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    GridDhtAtomicUpdateFuture(
        GridCacheContext cctx,
        CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicUpdateResponse> completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq,
        GridNearAtomicUpdateResponse updateRes
    ) {
        super(cctx,
            completionCb,
            writeVer,
            updateReq,
            updateRes);

        keys = new ArrayList<>(updateReq.size());
        mappings = U.newHashMap(updateReq.size());
    }

    /** {@inheritDoc} */
    @Override protected void addDhtKey(KeyCacheObject key, List<ClusterNode> dhtNodes) {
        keys.add(key);
    }

    /** {@inheritDoc} */
    @Override protected void addNearKey(KeyCacheObject key, Collection<UUID> readers) {
        keys.add(key);
    }

    /** {@inheritDoc} */
    @Override protected void addNearReaderEntry(GridDhtCacheEntry entry) {
        if (nearReadersEntries == null)
            nearReadersEntries = new HashMap<>();

        nearReadersEntries.put(entry.key(), entry);
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridDhtAtomicUpdateResponse updateRes) {
        if (log.isDebugEnabled())
            log.debug("Received DHT atomic update future result [nodeId=" + nodeId + ", updateRes=" + updateRes + ']');

        if (updateRes.error() != null)
            this.updateRes.addFailedKeys(updateRes.failedKeys(), updateRes.error());

        if (!F.isEmpty(updateRes.nearEvicted())) {
            for (KeyCacheObject key : updateRes.nearEvicted()) {
                GridDhtCacheEntry entry = nearReadersEntries.get(key);

                try {
                    entry.removeReader(nodeId, updateRes.messageId());
                }
                catch (GridCacheEntryRemovedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Entry with evicted reader was removed [entry=" + entry + ", err=" + e + ']');
                }
            }
        }

        registerResponse(nodeId);
    }

    /** {@inheritDoc} */
    @Override protected void addFailedKeys(GridNearAtomicUpdateResponse updateRes, Throwable err) {
        for (KeyCacheObject key : keys)
            updateRes.addFailedKey(key, err);
    }

    /** {@inheritDoc} */
    @Override protected GridDhtAtomicAbstractUpdateRequest createRequest(ClusterNode node,
        GridCacheVersion futVer,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer
    ) {
        return new GridDhtAtomicUpdateRequest(
            cctx.cacheId(),
            node.id(),
            futVer,
            writeVer,
            syncMode,
            topVer,
            false,
            updateReq.subjectId(),
            updateReq.taskNameHash(),
            null,
            cctx.deploymentEnabled(),
            updateReq.keepBinary(),
            updateReq.skipStore());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateFuture.class, this);
    }
}
