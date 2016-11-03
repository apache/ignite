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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridDhtAtomicSingleUpdateFuture extends GridDhtAtomicAbstractUpdateFuture {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future keys. */
    private KeyCacheObject key;

    /** Entries with readers. */
    private GridDhtCacheEntry nearReaderEntry;

    /**
     * @param cctx Cache context.
     * @param completionCb Callback to invoke when future is completed.
     * @param writeVer Write version.
     * @param updateReq Update request.
     * @param updateRes Update response.
     */
    public GridDhtAtomicSingleUpdateFuture(
        GridCacheContext cctx,
        CI2<GridNearAtomicUpdateRequest, GridNearAtomicUpdateResponse> completionCb,
        GridCacheVersion writeVer,
        GridNearAtomicUpdateRequest updateReq,
        GridNearAtomicUpdateResponse updateRes
    ) {
        super(cctx, completionCb, writeVer, updateReq, updateRes, cctx.config().getBackups());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void addWriteEntry(GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr) {
        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        List<ClusterNode> dhtNodes = cctx.dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        assert key == null || key.equals(entry.key()) : key;

        key = entry.key();

        for (int i = 0; i < dhtNodes.size(); i++) {
            ClusterNode node = dhtNodes.get(i);

            UUID nodeId = node.id();

            if (!nodeId.equals(cctx.localNodeId())) {
                GridDhtAtomicUpdateRequest updateReq = mappings.get(nodeId);

                if (updateReq == null) {
                    updateReq = new GridDhtAtomicUpdateRequest(
                        cctx.cacheId(),
                        nodeId,
                        futVer,
                        writeVer,
                        syncMode,
                        topVer,
                        forceTransformBackups,
                        this.updateReq.subjectId(),
                        this.updateReq.taskNameHash(),
                        forceTransformBackups ? this.updateReq.invokeArguments() : null,
                        cctx.deploymentEnabled(),
                        this.updateReq.keepBinary(),
                        this.updateReq.skipStore());

                    mappings.put(nodeId, updateReq);
                }

                updateReq.addWriteValue(entry.key(),
                    val,
                    entryProcessor,
                    ttl,
                    conflictExpireTime,
                    conflictVer,
                    addPrevVal,
                    entry.partition(),
                    prevVal,
                    updateCntr);
            }
        }
    }

    /** {@inheritDoc} */
    public void addNearWriteEntries(Iterable<UUID> readers,
        GridDhtCacheEntry entry,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {
        CacheWriteSynchronizationMode syncMode = updateReq.writeSynchronizationMode();

        assert key == null || key.equals(entry.key()) : key;
        key = entry.key();

        AffinityTopologyVersion topVer = updateReq.topologyVersion();

        for (UUID nodeId : readers) {
            GridDhtAtomicUpdateRequest updateReq = mappings.get(nodeId);

            if (updateReq == null) {
                ClusterNode node = cctx.discovery().node(nodeId);

                // Node left the grid.
                if (node == null)
                    continue;

                updateReq = new GridDhtAtomicUpdateRequest(
                    cctx.cacheId(),
                    nodeId,
                    futVer,
                    writeVer,
                    syncMode,
                    topVer,
                    forceTransformBackups,
                    this.updateReq.subjectId(),
                    this.updateReq.taskNameHash(),
                    forceTransformBackups ? this.updateReq.invokeArguments() : null,
                    cctx.deploymentEnabled(),
                    this.updateReq.keepBinary(),
                    this.updateReq.skipStore());

                mappings.put(nodeId, updateReq);
            }

            nearReaderEntry = entry;

            updateReq.addNearWriteValue(entry.key(),
                val,
                entryProcessor,
                ttl,
                expireTime);
        }
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridDhtAtomicUpdateResponse updateRes) {
        if (log.isDebugEnabled())
            log.debug("Received DHT atomic update future result [nodeId=" + nodeId + ", updateRes=" + updateRes + ']');

        if (updateRes.error() != null)
            this.updateRes.addFailedKeys(updateRes.failedKeys(), updateRes.error());

        if (!F.isEmpty(updateRes.nearEvicted())) {
            try {
                nearReaderEntry.removeReader(nodeId, updateRes.messageId());
            }
            catch (GridCacheEntryRemovedException e) {
                if (log.isDebugEnabled())
                    log.debug("Entry with evicted reader was removed [entry=" + nearReaderEntry + ", err=" + e + ']');
            }
        }

        registerResponse(nodeId);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            cctx.mvcc().removeAtomicFuture(version());

            boolean suc = err == null;

            if (!suc)
                updateRes.addFailedKey(key, err);

            if (cntQryClsrs != null) {
                for (CI1<Boolean> clsr : cntQryClsrs)
                    clsr.apply(suc);
            }

            if (updateReq.writeSynchronizationMode() == FULL_SYNC)
                completionCb.apply(updateReq, updateRes);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicSingleUpdateFuture.class, this);
    }
}
