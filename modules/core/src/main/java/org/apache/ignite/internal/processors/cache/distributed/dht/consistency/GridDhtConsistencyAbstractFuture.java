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

package org.apache.ignite.internal.processors.cache.distributed.dht.consistency;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridDhtConsistencyAbstractFuture<T> extends GridFutureAdapter<T> {
    /** Backup node's get futures. */
    protected final Collection<GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> backupFuts;

    /** Topology version. */
    protected final AffinityTopologyVersion topVer;

    /** Cctx. */
    protected final GridCacheContext cctx;

    /**
     *
     */
    protected GridDhtConsistencyAbstractFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext cctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        @Nullable String txLbl,
        @Nullable MvccSnapshot mvccSnapshot) {
        this.topVer = topVer;
        this.cctx = cctx;

        Map<ClusterNode, Collection<KeyCacheObject>> mappings = new HashMap<>();

        for (KeyCacheObject key : keys) {
            Collection<ClusterNode> nodes = cctx.affinity().backupsByKey(key, topVer);

            for (ClusterNode node : nodes) {
                mappings.computeIfAbsent(node, k -> new HashSet<>());

                mappings.get(node).add(key);
            }
        }

        backupFuts = new HashSet<>(mappings.size() - 1);

        for (Map.Entry<ClusterNode, Collection<KeyCacheObject>> entry : mappings.entrySet()) {
            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> backupFut = new GridPartitionedGetFuture<>(
                (GridCacheContext<KeyCacheObject, EntryGetResult>)cctx, // Todo
                entry.getValue(),
                readThrough,
                false, // Always backup.
                subjId,
                taskName,
                deserializeBinary,
                recovery,
                false, // This request goes to backup during consistency check procedure.
                expiryPlc,
                skipVals,
                true, // Version required to check consistency.
                true,
                txLbl,
                mvccSnapshot,
                entry.getKey()); // Explicit node instead of automated mapping.

            backupFut.listen(this::onResult);

            backupFuts.add(backupFut);
        }
    }

    /**
     *
     */
    public void init() {
        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : backupFuts)
            fut.init(topVer);
    }

    /**
     *
     */
    protected synchronized void onResult(IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut) {
        if (fut.error() == null)
            onResult();
        else
            onDone(fut.error());
    }

    /**
     *
     */
    protected abstract void onResult();
}
