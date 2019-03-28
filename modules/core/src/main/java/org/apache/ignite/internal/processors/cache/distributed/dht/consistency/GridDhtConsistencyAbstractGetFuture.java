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

/**
 *
 */
public abstract class GridDhtConsistencyAbstractGetFuture extends GridFutureAdapter<Map<KeyCacheObject, EntryGetResult>> {
    /** Backup node's get futures. */
    protected final Collection<GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> futs;

    /** Topology version. */
    protected final AffinityTopologyVersion topVer;

    /**
     *
     */
    protected GridDhtConsistencyAbstractGetFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext<KeyCacheObject, EntryGetResult> ctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        String txLbl,
        MvccSnapshot mvccSnapshot,
        boolean backups) {
        this.topVer = topVer;

        Map<ClusterNode, Collection<KeyCacheObject>> mappings = new HashMap<>();

        for (KeyCacheObject key : keys) {
            Collection<ClusterNode> nodes = backups ?
                ctx.affinity().backupsByKey(key, topVer):
                ctx.affinity().nodesByKey(key, topVer);

            for (ClusterNode node : nodes) {
                mappings.computeIfAbsent(node, k -> new HashSet<>());

                mappings.get(node).add(key);
            }
        }

        futs = new HashSet<>(mappings.size() - 1);

        for (Map.Entry<ClusterNode, Collection<KeyCacheObject>> mapping : mappings.entrySet()) {
            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut = new GridPartitionedGetFuture<>(
                ctx,
                mapping.getValue(),
                readThrough,
                false, // Local get.
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
                mapping.getKey()); // Explicit node instead of automated mapping.

            fut.listen(this::onResult);

            futs.add(fut);
        }
    }

    /**
     *
     */
    public void init() {
        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs)
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
