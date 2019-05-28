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

package org.apache.ignite.internal.processors.cache.distributed.near.consistency;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
public abstract class GridNearGetWithConsistencyAbstractFuture extends GridFutureAdapter<Map<KeyCacheObject, EntryGetResult>> {
    /** Affinity node's get futures. */
    protected final Map<ClusterNode, GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> futs;

    /** Topology version. */
    protected final AffinityTopologyVersion topVer;

    /**
     *
     */
    protected GridNearGetWithConsistencyAbstractFuture(
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
        MvccSnapshot mvccSnapshot) {
        this.topVer = topVer;

        Map<ClusterNode, Collection<KeyCacheObject>> mappings = new HashMap<>();

        for (KeyCacheObject key : keys) {
            Collection<ClusterNode> nodes = ctx.affinity().nodesByKey(key, topVer);

            for (ClusterNode node : nodes) {
                mappings.computeIfAbsent(node, k -> new HashSet<>());

                mappings.get(node).add(key);
            }
        }

        futs = new HashMap<>(mappings.size());

        for (Map.Entry<ClusterNode, Collection<KeyCacheObject>> mapping : mappings.entrySet()) {
            ClusterNode node = mapping.getKey();

            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut =
                new GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>(
                    ctx,
                    mapping.getValue(), // Keys.
                    readThrough,
                    false, // Local get required.
                    subjId,
                    taskName,
                    deserializeBinary,
                    recovery,
                    expiryPlc,
                    skipVals,
                    true, // Version required to check the consistency.
                    true, // Produces EntryGetResults.
                    txLbl,
                    mvccSnapshot) {
                    @Override protected boolean map(
                        KeyCacheObject key,
                        AffinityTopologyVersion topVer,
                        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> nodesToKeysMapping,
                        Map<ClusterNode, LinkedHashMap<KeyCacheObject, Boolean>> missedNodesToKeysMapping,
                        Map<KeyCacheObject, EntryGetResult> locVals) {
                        addNodeMapping(key, node, nodesToKeysMapping); // Explicit node instead of automated mapping.

                        assert nodesToKeysMapping.size() == 1; // One future per node.

                        return !node.isLocal();
                    }
                };

            fut.listen(this::onResult);

            futs.put(mapping.getKey(), fut);
        }
    }

    /**
     *
     */
    public GridNearGetWithConsistencyAbstractFuture init() {
        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values())
            fut.init(topVer);

        return this; // For chaining.
    }

    /**
     *
     */
    protected synchronized void onResult(IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> ignored) {
        if (isDone())
            return;

        for (IgniteInternalFuture fut : futs.values()) {
            if (!fut.isDone())
                return;

            if (fut.error() != null) {
                onDone(error());

                return;
            }
        }

        reduce();
    }

    /**
     * Reduces fut's results.
     */
    protected abstract void reduce();
}
