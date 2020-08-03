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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;

import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 * Checks data consistency. Checks that each affinity node's value equals other's. Prepares recovery data. Records
 * consistency violation event.
 */
public class GridNearReadRepairFuture extends GridNearReadRepairAbstractFuture {
    /**
     * Creates a new instance of GridNearReadRepairFuture.
     *
     * @param topVer Affinity topology version.
     * @param ctx Cache context.
     * @param keys Keys.
     * @param readThrough Read-through flag.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param recovery Partition recovery flag.
     * @param expiryPlc Expiry policy.
     * @param tx Transaction.
     */
    public GridNearReadRepairFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext ctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        IgniteCacheExpiryPolicy expiryPlc,
        IgniteInternalTx tx) {
        super(topVer,
            ctx,
            keys,
            readThrough,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            tx);
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        Map<KeyCacheObject, EntryGetResult> newestMap = new HashMap<>();
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.result().entrySet()) {
                KeyCacheObject key = entry.getKey();

                EntryGetResult candidate = entry.getValue();

                newestMap.putIfAbsent(key, candidate);

                EntryGetResult newest = newestMap.get(key);

                if (newest.version().compareTo(candidate.version()) < 0) {
                    newestMap.put(key, candidate);
                    fixedMap.put(key, candidate);
                }

                if (newest.version().compareTo(candidate.version()) > 0)
                    fixedMap.put(key, newest);
            }
        }

        recordConsistencyViolation(fixedMap);

        onDone(fixedMap);
    }

    /**
     * @param fixedRaw Fixed map.
     */
    private void recordConsistencyViolation(Map<KeyCacheObject, EntryGetResult> fixedRaw) {
        GridEventStorageManager evtMgr = ctx.gridEvents();

        if (!evtMgr.isRecordable(EVT_CONSISTENCY_VIOLATION))
            return;

        if (fixedRaw.isEmpty())
            return;

        Map<Object, Object> fixedMap = new HashMap<>();

        for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fixedRaw.entrySet()) {
            KeyCacheObject key = entry.getKey();
            CacheObject val = entry.getValue().value();

            ctx.addResult(
                fixedMap,
                key,
                val,
                false,
                false,
                deserializeBinary,
                false,
                null,
                0,
                0);
        }

        Map<UUID, Map<Object, Object>> originalMap = new HashMap<>();

        for (Map.Entry<ClusterNode, GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> pair : futs.entrySet()) {
            ClusterNode node = pair.getKey();

            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut = pair.getValue();

            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.result().entrySet()) {
                KeyCacheObject key = entry.getKey();

                if (fixedRaw.containsKey(key)) {
                    CacheObject val = entry.getValue().value();

                    originalMap.computeIfAbsent(node.id(), id -> new HashMap<>());

                    Map<Object, Object> map = originalMap.get(node.id());

                    ctx.addResult(
                        map,
                        key,
                        val,
                        false,
                        false,
                        deserializeBinary,
                        false,
                        null,
                        0,
                        0);
                }
            }
        }

        evtMgr.record(new CacheConsistencyViolationEvent<>(
            ctx.discovery().localNode(),
            "Consistency violation fixed.",
            originalMap,
            fixedMap));
    }
}
