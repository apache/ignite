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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;

import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 *
 */
public class GridConsistencyGetWithRecoveryFuture extends GridConsistencyAbstractGetFuture {
    /** Context. */
    GridCacheContext ctx;

    /**
     *
     */
    public GridConsistencyGetWithRecoveryFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext ctx,
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
        super(topVer,
            ctx,
            keys,
            readThrough,
            subjId,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            skipVals,
            txLbl,
            mvccSnapshot,
            false);

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override protected void onResult() {
        if (isDone())
            return;

        if (checkIsDone())
            onDone(checkAndFix());
    }

    /**
     *
     */
    private boolean checkIsDone() {
        for (IgniteInternalFuture fut : futs.values()) {
            if (!fut.isDone())
                return false;
        }

        return true;
    }

    /**
     * Returns latest (by version) entry for each key with consistency violation.
     */
    private Map<KeyCacheObject, EntryGetResult> checkAndFix() {
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

        return fixedMap;
    }

    /**
     * @param fixedMap Fixed map.
     */
    private void recordConsistencyViolation(Map<KeyCacheObject, EntryGetResult> fixedMap) {
        if (fixedMap.isEmpty())
            return;

        Map<Object, Object> consistentMap = new HashMap<>();

        for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fixedMap.entrySet()) {
            KeyCacheObject key = entry.getKey();
            CacheObject val = entry.getValue().value();

            ctx.addResult(
                consistentMap,
                key,
                val,
                false,
                false,
                true,
                false,
                null,
                0,
                0);
        }

        Map<UUID, Map<Object, Object>> inconsistentMap = new HashMap<>();

        for (Map.Entry<ClusterNode, GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> pair : futs.entrySet()) {
            ClusterNode node = pair.getKey();

            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut = pair.getValue();

            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.result().entrySet()) {
                KeyCacheObject key = entry.getKey();
                CacheObject val = entry.getValue().value();

                inconsistentMap.computeIfAbsent(node.id(), id -> new HashMap<>());

                Map<Object, Object> map = inconsistentMap.get(node.id());

                ctx.addResult(
                    map,
                    key,
                    val,
                    false,
                    false,
                    true,
                    false,
                    null,
                    0,
                    0);
            }
        }

        GridEventStorageManager evtMgr = ctx.gridEvents();

        if (evtMgr.isRecordable(EVT_CONSISTENCY_VIOLATION))
            evtMgr.record(new CacheConsistencyViolationEvent(
                ctx.discovery().localNode(),
                "Consistency violation detected.",
                inconsistentMap,
                consistentMap));

    }
}
