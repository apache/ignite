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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridDhtConsistencyRecoveryFuture<K, V> extends GridDhtConsistencyAbstractFuture<Map<K, V>> {
    /** Map. */
    private Map<KeyCacheObject, EntryGetResult> map;

    /** Tx. */
    private GridNearTxLocal tx;

    /**
     *
     */
    public GridDhtConsistencyRecoveryFuture(
        GridNearTxLocal tx,
        Map<KeyCacheObject, EntryGetResult> map,
        GridCacheContext cctx,
        boolean readThrough,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals) {
        super(tx.topologyVersion(),
            cctx,
            map.keySet(),
            readThrough,
            tx.subjectId(),
            tx.resolveTaskName(),
            deserializeBinary,
            recovery,
            expiryPlc,
            skipVals,
            tx.label(),
            tx.mvccSnapshot());

        this.map = map;
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override protected void onResult() {
        if (isDone())
            return;

        if (checkIsDone()) {
            IgniteInternalFuture<GridCacheReturn> fut = checkAndFix();

            fut.listen(future -> {
                if (fut.error() != null)
                    onDone(fut.error());
                else
                    onDone((Map<K, V>)map);
            });
        }
    }

    /**
     *
     */
    private boolean checkIsDone() {
        for (IgniteInternalFuture fut : backupFuts) {
            if (!fut.isDone())
                return false;
        }

        return true;
    }

    /**
     * Returns latest (by version) entry for each key with consistency violation.
     */
    private IgniteInternalFuture<GridCacheReturn> checkAndFix() {
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>();

        for (IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut : backupFuts) {
            Map<KeyCacheObject, EntryGetResult> backupRes = fut.result();

            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : backupRes.entrySet()) {
                EntryGetResult backup = backupRes.get(entry.getKey());
                EntryGetResult primary = map.get(entry.getKey());

                GridCacheVersion bv = backup.version();
                GridCacheVersion pv = primary.version();

                if (!pv.equals(bv)) {
                    fixedMap.putIfAbsent(entry.getKey(), primary);

                    EntryGetResult fixed = fixedMap.get(entry.getKey());
                    GridCacheVersion fv = fixed.version();

                    if (fv.compareTo(bv) < 0)
                        fixedMap.put(entry.getKey(), backup);
                }
            }
        }

        if (fixedMap.isEmpty())
            return new GridFinishedFuture<>();
        else { // Need to convert to <K,V>.
            map.putAll(fixedMap);

            Map<KeyCacheObject, CacheObject> fixedEntries = new HashMap<>();

            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fixedMap.entrySet())
                fixedEntries.put(entry.getKey(), entry.getValue().value());

            // Todo event

            return tx.putAllAsync(cctx, topVer, fixedEntries, false);
        }
    }
}
