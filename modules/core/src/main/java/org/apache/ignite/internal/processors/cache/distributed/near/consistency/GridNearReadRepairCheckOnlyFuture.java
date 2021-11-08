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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheObjectAdapter;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Checks data consistency. Checks that each backup value equals to primary value.
 */
public class GridNearReadRepairCheckOnlyFuture extends GridNearReadRepairAbstractFuture {
    /** Skip values. */
    private final boolean skipVals;

    /** Need version. */
    private final boolean needVer;

    /** Keep cache objects. */
    private final boolean keepCacheObjects;

    /**
     * Creates a new instance of GridNearReadRepairCheckOnlyFuture.
     *
     * @param ctx Cache context.
     * @param keys Keys.
     * @param readThrough Read-through flag.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param recovery Partition recovery flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param needVer Need version flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param tx Transaction. Can be {@code null} in case of atomic cache.
     */
    public GridNearReadRepairCheckOnlyFuture(
        GridCacheContext ctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean needVer,
        boolean keepCacheObjects,
        IgniteInternalTx tx) {
        super(null,
            ctx,
            keys,
            readThrough,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            tx);

        this.skipVals = skipVals;
        this.needVer = needVer;
        this.keepCacheObjects = keepCacheObjects;
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        Map<KeyCacheObject, EntryGetResult> resMap = new HashMap<>(keys.size());
        Set<KeyCacheObject> inconsistentKeys = new HashSet<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : fut.keys()) {
                EntryGetResult curRes = fut.result().get(key);

                if (!resMap.containsKey(key)) {
                    resMap.put(key, curRes);

                    continue;
                }

                EntryGetResult prevRes = resMap.get(key);

                if (curRes != null) {
                    if (prevRes == null || prevRes.version().compareTo(curRes.version()) != 0)
                        inconsistentKeys.add(key);
                    else {
                        CacheObjectAdapter curVal = curRes.value();
                        CacheObjectAdapter prevVal = prevRes.value();

                        try {
                            byte[] curBytes = curVal.valueBytes(ctx.cacheObjectContext());
                            byte[] prevBytes = prevVal.valueBytes(ctx.cacheObjectContext());

                            if (!Arrays.equals(curBytes, prevBytes))
                                inconsistentKeys.add(key);
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);

                            return;
                        }
                    }
                }
                else if (prevRes != null)
                    inconsistentKeys.add(key);
            }
        }

        if (!inconsistentKeys.isEmpty()) {
            if (REMAP_CNT_UPD.incrementAndGet(this) > MAX_REMAP_CNT) {
                if (!ctx.transactional()) // Will not be fixed, should be recorded as is.
                    recordConsistencyViolation(inconsistentKeys, /*nothing fixed*/ null);

                onDone(new IgniteConsistencyViolationException("Distributed cache consistency violation detected."));
            }
            else
                remap(ctx.affinity().affinityTopologyVersion()); // Rechecking possible "false positive" case.

            return;
        }

        // Misses recorded to detect partial misses, but should not be propagated when the key is null at each node.
        resMap.values().removeIf(Objects::isNull);

        onDone(resMap);
    }

    /**
     * Returns a future represents 1 entry's value.
     *
     * @return Future represents 1 entry's value.
     */
    public <K, V> IgniteInternalFuture<V> single() {
        return chain((fut) -> {
            try {
                final Map<K, V> map = new IgniteBiTuple<>();

                for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.get().entrySet()) {
                    EntryGetResult getRes = entry.getValue();

                    ctx.addResult(map,
                        entry.getKey(),
                        getRes.value(),
                        skipVals,
                        keepCacheObjects,
                        deserializeBinary,
                        false,
                        getRes,
                        getRes.version(),
                        0,
                        0,
                        needVer,
                        null);
                }

                if (skipVals) {
                    Boolean val = map.isEmpty() ? false : (Boolean)F.firstValue(map);

                    return (V)(val);
                }
                else
                    return F.firstValue(map);
            }
            catch (IgniteCheckedException e) {
                throw new GridClosureException(e);
            }
        });
    }

    /**
     * Returns a future represents entries map.
     *
     * @return Future represents entries map.
     */
    public <K, V> IgniteInternalFuture<Map<K, V>> multi() {
        return chain((fut) -> {
            try {
                final Map<K, V> map = U.newHashMap(keys.size());

                for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.get().entrySet()) {
                    EntryGetResult getRes = entry.getValue();

                    ctx.addResult(map,
                        entry.getKey(),
                        getRes.value(),
                        skipVals,
                        keepCacheObjects,
                        deserializeBinary,
                        false,
                        getRes,
                        getRes.version(),
                        0,
                        0,
                        needVer,
                        null);
                }

                return map;
            }
            catch (Exception e) {
                throw new GridClosureException(e);
            }
        });
    }
}
