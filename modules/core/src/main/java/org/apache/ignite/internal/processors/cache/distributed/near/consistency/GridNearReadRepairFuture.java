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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.transactions.TransactionState;

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
     * @param strategy Read repair strategy.
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
        ReadRepairStrategy strategy,
        boolean readThrough,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        IgniteCacheExpiryPolicy expiryPlc,
        IgniteInternalTx tx) {
        this(topVer,
            ctx,
            keys,
            strategy,
            readThrough,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            tx,
            null);
    }

    /**
     * @param topVer Affinity topology version.
     * @param ctx Cache context.
     * @param keys Keys.
     * @param strategy Read repair strategy.
     * @param readThrough Read-through flag.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param recovery Partition recovery flag.
     * @param expiryPlc Expiry policy.
     * @param tx Transaction.
     * @param remappedFut Remapped future.
     */
    private GridNearReadRepairFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext ctx,
        Collection<KeyCacheObject> keys,
        ReadRepairStrategy strategy,
        boolean readThrough,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        IgniteCacheExpiryPolicy expiryPlc,
        IgniteInternalTx tx,
        GridNearReadRepairFuture remappedFut) {
        super(topVer,
            ctx,
            keys,
            strategy,
            readThrough,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            tx,
            remappedFut);

        assert ctx.transactional() : "Atomic cache should not be recovered using this future";
    }

    /** {@inheritDoc} */
    @Override protected GridNearReadRepairAbstractFuture remapFuture(AffinityTopologyVersion topVer) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        assert strategy != null;

        try {
            check();

            onDone(Collections.emptyMap()); // Everything is fine.
        }
        catch (IgniteConsistencyViolationException e) { // Inconsistent entries found.
            try {
                Map<KeyCacheObject, EntryGetResult> fixedMap; // Entries required to be re-committed.

                if (strategy == ReadRepairStrategy.LWW)
                    fixedMap = fixWithLww(e.keys());
                else if (strategy == ReadRepairStrategy.PRIMARY)
                    fixedMap = fixWithPrimary(e.keys());
                else if (strategy == ReadRepairStrategy.RELATIVE_MAJORITY)
                    fixedMap = fixWithMajority(e.keys());
                else if (strategy == ReadRepairStrategy.REMOVE)
                    fixedMap = fixWithRemove(e.keys());
                else if (strategy == ReadRepairStrategy.CHECK_ONLY)
                    throw new IgniteIrreparableConsistencyViolationException(null,
                        ctx.unwrapBinariesIfNeeded(e.keys(), !deserializeBinary));
                else
                    throw new UnsupportedOperationException("Unsupported strategy: " + strategy);

                if (!fixedMap.isEmpty()) {
                    tx.finishFuture().listen(future -> {
                        TransactionState state = tx.state();

                        if (state == TransactionState.COMMITTED) // Explicit tx may fix the values but become rolled back later.
                            recordConsistencyViolation(fixedMap.keySet(), fixedMap, strategy);
                    });
                }

                onDone(fixedMap);
            }
            catch (IgniteIrreparableConsistencyViolationException ie) { // Unable to repair all entries.
                recordConsistencyViolation(e.keys(), /*nothing fixed*/ null, strategy);

                onDone(ie);
            }
            catch (IgniteCheckedException ce) {
                onDone(ce);
            }
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     *
     */
    public Map<KeyCacheObject, EntryGetResult> fixWithLww(Set<KeyCacheObject> inconsistentKeys) throws IgniteCheckedException {
        Map<KeyCacheObject, EntryGetResult> newestMap = new HashMap<>(inconsistentKeys.size()); // Newest entries (by version).
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>(inconsistentKeys.size());

        Set<KeyCacheObject> irreparableSet = new HashSet<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : inconsistentKeys) {
                if (!fut.keys().contains(key))
                    continue;

                EntryGetResult candidateRes = fut.result().get(key);

                boolean hasNewest = newestMap.containsKey(key);

                if (!hasNewest) {
                    newestMap.put(key, candidateRes);

                    continue;
                }

                EntryGetResult newestRes = newestMap.get(key);

                if (candidateRes != null) {
                    if (newestRes == null) {
                        if (hasNewest) // Newest is null.
                            irreparableSet.add(key);
                        else { // Existing data wins.
                            newestMap.put(key, candidateRes);
                            fixedMap.put(key, candidateRes);
                        }
                    }
                    else {
                        int compareRes = candidateRes.version().compareTo(newestRes.version());

                        if (compareRes > 0) { // Newest data wins.
                            newestMap.put(key, candidateRes);
                            fixedMap.put(key, candidateRes);
                        }
                        else if (compareRes < 0)
                            fixedMap.put(key, newestRes);
                        else if (compareRes == 0) {
                            CacheObject candidateVal = candidateRes.value();
                            CacheObject newestVal = newestRes.value();

                            byte[] candidateBytes = candidateVal.valueBytes(ctx.cacheObjectContext());
                            byte[] newestBytes = newestVal.valueBytes(ctx.cacheObjectContext());

                            if (!Arrays.equals(candidateBytes, newestBytes))
                                irreparableSet.add(key);
                        }
                    }
                }
                else if (newestRes != null)
                    irreparableSet.add(key); // Impossible to detect latest between existing and null.
            }
        }

        assert !fixedMap.containsValue(null) : "null should never be considered as a fix";

        if (!irreparableSet.isEmpty())
            throwIrreparable(inconsistentKeys, irreparableSet);

        return fixedMap;
    }

    /**
     *
     */
    public Map<KeyCacheObject, EntryGetResult> fixWithPrimary(Collection<KeyCacheObject> inconsistentKeys) {
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>(inconsistentKeys.size());

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : inconsistentKeys) {
                if (fut.keys().contains(key) && primaries.get(key).equals(fut.affNode()))
                    fixedMap.put(key, fut.result().get(key));
            }
        }

        return fixedMap;
    }

    /**
     *
     */
    public Map<KeyCacheObject, EntryGetResult> fixWithRemove(Collection<KeyCacheObject> inconsistentKeys) {
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>(inconsistentKeys.size());

        for (KeyCacheObject key : inconsistentKeys)
            fixedMap.put(key, null);

        return fixedMap;
    }

    /**
     *
     */
    public Map<KeyCacheObject, EntryGetResult> fixWithMajority(Collection<KeyCacheObject> inconsistentKeys)
        throws IgniteCheckedException {
        Set<KeyCacheObject> irreparableSet = new HashSet<>(inconsistentKeys.size());
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>(inconsistentKeys.size());

        for (KeyCacheObject key : inconsistentKeys) {
            Map<T2<ByteArrayWrapper, GridCacheVersion>, T2<EntryGetResult, Integer>> cntMap = new HashMap<>();

            for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
                if (!fut.keys().contains(key))
                    continue;

                EntryGetResult res = fut.result().get(key);

                ByteArrayWrapper wrapped;
                GridCacheVersion ver;

                if (res != null) {
                    CacheObject val = res.value();

                    wrapped = new ByteArrayWrapper(val.valueBytes(ctx.cacheObjectContext()));
                    ver = res.version();
                }
                else {
                    wrapped = new ByteArrayWrapper(null);
                    ver = null;
                }

                T2<ByteArrayWrapper, GridCacheVersion> keyVer = new T2<>(wrapped, ver);

                cntMap.putIfAbsent(keyVer, new T2<>(res, 0));

                cntMap.compute(keyVer, (kv, ri) -> new T2<>(ri.getKey(), ri.getValue() + 1));
            }

            int[] sorted = cntMap.values().stream()
                .map(IgniteBiTuple::getValue)
                .sorted(Comparator.reverseOrder())
                .mapToInt(v -> v)
                .toArray();

            int max = sorted[0];

            assert max > 0;

            if (sorted.length > 1 && sorted[1] == max) { // Majority was not found.
                irreparableSet.add(key);

                continue;
            }

            for (Map.Entry<T2<ByteArrayWrapper, GridCacheVersion>, T2<EntryGetResult, Integer>> count : cntMap.entrySet())
                if (count.getValue().getValue().equals(max)) {
                    fixedMap.put(key, count.getValue().getKey());

                    break;
                }
        }

        if (!irreparableSet.isEmpty())
            throwIrreparable(inconsistentKeys, irreparableSet);

        return fixedMap;
    }

    /**
     *
     */
    private void throwIrreparable(Collection<KeyCacheObject> inconsistentKeys, Set<KeyCacheObject> irreparableSet)
        throws IgniteIrreparableConsistencyViolationException {
        Set<KeyCacheObject> repairableKeys = new HashSet<>(inconsistentKeys);

        repairableKeys.removeAll(irreparableSet);

        throw new IgniteIrreparableConsistencyViolationException(ctx.unwrapBinariesIfNeeded(repairableKeys, !deserializeBinary),
            ctx.unwrapBinariesIfNeeded(irreparableSet, !deserializeBinary));
    }
}
