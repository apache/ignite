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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectAdapter;
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
    /** Strategy. */
    private final ReadRepairStrategy strategy;

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
        super(topVer,
            ctx,
            keys,
            readThrough,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            tx);

        assert strategy != null;

        this.strategy = strategy;

        assert ctx.transactional() : "Atomic cache should not be recovered using this future";

        init();
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>(); // Entries required to be re-committed.

        assert strategy != null;

        try {
            try {
                check();
            }
            catch (IgniteConsistencyViolationException e) {
                if (strategy == ReadRepairStrategy.LWW)
                    fixWithLww(fixedMap, e.keys());
                else if (strategy == ReadRepairStrategy.PRIMARY)
                    fixWithPrimary(fixedMap, e.keys());
                else if (strategy == ReadRepairStrategy.MAJORITY)
                    fixWithMajority(fixedMap, e.keys());
                else if (strategy == ReadRepairStrategy.REMOVE)
                    fixWithRemove(fixedMap, e.keys());
                else if (strategy == ReadRepairStrategy.CHECK_ONLY)
                    throw e;
                else
                    throw new UnsupportedOperationException("Unsupported strategy: " + strategy);

                if (!fixedMap.isEmpty()) {
                    tx.finishFuture().listen(future -> {
                        TransactionState state = tx.state();

                        if (state == TransactionState.COMMITTED) // Explicit tx may fix the values but become rolled back later.
                            recordConsistencyViolation(fixedMap.keySet(), fixedMap, strategy);
                    });
                }
            }

            onDone(fixedMap);
        }
        catch (IgniteConsistencyViolationException e) {
            Collection<KeyCacheObject> irreparableSet = (Collection<KeyCacheObject>)e.keys();

            recordConsistencyViolation(irreparableSet, /*nothing fixed*/ null, strategy);

            Set<KeyCacheObject> repairableSet = fixedMap.keySet();

            repairableSet.removeAll(irreparableSet);

            if (!repairableSet.isEmpty())
                recordConsistencyViolation(repairableSet, /*nothing fixed*/ null, strategy);

            onDone(new IgniteIrreparableConsistencyViolationException(
                ctx.unwrapBinariesIfNeeded(repairableSet, !deserializeBinary),
                ctx.unwrapBinariesIfNeeded(irreparableSet, !deserializeBinary)));
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     *
     */
    public void fixWithLww(Map<KeyCacheObject, EntryGetResult> fixedMap, Collection<?> inconsistentKeys)
        throws IgniteCheckedException {
        Map<KeyCacheObject, EntryGetResult> newestMap = new HashMap<>(inconsistentKeys.size()); // Newest entries (by version).

        Set<KeyCacheObject> irreparableSet = new HashSet<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : fut.keys()) {
                if (!inconsistentKeys.contains(key))
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
                        if (hasNewest) // Newewst is null.
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
                            CacheObjectAdapter candidateVal = candidateRes.value();
                            CacheObjectAdapter newestVal = newestRes.value();

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
            throw new IgniteConsistencyViolationException(irreparableSet);
    }

    /**
     *
     */
    public void fixWithPrimary(Map<KeyCacheObject, EntryGetResult> fixedMap, Collection<?> inconsistentKeys) {
        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : fut.keys()) {
                if (!inconsistentKeys.contains(key) ||
                    !primaries.get(key).equals(fut.affNode()))
                    continue;

                fixedMap.put(key, fut.result().get(key));
            }
        }
    }

    /**
     *
     */
    public void fixWithRemove(Map<KeyCacheObject, EntryGetResult> fixedMap, Collection<?> inconsistentKeys) {
        for (Object key : inconsistentKeys)
            fixedMap.put((KeyCacheObject)key, null);
    }

    /**
     *
     */
    public void fixWithMajority(Map<KeyCacheObject, EntryGetResult> fixedMap, Collection<?> inconsistentKeys)
        throws IgniteCheckedException {
        class ByteArrayWrapper {
            final byte[] arr;

            public ByteArrayWrapper(byte[] arr) {
                this.arr = arr;
            }

            @Override public boolean equals(Object o) {
                return Arrays.equals(arr, ((ByteArrayWrapper)o).arr);
            }

            @Override public int hashCode() {
                return Arrays.hashCode(arr);
            }
        }

        Set<KeyCacheObject> irreparableSet = new HashSet<>();

        for (Object inconsistentKey : inconsistentKeys) {
            Map<T2<ByteArrayWrapper, GridCacheVersion>, T2<EntryGetResult, Integer>> cntMap = new HashMap<>();

            for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
                for (KeyCacheObject key : fut.keys()) {
                    if (!inconsistentKey.equals(key))
                        continue;

                    EntryGetResult res = fut.result().get(key);

                    ByteArrayWrapper wrapped;
                    GridCacheVersion ver;

                    if (res != null) {
                        CacheObjectAdapter val = res.value();

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
            }

            int[] sorted = cntMap.values().stream()
                .map(IgniteBiTuple::getValue)
                .sorted(Comparator.reverseOrder())
                .mapToInt(v -> v)
                .toArray();

            int max = sorted[0];

            assert max > 0;

            if (sorted.length > 1 && sorted[1] == max) { // Majority was not found.
                irreparableSet.add((KeyCacheObject)inconsistentKey);

                continue;
            }

            for (Map.Entry<T2<ByteArrayWrapper, GridCacheVersion>, T2<EntryGetResult, Integer>> count : cntMap.entrySet())
                if (count.getValue().getValue().equals(max)) {
                    fixedMap.put((KeyCacheObject)inconsistentKey, count.getValue().getKey());

                    break;
                }
        }

        if (!irreparableSet.isEmpty())
            throw new IgniteConsistencyViolationException(irreparableSet);
    }
}
