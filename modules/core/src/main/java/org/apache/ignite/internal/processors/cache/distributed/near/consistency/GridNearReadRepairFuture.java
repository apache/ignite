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
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectAdapter;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
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

        assert ctx.transactional() : "Atomic cache should not be recovered using this future";
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        Map<KeyCacheObject, EntryGetResult> newestMap = new HashMap<>(keys.size()); // Newest entries (by version).
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>(); // Newest entries required to be re-committed.

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : fut.keys()) {
                EntryGetResult candidateRes = fut.result().get(key);

                if (!newestMap.containsKey(key)) {
                    newestMap.put(key, candidateRes);

                    continue;
                }

                EntryGetResult newestRes = newestMap.get(key);

                if (candidateRes != null) {
                    if (newestRes == null) { // Existing data wins.
                        newestMap.put(key, candidateRes);
                        fixedMap.put(key, candidateRes);
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

                            try {
                                byte[] candidateBytes = candidateVal.valueBytes(ctx.cacheObjectContext());
                                byte[] newestBytes = newestVal.valueBytes(ctx.cacheObjectContext());

                                if (!Arrays.equals(candidateBytes, newestBytes))
                                    fixedMap.put(key, newestRes); // Same version, fixing values inconsistency.
                            }
                            catch (IgniteCheckedException e) {
                                onDone(e);

                                return;
                            }
                        }
                    }
                }
                else if (newestRes != null)
                    fixedMap.put(key, newestRes); // Existing data wins.
            }
        }

        assert !fixedMap.containsValue(null) : "null should never be considered as a fix";

        if (!fixedMap.isEmpty()) {
            tx.finishFuture().listen(future -> {
                TransactionState state = tx.state();

                if (state == TransactionState.COMMITTED) // Explicit tx may fix the values but become rolled back later.
                    recordConsistencyViolation(fixedMap.keySet(), fixedMap);
            });
        }

        onDone(fixedMap);
    }
}
