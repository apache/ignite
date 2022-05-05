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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
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
        catch (IgniteConsistencyCheckFailedException e) { // Inconsistent entries found.
            Set<KeyCacheObject> inconsistentKeys = e.keys();

            try {
                Map<KeyCacheObject, EntryGetResult> correctedMap = correct(inconsistentKeys);

                assert !correctedMap.isEmpty(); // Check failed on the same data.

                tx.finishFuture().listen(future -> {
                    TransactionState state = tx.state();

                    if (state == TransactionState.COMMITTED) // Explicit tx may fix the values but become rolled back later.
                        recordConsistencyViolation(correctedMap.keySet(), correctedMap);
                });

                onDone(correctedMap);
            }
            catch (IgniteConsistencyRepairFailedException rfe) { // Unable to repair all entries.
                recordConsistencyViolation(inconsistentKeys, /*nothing repaired*/ null);

                Map<KeyCacheObject, EntryGetResult> correctedMap = rfe.correctedMap();

                onDone(new IgniteIrreparableConsistencyViolationException(
                    correctedMap != null ?
                        ctx.unwrapBinariesIfNeeded(correctedMap.keySet(), !deserializeBinary) : null,
                    ctx.unwrapBinariesIfNeeded(rfe.irreparableKeys(), !deserializeBinary)));
            }
            catch (IgniteCheckedException ce) {
                onDone(ce);
            }
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }
}
