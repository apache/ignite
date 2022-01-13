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
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
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
     * @param strategy Read repair strategy.
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
        ReadRepairStrategy strategy,
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
            strategy,
            readThrough,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            tx);

        this.skipVals = skipVals;
        this.needVer = needVer;
        this.keepCacheObjects = keepCacheObjects;

        init();
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        try {
            onDone(check());
        }
        catch (IgniteConsistencyViolationException e) {
            Set<KeyCacheObject> inconsistentKeys = e.keys();

            if (REMAP_CNT_UPD.incrementAndGet(this) > MAX_REMAP_CNT) {
                if (ctx.atomic() || strategy == ReadRepairStrategy.CHECK_ONLY) { // Will not be fixed, should be recorded as is.
                    recordConsistencyViolation(inconsistentKeys, /*nothing fixed*/ null, ReadRepairStrategy.CHECK_ONLY);

                    onDone(new IgniteIrreparableConsistencyViolationException(null,
                        ctx.unwrapBinariesIfNeeded(inconsistentKeys, !deserializeBinary)));
                }
                else // Should be fixed by concurrent tx(s).
                    onDone(new IgniteConsistencyViolationException(inconsistentKeys));
            }
            else
                remap(ctx.affinity().affinityTopologyVersion()); // Rechecking possible "false positive" case.
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
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

                addResult(fut, map);

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

                addResult(fut, map);

                return map;
            }
            catch (Exception e) {
                throw new GridClosureException(e);
            }
        });
    }

    /**
     *
     */
    private <K, V> void addResult(IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut,
        Map<K, V> map) throws IgniteCheckedException {
        for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.get().entrySet()) {
            EntryGetResult getRes = entry.getValue();

            if (getRes != null)
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
                    U.deploymentClassLoader(ctx.kernalContext(), U.contextDeploymentClassLoaderId(ctx.kernalContext())));
        }
    }
}
