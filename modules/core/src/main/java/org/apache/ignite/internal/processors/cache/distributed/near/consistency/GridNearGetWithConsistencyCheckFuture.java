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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Checks data consistency. Checks that each backup value equals to primary value.
 */
public class GridNearGetWithConsistencyCheckFuture extends GridNearGetWithConsistencyAbstractFuture {
    /** Context. */
    private GridCacheContext ctx;

    /** Deserialize binary. */
    private boolean deserializeBinary;

    /** Skip values. */
    private boolean skipVals;

    /** Need version. */
    private boolean needVer;

    /** Keys. */
    private Collection<KeyCacheObject> keys;

    /**
     *
     */
    public GridNearGetWithConsistencyCheckFuture(
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
        boolean needVer,
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
            txLbl,
            mvccSnapshot);

        this.ctx = ctx;
        this.deserializeBinary = deserializeBinary;
        this.skipVals = skipVals;
        this.needVer = needVer;
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        Map<KeyCacheObject, EntryGetResult> map = new HashMap<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.result().entrySet()) {
                KeyCacheObject key = entry.getKey();
                EntryGetResult candidae = entry.getValue();
                EntryGetResult old = map.get(key);

                if (old != null && old.version().compareTo(candidae.version()) != 0) {
                    onDone(new IgniteConsistencyViolationException("Distributed cache consistency violation detected."));

                    return;
                }

                map.put(key, candidae);
            }
        }

        onDone(map);
    }

    /**
     * Produces 1 entry's value.
     */
    public <K, V> IgniteInternalFuture<V> single() {
        return init().chain(
            (fut) -> {
                try {
                    final Map<K, V> map = new IgniteBiTuple<>();

                    for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.get().entrySet()) {
                        EntryGetResult getRes = entry.getValue();

                        ctx.addResult(map,
                            entry.getKey(),
                            getRes.value(),
                            skipVals,
                            false,
                            deserializeBinary,
                            false,
                            getRes,
                            getRes.version(),
                            0,
                            0,
                            needVer);
                    }

                    return F.firstValue(map);
                }
                catch (IgniteCheckedException e) {
                    throw new GridClosureException(e);
                }
            });
    }

    /**
     * Produces entry map.
     */
    public <K, V> IgniteInternalFuture<Map<K, V>> multi() {
        return init().chain(
            (fut) -> {
                try {
                    final Map<K, V> map = U.newHashMap(keys.size());

                    for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.get().entrySet()) {
                        EntryGetResult getRes = entry.getValue();

                        ctx.addResult(map,
                            entry.getKey(),
                            getRes.value(),
                            skipVals,
                            false,
                            deserializeBinary,
                            false,
                            getRes,
                            getRes.version(),
                            0,
                            0,
                            needVer);
                    }

                    return map;
                }
                catch (Exception e) {
                    throw new GridClosureException(e);
                }
            });
    }
}
