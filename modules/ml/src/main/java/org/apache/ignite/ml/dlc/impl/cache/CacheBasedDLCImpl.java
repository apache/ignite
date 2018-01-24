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

package org.apache.ignite.ml.dlc.impl.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.ml.dlc.DLC;
import org.apache.ignite.ml.dlc.DLCPartition;
import org.apache.ignite.ml.dlc.impl.cache.util.CacheBasedDLCPartitionBuilder;
import org.apache.ignite.ml.dlc.impl.cache.util.UpstreamPartitionNotFoundException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

/**
 * Cache based implementation of a Distributed Learning Context.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 * @param <Q> type of replicated data of a partition
 * @param <W> type of recoverable data of a partition
 */
public class CacheBasedDLCImpl<K, V, Q extends Serializable, W extends AutoCloseable> implements DLC<K, V, Q, W> {
    /** Ignite instance. */
    private final Ignite ignite;

    /** Upstream cache. */
    private final IgniteCache<K, V> upstreamCache;

    /** Distributed Learning Context cache. */
    private final IgniteCache<Integer, DLCPartition<K, V, Q, W>> dlcCache;

    /** Distributed Learning Context id. */
    private final UUID dlcId;

    /**
     * Constructs a new instance of a cache based distributed learning context.
     *
     * @param ignite ignite instance
     * @param upstreamCache upstream cache
     * @param dlcCache distributed learning context cache
     * @param dlcId distributed learning context id
     */
    public CacheBasedDLCImpl(Ignite ignite, IgniteCache<K, V> upstreamCache,
        IgniteCache<Integer, DLCPartition<K, V, Q, W>> dlcCache, UUID dlcId) {
        this.ignite = ignite;
        this.upstreamCache = upstreamCache;
        this.dlcCache = dlcCache;
        this.dlcId = dlcId;
    }

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<DLCPartition<K, V, Q, W>, Integer, R> mapper, IgniteBinaryOperator<R> reducer,
        R identity) {
        Affinity<K> affinity = ignite.affinity(dlcCache.getName());
        int partitions = affinity.partitions();

        String upstreamCacheName = upstreamCache.getName();
        String iddCacheName = dlcCache.getName();

        Map<Integer, IgniteCallable<R>> calls = new HashMap<>();
        Map<Integer, IgniteFuture<R>> futures = new HashMap<>();

        ClusterGroup clusterGrp = ignite.cluster().forDataNodes(iddCacheName);

        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            final int currPartIdx = partIdx;

            IgniteCallable<R> call = () -> {
                CacheBasedDLCPartitionBuilder<K, V, Q, W> partBuilder = new CacheBasedDLCPartitionBuilder<>(
                    Ignition.localIgnite(),
                    upstreamCacheName,
                    iddCacheName,
                    dlcId,
                    currPartIdx
                );

                DLCPartition<K, V, Q, W> part = partBuilder.build();

                R partRes = mapper.apply(part, currPartIdx);

                IgniteCache<Integer, DLCPartition<K, V, Q, W>> dlcCache = ignite.cache(iddCacheName);

                dlcCache.put(currPartIdx, part);

                return partRes;
            };

            IgniteFuture<R> fut = ignite.compute(clusterGrp).affinityCallAsync(
                Arrays.asList(iddCacheName, upstreamCacheName),
                currPartIdx,
                call
            );

            calls.put(currPartIdx, call);
            futures.put(currPartIdx, fut);
        }

        R res = identity;

        while (!calls.isEmpty()) {
            Iterator<Map.Entry<Integer, IgniteCallable<R>>> callIter = calls.entrySet().iterator();
            while (callIter.hasNext()) {
                Map.Entry<Integer, IgniteCallable<R>> callEntry = callIter.next();

                int currPartIdx = callEntry.getKey();
                IgniteCallable<R> call = callEntry.getValue();

                IgniteFuture<R> fut = futures.get(callEntry.getKey());
                try {
                    R partRes = fut.get();
                    res = reducer.apply(res, partRes);
                    callIter.remove();
                }
                catch (UpstreamPartitionNotFoundException e) {
                    IgniteFuture<R> newFut = ignite.compute(clusterGrp).affinityCallAsync(
                        Arrays.asList(iddCacheName, upstreamCacheName),
                        currPartIdx,
                        call
                    );
                    futures.put(currPartIdx, newFut);
                }
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ignite.destroyCache(dlcCache.getName());
    }
}
