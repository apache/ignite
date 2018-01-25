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
import java.util.Collections;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dlc.DLC;
import org.apache.ignite.ml.dlc.DLCFactory;
import org.apache.ignite.ml.dlc.DLCPartition;
import org.apache.ignite.ml.dlc.DLCPartitionRecoverableTransformer;
import org.apache.ignite.ml.dlc.DLCPartitionReplicatedTransformer;
import org.apache.ignite.ml.dlc.impl.cache.util.DLCAffinityFunctionWrapper;
import org.apache.ignite.ml.dlc.impl.cache.util.DLCUpstreamCursorAdapter;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * DLC factory which produces distributed learning contexts based on the Ignite Cache.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class CacheBasedDLCFactory<K, V> implements DLCFactory<K, V> {
    /** Ignite instance. */
    private final Ignite ignite;

    /** Upstream cache. */
    private final IgniteCache<K, V> upstreamCache;

    /**
     * Constructs a new instance of cache based DLC factory.
     *
     * @param ignite Ignite instance
     * @param upstreamCache upstream cache
     */
    public CacheBasedDLCFactory(Ignite ignite, IgniteCache<K, V> upstreamCache) {
        this.ignite = ignite;
        this.upstreamCache = upstreamCache;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <Q extends Serializable, W extends AutoCloseable, I extends DLC<K, V, Q, W>> I createDLC(
        DLCPartitionReplicatedTransformer<K, V, Q> replicatedTransformer,
        DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableTransformer,
        IgniteFunction<DLC<K, V, Q, W>, I> wrapDLC) {
        UUID dlcId = UUID.randomUUID();

        AffinityFunction upstreamCacheAffinity = upstreamCache.getConfiguration(CacheConfiguration.class).getAffinity();

        CacheConfiguration<Integer, DLCPartition<K, V, Q, W>> dlcCacheCfg = new CacheConfiguration<>();
        dlcCacheCfg.setName(dlcId.toString());
        dlcCacheCfg.setAffinity(new DLCAffinityFunctionWrapper(upstreamCacheAffinity));

        IgniteCache<Integer, DLCPartition<K, V, Q, W>> dlcCache = ignite.createCache(dlcCacheCfg);

        Affinity<K> affinity = ignite.affinity(dlcCache.getName());
        int partitions = affinity.partitions();

        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            int currPartIdx = partIdx;

            ignite.compute().affinityRun(Collections.singletonList(dlcCache.getName()), partIdx, () -> {
                Ignite locIgnite = Ignition.localIgnite();
                IgniteCache<K, V> locUpstreamCache = locIgnite.cache(upstreamCache.getName());

                ScanQuery<K, V> qry = new ScanQuery<>();
                qry.setLocal(true);
                qry.setPartition(currPartIdx);

                long cnt = locUpstreamCache.localSizeLong(currPartIdx);
                Q replicated;
                try (QueryCursor<Cache.Entry<K, V>> cursor = locUpstreamCache.query(qry)) {
                    replicated = replicatedTransformer.transform(new DLCUpstreamCursorAdapter<>(cursor), cnt);
                }
                DLCPartition<K, V, Q, W> part = new DLCPartition<>(replicated, recoverableTransformer);
                dlcCache.put(currPartIdx, part);
            });
        }

        DLC<K, V, Q, W> dlc = new CacheBasedDLCImpl<>(ignite, upstreamCache, dlcCache, dlcId);

        return wrapDLC.apply(dlc);
    }
}
