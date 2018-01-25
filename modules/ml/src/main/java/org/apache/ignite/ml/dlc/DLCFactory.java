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

package org.apache.ignite.ml.dlc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.ml.dlc.impl.cache.CacheBasedDLCImpl;
import org.apache.ignite.ml.dlc.impl.cache.DLCAffinityFunctionWrapper;
import org.apache.ignite.ml.dlc.impl.cache.util.DLCUpstreamCursorAdapter;
import org.apache.ignite.ml.dlc.impl.local.MapBasedDLCImpl;
import org.apache.ignite.ml.dlc.impl.local.util.DLCUpstreamMapAdapter;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Distributed Learning Context factory which produces contexts based on Ignite Cache and local Map.
 */
public class DLCFactory {
    /**
     * Constructs a new instance of Distributed Learning Context based on the specified upstream Ignite Cache and uses
     * Ignite Cache as reliable storage.
     *
     * @param ignite Ignite instance
     * @param upstreamCache upstream cache
     * @param replicatedTransformer replicated data transformer
     * @param recoverableTransformer recoverable data transformer
     * @param wrapDLC learning context wrapper
     * @param <K> type of an upstream value key
     * @param <V> type of an upstream value
     * @param <Q> type of replicated data of a partition
     * @param <W> type of recoverable data of a partition
     * @param <I> type of returned learning context
     * @return distributed learning context
     */
    @SuppressWarnings("unchecked")
    public static <K, V, Q extends Serializable, W extends AutoCloseable, I extends DLC<K, V, Q, W>> I createDLC(
        Ignite ignite,
        IgniteCache<K, V> upstreamCache,
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
                    replicated = replicatedTransformer.apply(new DLCUpstreamCursorAdapter<>(cursor), cnt);
                }
                DLCPartition<K, V, Q, W> part = new DLCPartition<>(replicated, recoverableTransformer);
                dlcCache.put(currPartIdx, part);
            });
        }

        DLC<K, V, Q, W> dlc = new CacheBasedDLCImpl<>(ignite, upstreamCache, dlcCache, dlcId);

        return wrapDLC.apply(dlc);
    }

    /**
     * Constructs a new instance of Distributed Learning Context based on the specified Map and uses local HashMap as
     * reliable storage.
     *
     * @param upstreamData upstream data
     * @param partitions number of partitions
     * @param replicatedTransformer replicated data transformer
     * @param recoverableTransformer recoverable data transformer
     * @param wrapDLC learning context wrapper
     * @param <K> type of an upstream value key
     * @param <V> type of an upstream value
     * @param <Q> type of replicated data of a partition
     * @param <W> type of recoverable data of a partition
     * @param <I> type of returned learning context
     * @return distributed learning context
     */
    public static <K, V, Q extends Serializable, W extends AutoCloseable, I extends DLC<K, V, Q, W>> I createDLC(
        Map<K, V> upstreamData, int partitions,
        DLCPartitionReplicatedTransformer<K, V, Q> replicatedTransformer,
        DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableTransformer,
        IgniteFunction<DLC<K, V, Q, W>, I> wrapDLC) {
        Map<Integer, DLCPartition<K, V, Q, W>> dlcMap = new HashMap<>();

        int partSize = upstreamData.size() / partitions;

        List<K> keys = new ArrayList<>(upstreamData.keySet());

        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            List<K> partKeys = keys.subList(partIdx * partSize, Math.min((partIdx + 1) * partSize, upstreamData.size()));
            Q replicated = replicatedTransformer.apply(
                new DLCUpstreamMapAdapter<>(upstreamData, partKeys),
                (long) partKeys.size()
            );
            W recoverable = recoverableTransformer.apply(
                new DLCUpstreamMapAdapter<>(upstreamData, partKeys),
                (long) partKeys.size(),
                replicated
            );
            DLCPartition<K, V, Q, W> part = new DLCPartition<>(replicated, null);
            part.setRecoverableData(recoverable);
            dlcMap.put(partIdx, part);
        }

        DLC<K, V, Q, W> dlc = new MapBasedDLCImpl<>(dlcMap, partitions);

        return wrapDLC.apply(dlc);
    }
}
