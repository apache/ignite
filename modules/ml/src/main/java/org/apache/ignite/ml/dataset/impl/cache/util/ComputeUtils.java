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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 *
 */
public class ComputeUtils {

    private static final String DATA_STORAGE_KEY = "part_data_storage_%s";

    /**
     * @param ignite
     * @param cacheNames
     * @param fun
     * @param retries
     * @param interval
     * @param <R>
     * @return
     */
    public static <R> Collection<R> affinityCallWithRetries(Ignite ignite, Collection<String> cacheNames,
        IgniteFunction<Integer, R> fun, int retries, int interval) {
        assert cacheNames.size() > 0;
        assert interval >= 0;

        String primaryCache = cacheNames.iterator().next();

        Affinity<?> affinity = ignite.affinity(primaryCache);
        int partitions = affinity.partitions();

        BitSet completionFlags = new BitSet(partitions);
        Collection<R> results = new ArrayList<>();

        for (int t = 0; t < retries; t++) {
            ClusterGroup clusterGrp = ignite.cluster().forDataNodes(primaryCache);

            // Sends jobs.
            Map<Integer, IgniteFuture<R>> futures = new HashMap<>();
            for (int part = 0; part < partitions; part++)
                if (!completionFlags.get(part)) {
                    final int currPart = part;

                    futures.put(currPart, ignite.compute(clusterGrp).affinityCallAsync(cacheNames, currPart, () -> {
                        checkAllPartitionsAvailable(Ignition.localIgnite(), cacheNames, currPart);

                        return fun.apply(currPart);
                    }));
                }

            // Collects results.
            for (int part : futures.keySet()) {
                try {
                    R res = futures.get(part).get();
                    results.add(res);
                    completionFlags.set(part);
                }
                catch (PartitionNotFoundException ignore) {
                }
            }

            if (completionFlags.cardinality() == partitions)
                return results;

            LockSupport.parkNanos(interval * 1_000_000);
        }

        throw new IllegalStateException();
    }

    /**
     * @param ignite
     * @param cacheNames
     * @param fun
     * @param retries
     * @param <R>
     * @return
     */
    public static <R> Collection<R> affinityCallWithRetries(Ignite ignite, Collection<String> cacheNames,
        IgniteFunction<Integer, R> fun, int retries) {
        return affinityCallWithRetries(ignite, cacheNames, fun, retries, 0);
    }

    /**
     * @param ignite
     * @param upstreamCacheName
     * @param datasetCacheName
     * @param learningCtxId
     * @param part
     * @param partDataBuilder
     * @param <K>
     * @param <V>
     * @param <C>
     * @param <D>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <K, V, C extends Serializable, D extends AutoCloseable> D getData(Ignite ignite,
        String upstreamCacheName, String datasetCacheName, UUID learningCtxId, int part,
        PartitionDataBuilder<K, V, C, D> partDataBuilder) {

        PartitionDataStorage dataStorage = (PartitionDataStorage)ignite
            .cluster()
            .nodeLocalMap()
            .computeIfAbsent(String.format(DATA_STORAGE_KEY, learningCtxId), key -> new PartitionDataStorage());

        Object data = dataStorage.computeDataIfAbsent(part, () -> {
            IgniteCache<Integer, C> learningCtxCache = ignite.cache(datasetCacheName);
            C ctx = learningCtxCache.get(part);

            IgniteCache<K, V> upstreamCache = ignite.cache(upstreamCacheName);

            ScanQuery<K, V> qry = new ScanQuery<>();
            qry.setLocal(true);
            qry.setPartition(part);

            // TODO: how to guarantee that cache size will not be changed between these calls?
            long cnt = upstreamCache.localSizeLong(part);
            try (QueryCursor<Cache.Entry<K, V>> cursor = upstreamCache.query(qry)) {
                return partDataBuilder.build(new UpstreamCursorAdapter<>(cursor.iterator()), cnt, ctx);
            }
        });

        return (D)data;
    }

    /**
     *
     * @param ignite
     * @param upstreamCacheName
     * @param datasetCacheName
     * @param ctxBuilder
     * @param <K>
     * @param <V>
     * @param <C>
     */
    public static <K, V, C extends Serializable> void initContext(Ignite ignite, String upstreamCacheName,
        String datasetCacheName, PartitionContextBuilder<K, V, C> ctxBuilder, int retries, int interval) {
        affinityCallWithRetries(ignite, Arrays.asList(datasetCacheName, upstreamCacheName), part -> {
            Ignite locIgnite = Ignition.localIgnite();

            IgniteCache<K, V> locUpstreamCache = locIgnite.cache(upstreamCacheName);

            ScanQuery<K, V> qry = new ScanQuery<>();
            qry.setLocal(true);
            qry.setPartition(part);

            long cnt = locUpstreamCache.localSizeLong(part);
            C ctx;
            try (QueryCursor<Cache.Entry<K, V>> cursor = locUpstreamCache.query(qry)) {
                ctx = ctxBuilder.build(new UpstreamCursorAdapter<>(cursor.iterator()), cnt);
            }

            IgniteCache<Integer, C> datasetCache = ignite.cache(datasetCacheName);

            datasetCache.put(part, ctx);

            return part;
        }, retries, interval);
    }

    /**
     *
     * @param ignite
     * @param upstreamCacheName
     * @param datasetCacheName
     * @param ctxBuilder
     * @param retries
     * @param <K>
     * @param <V>
     * @param <C>
     */
    public static <K, V, C extends Serializable> void initContext(Ignite ignite, String upstreamCacheName,
        String datasetCacheName, PartitionContextBuilder<K, V, C> ctxBuilder, int retries) {
        initContext(ignite, upstreamCacheName, datasetCacheName, ctxBuilder, retries, 0);
    }

    /**
     * @param ignite
     * @param datasetCacheName
     * @param part
     * @param <C>
     * @return
     */
    public static <C extends Serializable> C getContext(Ignite ignite, String datasetCacheName, int part) {
        IgniteCache<Integer, C> learningCtxCache = ignite.cache(datasetCacheName);
        return learningCtxCache.get(part);
    }

    /**
     * @param ignite
     * @param cacheNames
     * @param part
     */
    private static void checkAllPartitionsAvailable(Ignite ignite, Collection<String> cacheNames, int part) {
        for (String cacheName : cacheNames) {
            Affinity<?> affinity = ignite.affinity(cacheName);

            ClusterNode partNode = affinity.mapPartitionToNode(part);
            ClusterNode locNode = ignite.cluster().localNode();

            if (!partNode.equals(locNode))
                throw new PartitionNotFoundException(cacheName, locNode.id(), part);
        }
    }
}
