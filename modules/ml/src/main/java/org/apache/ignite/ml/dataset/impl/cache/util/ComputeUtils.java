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
 * Util class that provides common methods to perform computations on top of the Ignite Compute Grid.
 */
public class ComputeUtils {
    /** Template of the key used to store partition {@code data} in local storage. */
    private static final String DATA_STORAGE_KEY_TEMPLATE = "part_data_storage_%s";

    /**
     * Calls the specified {@code fun} function on all partitions so that is't guaranteed that partitions with the same
     * index of all specified caches will be placed on the same node and will not be moved before computation is
     * finished. If partitions are placed on different nodes then call will be retried, but not more than {@code
     * retries} times with {@code interval} interval specified in milliseconds.
     *
     * @param ignite Ignite instance
     * @param cacheNames collection of cache names
     * @param fun function to be applied on all partitions
     * @param retries number of retries for the case when one of partitions not found on the node
     * @param interval interval of retries for the case when one of partitions not found on the node
     * @param <R> type of a result
     * @return collection of results
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
            for (int part : futures.keySet())
                try {
                    R res = futures.get(part).get();
                    results.add(res);
                    completionFlags.set(part);
                }
                catch (PartitionNotFoundException ignore) {
                }

            if (completionFlags.cardinality() == partitions)
                return results;

            LockSupport.parkNanos(interval * 1_000_000);
        }

        throw new IllegalStateException();
    }

    /**
     * Calls the specified {@code fun} function on all partitions so that is't guaranteed that partitions with the same
     * index of all specified caches will be placed on the same node and will not be moved before computation is
     * finished. If partitions are placed on different nodes then call will be retried, but not more than {@code
     * retries} times.
     *
     * @param ignite Ignite instance
     * @param cacheNames collection of cache names
     * @param fun function to be applied on all partitions
     * @param retries number of retries for the case when one of partitions not found on the node
     * @param <R> type of a result
     * @return collection of results
     */
    public static <R> Collection<R> affinityCallWithRetries(Ignite ignite, Collection<String> cacheNames,
        IgniteFunction<Integer, R> fun, int retries) {
        return affinityCallWithRetries(ignite, cacheNames, fun, retries, 0);
    }

    /**
     * Extracts partition {@code data} from the local storage, if it's not found in local storage recovers this {@code
     * data} from a partition {@code upstream} and {@code context}.
     *
     * @param ignite Ignite instance
     * @param upstreamCacheName name of an {@code upstream} cache
     * @param datasetCacheName name of a partition {@code context} cache
     * @param datasetId dataset ID
     * @param part partition index
     * @param partDataBuilder partition data builder
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     * @param <D> type of a partition {@code data}
     * @return partition {@code data}
     */
    @SuppressWarnings("unchecked")
    public static <K, V, C extends Serializable, D extends AutoCloseable> D getData(Ignite ignite,
        String upstreamCacheName, String datasetCacheName, UUID datasetId, int part,
        PartitionDataBuilder<K, V, C, D> partDataBuilder) {

        PartitionDataStorage dataStorage = (PartitionDataStorage)ignite
            .cluster()
            .nodeLocalMap()
            .computeIfAbsent(String.format(DATA_STORAGE_KEY_TEMPLATE, datasetId), key -> new PartitionDataStorage());

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
     * Initializes partition {@code context} by loading it from a partition {@code upstream}.
     *
     * @param ignite Ignite instance
     * @param upstreamCacheName name of an {@code upstream} cache
     * @param datasetCacheName name of a partition {@code context} cache
     * @param ctxBuilder partition {@code context} builder
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
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

            IgniteCache<Integer, C> datasetCache = locIgnite.cache(datasetCacheName);

            datasetCache.put(part, ctx);

            return part;
        }, retries, interval);
    }

    /**
     * Initializes partition {@code context} by loading it from a partition {@code upstream}.
     *
     * @param ignite Ignite instance
     * @param upstreamCacheName name of an {@code upstream} cache
     * @param datasetCacheName name of a partition {@code context} cache
     * @param ctxBuilder partition {@code context} builder
     * @param retries number of retries for the case when one of partitions not found on the node
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     */
    public static <K, V, C extends Serializable> void initContext(Ignite ignite, String upstreamCacheName,
        String datasetCacheName, PartitionContextBuilder<K, V, C> ctxBuilder, int retries) {
        initContext(ignite, upstreamCacheName, datasetCacheName, ctxBuilder, retries, 0);
    }

    /**
     * Extracts partition {@code context} from the Ignite Cache.
     *
     * @param ignite Ignite instance
     * @param datasetCacheName dataset cache names
     * @param part partition index
     * @param <C> type of a partition {@code context}
     * @return partition {@code context}
     */
    public static <C extends Serializable> C getContext(Ignite ignite, String datasetCacheName, int part) {
        IgniteCache<Integer, C> datasetCache = ignite.cache(datasetCacheName);
        return datasetCache.get(part);
    }

    /**
     * Saves the specified partition {@code context} into the Ignite Cache.
     *
     * @param ignite Ignite instance
     * @param datasetCacheName dataset cache name
     * @param part partition index
     * @param <C> type of a partition {@code context}
     */
    public static <C extends Serializable> void saveContext(Ignite ignite, String datasetCacheName, int part, C ctx) {
        IgniteCache<Integer, C> datasetCache = ignite.cache(datasetCacheName);
        datasetCache.put(part, ctx);
    }

    /**
     * Checks that partitions with the specified partition index of all caches are placed on the same node. In case of
     * rebalancing it's not guaranteed that partitions of caches even with the same affinity function will be moved
     * synchronously. A workaround used here is based on optimistic locking with checking that partitions available on
     * the node.
     *
     * @param ignite Ignite instance
     * @param cacheNames collection of cache names
     * @param part partition index
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
