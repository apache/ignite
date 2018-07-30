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

package org.apache.ignite.ml.dataset.impl.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.impl.cache.util.ComputeUtils;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

/**
 * An implementation of dataset based on Ignite Cache, which is used as {@code upstream} and as reliable storage for
 * partition {@code context} as well.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 * @param <C> Type of a partition {@code context}.
 * @param <D> Type of a partition {@code data}.
 */
public class CacheBasedDataset<K, V, C extends Serializable, D extends AutoCloseable>
    implements Dataset<C, D> {
    /** Number of retries for the case when one of partitions not found on the node where computation is performed. */
    private static final int RETRIES = 15 * 60;

    /** Retry interval (ms) for the case when one of partitions not found on the node where computation is performed. */
    private static final int RETRY_INTERVAL = 1000;

    /** Ignite instance. */
    private final Ignite ignite;

    /** Ignite Cache with {@code upstream} data. */
    private final IgniteCache<K, V> upstreamCache;

    /** Filter for {@code upstream} data. */
    private final IgniteBiPredicate<K, V> filter;

    /** Ignite Cache with partition {@code context}. */
    private final IgniteCache<Integer, C> datasetCache;

    /** Partition {@code data} builder. */
    private final PartitionDataBuilder<K, V, C, D> partDataBuilder;

    /** Dataset ID that is used to identify dataset in local storage on the node where computation is performed. */
    private final UUID datasetId;

    /**
     * Constructs a new instance of dataset based on Ignite Cache, which is used as {@code upstream} and as reliable storage for
     * partition {@code context} as well.
     *
     * @param ignite Ignite instance.
     * @param upstreamCache Ignite Cache with {@code upstream} data.
     * @param filter Filter for {@code upstream} data.
     * @param datasetCache Ignite Cache with partition {@code context}.
     * @param partDataBuilder Partition {@code data} builder.
     * @param datasetId Dataset ID.
     */
    public CacheBasedDataset(Ignite ignite, IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter,
        IgniteCache<Integer, C> datasetCache, PartitionDataBuilder<K, V, C, D> partDataBuilder,
        UUID datasetId) {
        this.ignite = ignite;
        this.upstreamCache = upstreamCache;
        this.filter = filter;
        this.datasetCache = datasetCache;
        this.partDataBuilder = partDataBuilder;
        this.datasetId = datasetId;
    }

    /** {@inheritDoc} */
    @Override public <R> R computeWithCtx(IgniteTriFunction<C, D, Integer, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        String upstreamCacheName = upstreamCache.getName();
        String datasetCacheName = datasetCache.getName();

        return computeForAllPartitions(part -> {
            C ctx = ComputeUtils.getContext(Ignition.localIgnite(), datasetCacheName, part);

            D data = ComputeUtils.getData(
                Ignition.localIgnite(),
                upstreamCacheName,
                filter,
                datasetCacheName,
                datasetId,
                part,
                partDataBuilder
            );

            if (data != null) {
                R res = map.apply(ctx, data, part);

                // Saves partition context after update.
                ComputeUtils.saveContext(Ignition.localIgnite(), datasetCacheName, part, ctx);

                return res;
            }

            return null;
        }, reduce, identity);
    }

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<D, Integer, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        String upstreamCacheName = upstreamCache.getName();
        String datasetCacheName = datasetCache.getName();

        return computeForAllPartitions(part -> {
            D data = ComputeUtils.getData(
                Ignition.localIgnite(),
                upstreamCacheName,
                filter,
                datasetCacheName,
                datasetId,
                part,
                partDataBuilder
            );

            return data != null ? map.apply(data, part) : null;
        }, reduce, identity);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        datasetCache.destroy();
    }

    /**
     * Calls the {@code MapReduce} job specified as the {@code fun} function and the {@code reduce} reducer on all
     * partitions with guarantee that partitions with the same index of upstream and partition {@code context} caches
     * will be on the same node during the computation and will not be moved before computation is finished.
     *
     * @param fun Function that applies to all partitions.
     * @param reduce Function that reduces results of {@code fun}.
     * @param identity Identity.
     * @param <R> Type of a result.
     * @return Final result.
     */
    private <R> R computeForAllPartitions(IgniteFunction<Integer, R> fun, IgniteBinaryOperator<R> reduce, R identity) {
        Collection<String> cacheNames = Arrays.asList(datasetCache.getName(), upstreamCache.getName());
        Collection<R> results = ComputeUtils.affinityCallWithRetries(ignite, cacheNames, fun, RETRIES, RETRY_INTERVAL);

        R res = identity;
        for (R partRes : results)
            if (partRes != null)
                res = reduce.apply(res, partRes);

        return res;
    }

    /** */
    public IgniteCache<K, V> getUpstreamCache() {
        return upstreamCache;
    }

    /** */
    public IgniteCache<Integer, C> getDatasetCache() {
        return datasetCache;
    }
}
