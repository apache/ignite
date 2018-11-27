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
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamTransformerChain;
import org.apache.ignite.ml.dataset.impl.cache.util.ComputeUtils;
import org.apache.ignite.ml.dataset.impl.cache.util.DatasetAffinityFunctionWrapper;

/**
 * A dataset builder that makes {@link CacheBasedDataset}. Encapsulate logic of building cache based dataset such as
 * allocation required data structures and initialization of {@code context} part of partitions.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class CacheBasedDatasetBuilder<K, V> implements DatasetBuilder<K, V> {
    /** Number of retries for the case when one of partitions not found on the node where loading is performed. */
    private static final int RETRIES = 15 * 60;

    /** Retry interval (ms) for the case when one of partitions not found on the node where loading is performed. */
    private static final int RETRY_INTERVAL = 1000;

    /** Template of the name of Ignite Cache containing partition {@code context}. */
    private static final String DATASET_CACHE_TEMPLATE = "%s_DATASET_%s";

    /** Ignite instance. */
    private final Ignite ignite;

    /** Ignite Cache with {@code upstream} data. */
    private final IgniteCache<K, V> upstreamCache;

    /** Filter for {@code upstream} data. */
    private final IgniteBiPredicate<K, V> filter;

    /** Chain of upstream transformers. */
    private final UpstreamTransformerChain<K, V> transformersChain;

    /**
     * Constructs a new instance of cache based dataset builder that makes {@link CacheBasedDataset} with default
     * predicate that passes all upstream entries to dataset.
     *
     * @param ignite Ignite instance.
     * @param upstreamCache Ignite Cache with {@code upstream} data.
     */
    public CacheBasedDatasetBuilder(Ignite ignite, IgniteCache<K, V> upstreamCache) {
        this(ignite, upstreamCache, (a, b) -> true);
    }

    /**
     * Constructs a new instance of cache based dataset builder that makes {@link CacheBasedDataset}.
     *
     * @param ignite Ignite instance.
     * @param upstreamCache Ignite Cache with {@code upstream} data.
     * @param filter Filter for {@code upstream} data.
     */
    public CacheBasedDatasetBuilder(Ignite ignite, IgniteCache<K, V> upstreamCache, IgniteBiPredicate<K, V> filter) {
        this.ignite = ignite;
        this.upstreamCache = upstreamCache;
        this.filter = filter;
        transformersChain = UpstreamTransformerChain.empty();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <C extends Serializable, D extends AutoCloseable> CacheBasedDataset<K, V, C, D> build(
        PartitionContextBuilder<K, V, C> partCtxBuilder, PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        UUID datasetId = UUID.randomUUID();

        // Retrieves affinity function of the upstream Ignite Cache.
        CacheConfiguration<K, V> upstreamCacheConfiguration = upstreamCache.getConfiguration(CacheConfiguration.class);
        AffinityFunction upstreamCacheAffinity = upstreamCacheConfiguration.getAffinity();

        // Creates dataset cache configuration with affinity function that mimics to affinity function of the upstream
        // cache.
        CacheConfiguration<Integer, C> datasetCacheConfiguration = new CacheConfiguration<>();
        datasetCacheConfiguration.setName(String.format(DATASET_CACHE_TEMPLATE, upstreamCache.getName(), datasetId));
        datasetCacheConfiguration.setAffinity(new DatasetAffinityFunctionWrapper(upstreamCacheAffinity));

        IgniteCache<Integer, C> datasetCache = ignite.createCache(datasetCacheConfiguration);

        ComputeUtils.initContext(
            ignite,
            upstreamCache.getName(),
            filter,
            transformersChain,
            datasetCache.getName(),
            partCtxBuilder,
            RETRIES,
            RETRY_INTERVAL
        );

        return new CacheBasedDataset<>(ignite, upstreamCache, filter, transformersChain, datasetCache, partDataBuilder, datasetId);
    }

    /** {@inheritDoc} */
    @Override public UpstreamTransformerChain<K, V> upstreamTransformersChain() {
        return transformersChain;
    }

    /**
     * {@inheritDoc}
     */
    @Override public DatasetBuilder<K, V> withFilter(IgniteBiPredicate<K, V> filterToAdd) {
        return new CacheBasedDatasetBuilder<>(ignite, upstreamCache,
            (e1, e2) -> filter.apply(e1, e2) && filterToAdd.apply(e1, e2));
    }
}
