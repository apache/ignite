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
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.impl.cache.util.ComputeUtils;
import org.apache.ignite.ml.dataset.impl.cache.util.DatasetAffinityFunctionWrapper;

/**
 *
 * @param <K>
 * @param <V>
 * @param <C>
 * @param <D>
 */
public class CacheBasedDatasetBuilder<K, V, C extends Serializable, D extends AutoCloseable>
    implements DatasetBuilder<C, D> {

    private static final int RETRIES = 100;

    private static final int RETRY_INTERVAL = 500;

    private static final String DATASET_CACHE_TEMPLATE = "%s_DATASET_%s";

    /** Ignite instance. */
    private final Ignite ignite;

    private final IgniteCache<K, V> upstreamCache;

    private final PartitionContextBuilder<K, V, C> partCtxBuilder;

    private final PartitionDataBuilder<K, V, C, D> partDataBuilder;

    public CacheBasedDatasetBuilder(Ignite ignite, IgniteCache<K, V> upstreamCache,
        PartitionContextBuilder<K, V, C> partCtxBuilder, PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        this.ignite = ignite;
        this.upstreamCache = upstreamCache;
        this.partCtxBuilder = partCtxBuilder;
        this.partDataBuilder = partDataBuilder;
    }

    @SuppressWarnings("unchecked")
    @Override public Dataset<C, D> build() {
        UUID datasetId = UUID.randomUUID();

        CacheConfiguration<K, V> upstreamCacheConfiguration = upstreamCache.getConfiguration(CacheConfiguration.class);
        AffinityFunction upstreamCacheAffinity = upstreamCacheConfiguration.getAffinity();

        CacheConfiguration<Integer, C> datasetCacheConfiguration = new CacheConfiguration<>();
        datasetCacheConfiguration.setName(String.format(DATASET_CACHE_TEMPLATE, upstreamCache.getName(), datasetId));
        datasetCacheConfiguration.setAffinity(new DatasetAffinityFunctionWrapper(upstreamCacheAffinity));

        IgniteCache<Integer, C> datasetCache = ignite.createCache(datasetCacheConfiguration);

        ComputeUtils.initContext(
            ignite,
            upstreamCache.getName(),
            datasetCache.getName(),
            partCtxBuilder,
            RETRIES,
            RETRY_INTERVAL
        );

        return new CacheBasedDataset<>(ignite, upstreamCache, datasetCache, partDataBuilder, datasetId);
    }
}
