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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.dataset.api.SimpleDataset;
import org.apache.ignite.ml.dataset.api.SimpleLabeledDataset;
import org.apache.ignite.ml.dataset.api.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.api.builder.data.SimpleDatasetDataBuilder;
import org.apache.ignite.ml.dataset.api.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.dataset.api.context.EmptyContext;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

public class DatasetFactory {
    /**
     *
     *
     * @param ignite
     * @param upstreamCache
     * @param partCtxBuilder
     * @param partDataBuilder
     * @param <K>
     * @param <V>
     * @param <C>
     * @param <D>
     * @return
     */
    public static <K, V, C extends Serializable, D extends AutoCloseable> Dataset<C, D> create(
        Ignite ignite, IgniteCache<K, V> upstreamCache, PartitionContextBuilder<K, V, C> partCtxBuilder,
        PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        return new CacheBasedDatasetBuilder<>(ignite, upstreamCache, partCtxBuilder, partDataBuilder).build();
    }

    /**
     *
     *
     * @param ignite
     * @param upstreamCache
     * @param partCtxBuilder
     * @param featureExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @param <C>
     * @return
     */
    public static <K, V, C extends Serializable> SimpleDataset<C> createSimpleDataset(Ignite ignite,
        IgniteCache<K, V> upstreamCache, PartitionContextBuilder<K, V, C> partCtxBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, int cols) {
        return create(
            ignite,
            upstreamCache,
            partCtxBuilder,
            new SimpleDatasetDataBuilder<>(featureExtractor, cols)
        ).wrap(SimpleDataset::new);
    }

    /**
     *
     *
     * @param ignite
     * @param upstreamCache
     * @param partCtxBuilder
     * @param featureExtractor
     * @param lbExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @param <C>
     * @return
     */
    public static <K, V, C extends Serializable> SimpleLabeledDataset<C> createSimpleLabeledDataset(Ignite ignite,
        IgniteCache<K, V> upstreamCache, PartitionContextBuilder<K, V, C> partCtxBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, int cols) {
        return create(
            ignite,
            upstreamCache,
            partCtxBuilder,
            new SimpleLabeledDatasetDataBuilder<>(featureExtractor, lbExtractor, cols)
        ).wrap(SimpleLabeledDataset::new);
    }

    /**
     *
     *
     * @param ignite
     * @param upstreamCache
     * @param featureExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> SimpleDataset<EmptyContext> createSimpleDataset(Ignite ignite, IgniteCache<K, V> upstreamCache,
        IgniteBiFunction<K, V, double[]> featureExtractor, int cols) {
        return createSimpleDataset(ignite, upstreamCache, new EmptyContextBuilder<>(), featureExtractor, cols);
    }

    /**
     *
     *
     * @param ignite
     * @param upstreamCache
     * @param featureExtractor
     * @param lbExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> SimpleLabeledDataset<EmptyContext> createSimpleLabeledDataset(Ignite ignite,
        IgniteCache<K, V> upstreamCache, IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor, int cols) {
        return createSimpleLabeledDataset(ignite, upstreamCache, new EmptyContextBuilder<>(), featureExtractor, lbExtractor, cols);
    }

    /**
     *
     *
     * @param upstreamMap
     * @param partitions
     * @param partCtxBuilder
     * @param partDataBuilder
     * @param <K>
     * @param <V>
     * @param <C>
     * @param <D>
     * @return
     */
    public static <K, V, C extends Serializable, D extends AutoCloseable> Dataset<C, D> create(
        Map<K, V> upstreamMap, int partitions, PartitionContextBuilder<K, V, C> partCtxBuilder,
        PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        return new LocalDatasetBuilder<>(upstreamMap, partitions, partCtxBuilder, partDataBuilder).build();
    }

    /**
     *
     *
     * @param upstreamMap
     * @param partitions
     * @param partCtxBuilder
     * @param featureExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @param <C>
     * @return
     */
    public static <K, V, C extends Serializable> SimpleDataset<C> createSimpleDataset(Map<K, V> upstreamMap,
        int partitions, PartitionContextBuilder<K, V, C> partCtxBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, int cols) {
        return create(
            upstreamMap,
            partitions,
            partCtxBuilder,
            new SimpleDatasetDataBuilder<>(featureExtractor, cols)
        ).wrap(SimpleDataset::new);
    }

    /**
     *
     *
     * @param upstreamMap
     * @param partitions
     * @param partCtxBuilder
     * @param featureExtractor
     * @param lbExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @param <C>
     * @return
     */
    public static <K, V, C extends Serializable> SimpleLabeledDataset<C> createSimpleLabeledDataset(
        Map<K, V> upstreamMap, int partitions, PartitionContextBuilder<K, V, C> partCtxBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, int cols) {
        return create(
            upstreamMap,
            partitions,
            partCtxBuilder,
            new SimpleLabeledDatasetDataBuilder<>(featureExtractor, lbExtractor, cols)
        ).wrap(SimpleLabeledDataset::new);
    }

    /**
     *
     *
     * @param upstreamMap
     * @param partitions
     * @param featureExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> SimpleDataset<EmptyContext> createSimpleDataset(Map<K, V> upstreamMap, int partitions,
        IgniteBiFunction<K, V, double[]> featureExtractor, int cols) {
        return createSimpleDataset(upstreamMap, partitions, new EmptyContextBuilder<>(), featureExtractor, cols);
    }

    /**
     *
     *
     * @param upstreamMap
     * @param partitions
     * @param featureExtractor
     * @param lbExtractor
     * @param cols
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> SimpleLabeledDataset<EmptyContext> createSimpleLabeledDataset(Map<K, V> upstreamMap,
        int partitions, IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor,
        int cols) {
        return createSimpleLabeledDataset(upstreamMap, partitions, new EmptyContextBuilder<>(), featureExtractor, lbExtractor, cols);
    }
}
