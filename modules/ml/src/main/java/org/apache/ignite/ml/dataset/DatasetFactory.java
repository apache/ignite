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
import org.apache.ignite.ml.dataset.api.data.SimpleDatasetData;
import org.apache.ignite.ml.dataset.api.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Factory providing a client facing API that allows to construct basic and the most frequently used types of dataset.
 *
 *
 * <p>Dataset construction is based on three major concepts: a partition {@code upstream}, {@code context} and
 * {@code data}. A partition {@code upstream} is a data source, which assumed to be available all the time regardless
 * node failures and rebalancing events. A partition {@code context} is a part of a partition maintained during the
 * whole computation process and stored in a reliable storage so that a {@code context} is staying available and
 * consistent regardless node failures and rebalancing events as well as an {@code upstream}. A partition {@code data}
 * is a part of partition maintained during a computation process in unreliable local storage such as heap, off-heap or
 * GPU memory on the node where current computation is performed, so that partition {@code data} can be lost as result
 * of node failure or rebalancing, but it can be restored from an {@code upstream} and a partition {@code context}.
 *
 * <p>A partition {@code context} and {@code data} are built on top of an {@code upstream} by using specified
 * builders: {@link PartitionContextBuilder} and {@link PartitionDataBuilder} correspondingly. To build a generic
 * dataset the following approach is used:
 *
 * <code>
 * {@code
 * Dataset<C, D> dataset = DatasetFactory.create(
 *     ignite,
 *     cache,
 *     partitionContextBuilder,
 *     partitionDataBuilder
 * );
 * }
 * </code>
 *
 * <p>As well as the generic building method {@code create} this factory provides methods that allow to create a
 * specific dataset types such as method {@code createSimpleDataset} to create {@link SimpleDataset} and method
 * {@code createSimpleLabeledDataset} to create {@link SimpleLabeledDataset}.
 *
 * @see Dataset
 * @see PartitionContextBuilder
 * @see PartitionDataBuilder
 */
public class DatasetFactory {
    /**
     * Creates a new instance of distributed dataset using the specified {@code partCtxBuilder} and
     * {@code partDataBuilder}. This is the generic methods that allows to create any Ignite Cache based datasets with
     * any desired partition {@code context} and {@code data}.
     *
     * @param ignite Ignite instance
     * @param upstreamCache Ignite Cache with {@code upstream} data
     * @param partCtxBuilder partition {@code context} builder
     * @param partDataBuilder partition {@code data} builder
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     * @param <D> type of a partition {@code data}
     * @return dataset
     */
    public static <K, V, C extends Serializable, D extends AutoCloseable> Dataset<C, D> create(
        Ignite ignite, IgniteCache<K, V> upstreamCache, PartitionContextBuilder<K, V, C> partCtxBuilder,
        PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        return new CacheBasedDatasetBuilder<>(ignite, upstreamCache, partCtxBuilder, partDataBuilder).build();
    }

    /**
     * Creates a new instance of distributed {@link SimpleDataset} using the specified {@code partCtxBuilder} and
     * {@code featureExtractor}. This methods determines partition {@code data} to be {@link SimpleDatasetData}, but
     * allows to use any desired type of partition {@code context}.
     *
     * @param ignite Ignite instance
     * @param upstreamCache Ignite Cache with {@code upstream} data
     * @param partCtxBuilder partition {@code context} builder
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     * @return dataset
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
     * Creates a new instance of distributed {@link SimpleLabeledDataset} using the specified {@code partCtxBuilder},
     * {@code featureExtractor} and {@code lbExtractor}. This method determines partition {@code data} to be
     * {@link SimpleLabeledDatasetData}, but allows to use any desired type of partition {@code context}.
     *
     * @param ignite Ignite instance
     * @param upstreamCache Ignite Cache with {@code upstream} data
     * @param partCtxBuilder partition {@code context} builder
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleLabeledDatasetData}
     * @param lbExtractor label extractor used to extract labels and buikd {@link SimpleLabeledDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     * @return dataset
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
     * Creates a new instance of distributed {@link SimpleDataset} using the specified {@code featureExtractor}. This
     * methods determines partition {@code context} to be {@link EmptyContext} and partition {@code data} to be
     * {@link SimpleDatasetData}.
     *
     * @param ignite Ignite instance
     * @param upstreamCache Ignite Cache with {@code upstream} data
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @return dataset
     */
    public static <K, V> SimpleDataset<EmptyContext> createSimpleDataset(Ignite ignite, IgniteCache<K, V> upstreamCache,
        IgniteBiFunction<K, V, double[]> featureExtractor, int cols) {
        return createSimpleDataset(ignite, upstreamCache, new EmptyContextBuilder<>(), featureExtractor, cols);
    }

    /**
     * Creates a new instance of distributed {@link SimpleLabeledDataset} using the specified {@code featureExtractor}
     * and {@code lbExtractor}. This methods determines partition {@code context} to be {@link EmptyContext} and
     * partition {@code data} to be {@link SimpleLabeledDatasetData}.
     *
     * @param ignite Ignite instance
     * @param upstreamCache Ignite Cache with {@code upstream} data
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleLabeledDatasetData}
     * @param lbExtractor label extractor used to extract labels and buikd {@link SimpleLabeledDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @return dataset
     */
    public static <K, V> SimpleLabeledDataset<EmptyContext> createSimpleLabeledDataset(Ignite ignite,
        IgniteCache<K, V> upstreamCache, IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor, int cols) {
        return createSimpleLabeledDataset(ignite, upstreamCache, new EmptyContextBuilder<>(), featureExtractor,
            lbExtractor, cols);
    }

    /**
     * Creates a new instance of local dataset using the specified {@code partCtxBuilder} and {@code partDataBuilder}.
     * This is the generic methods that allows to create any Ignite Cache based datasets with any desired partition
     * {@code context} and {@code data}.
     *
     * @param upstreamMap {@code Map} with {@code upstream} data
     * @param partitions number of partitions {@code upstream} {@code Map} will be divided on
     * @param partCtxBuilder partition {@code context} builder
     * @param partDataBuilder partition {@code data} builder
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     * @param <D> type of a partition {@code data}
     * @return dataset
     */
    public static <K, V, C extends Serializable, D extends AutoCloseable> Dataset<C, D> create(
        Map<K, V> upstreamMap, int partitions, PartitionContextBuilder<K, V, C> partCtxBuilder,
        PartitionDataBuilder<K, V, C, D> partDataBuilder) {
        return new LocalDatasetBuilder<>(upstreamMap, partitions, partCtxBuilder, partDataBuilder).build();
    }

    /**
     * Creates a new instance of local {@link SimpleDataset} using the specified {@code partCtxBuilder} and
     * {@code featureExtractor}. This methods determines partition {@code data} to be {@link SimpleDatasetData}, but
     * allows to use any desired type of partition {@code context}.
     *
     * @param upstreamMap {@code Map} with {@code upstream} data
     * @param partitions number of partitions {@code upstream} {@code Map} will be divided on
     * @param partCtxBuilder partition {@code context} builder
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     * @return dataset
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
     * Creates a new instance of local {@link SimpleLabeledDataset} using the specified {@code partCtxBuilder},
     * {@code featureExtractor} and {@code lbExtractor}. This method determines partition {@code data} to be
     * {@link SimpleLabeledDatasetData}, but allows to use any desired type of partition {@code context}.
     *
     * @param upstreamMap {@code Map} with {@code upstream} data
     * @param partitions number of partitions {@code upstream} {@code Map} will be divided on
     * @param partCtxBuilder partition {@code context} builder
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleLabeledDatasetData}
     * @param lbExtractor label extractor used to extract labels and buikd {@link SimpleLabeledDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @param <C> type of a partition {@code context}
     * @return dataset
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
     * Creates a new instance of local {@link SimpleDataset} using the specified {@code featureExtractor}. This
     * methods determines partition {@code context} to be {@link EmptyContext} and partition {@code data} to be
     * {@link SimpleDatasetData}.
     *
     * @param upstreamMap {@code Map} with {@code upstream} data
     * @param partitions number of partitions {@code upstream} {@code Map} will be divided on
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @return dataset
     */
    public static <K, V> SimpleDataset<EmptyContext> createSimpleDataset(Map<K, V> upstreamMap, int partitions,
        IgniteBiFunction<K, V, double[]> featureExtractor, int cols) {
        return createSimpleDataset(upstreamMap, partitions, new EmptyContextBuilder<>(), featureExtractor, cols);
    }

    /**
     * Creates a new instance of local {@link SimpleLabeledDataset} using the specified {@code featureExtractor}
     * and {@code lbExtractor}. This methods determines partition {@code context} to be {@link EmptyContext} and
     * partition {@code data} to be {@link SimpleLabeledDatasetData}.
     *
     * @param upstreamMap {@code Map} with {@code upstream} data
     * @param partitions number of partitions {@code upstream} {@code Map} will be divided on
     * @param featureExtractor feature extractor used to extract features and build {@link SimpleLabeledDatasetData}
     * @param lbExtractor label extractor used to extract labels and buikd {@link SimpleLabeledDatasetData}
     * @param cols number of columns (features) will be extracted
     * @param <K> type of a key in {@code upstream} data
     * @param <V> type of a value in {@code upstream} data
     * @return dataset
     */
    public static <K, V> SimpleLabeledDataset<EmptyContext> createSimpleLabeledDataset(Map<K, V> upstreamMap,
        int partitions, IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor,
        int cols) {
        return createSimpleLabeledDataset(upstreamMap, partitions, new EmptyContextBuilder<>(), featureExtractor,
            lbExtractor, cols);
    }
}
