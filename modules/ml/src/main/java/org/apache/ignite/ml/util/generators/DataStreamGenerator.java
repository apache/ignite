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

package org.apache.ignite.ml.util.generators;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.primitives.scalar.RandomProducer;

/**
 * Provides general interface for generation of pseudorandom vectors according to shape defined by logic of specific
 * data stream generator.
 */
public interface DataStreamGenerator {
    /**
     * Size of batch for {@link IgniteCache#putAll(Map)}.
     */
    public static final int FILL_CACHE_BATCH_SIZE = 1000;

    /**
     * @return Stream of {@link LabeledVector} in according to dataset shape.
     */
    public Stream<LabeledVector<Double>> labeled();

    /**
     * @return Stream of unlabeled {@link Vector} in according to dataset shape.
     */
    public default Stream<Vector> unlabeled() {
        return labeled().map(DatasetRow::features);
    }

    /**
     * @param classifier User defined classifier for vectors stream.
     * @return Stream of {@link LabeledVector} in according to dataset shape and user's classifier.
     */
    public default Stream<LabeledVector<Double>> labeled(IgniteFunction<Vector, Double> classifier) {
        return labeled().map(DatasetRow::features).map(v -> new LabeledVector<>(v, classifier.apply(v)));
    }

    /**
     * Apply user defined mapper to vectors stream without labels hiding.
     *
     * @param f Mapper of vectors of data stream.
     * @return Stream of mapped vectors.
     */
    public default DataStreamGenerator mapVectors(IgniteFunction<Vector, Vector> f) {
        return new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return DataStreamGenerator.this.labeled()
                    .map(v -> new LabeledVector<>(f.apply(v.features()), v.label()));
            }
        };
    }

    /**
     * Apply pseudorandom noize to vectors without labels mapping. Such method can be useful in cases when vectors with
     * different labels should be mixed between them on class bounds.
     *
     * @param rnd Generator of pseudorandom scalars modifying vector components with label saving.
     * @return Stream of blurred vectors with same labels.
     */
    public default DataStreamGenerator blur(RandomProducer rnd) {
        return mapVectors(rnd::noizify);
    }

    /**
     * Convert first N values from stream to map.
     *
     * @param datasetSize Dataset size.
     * @return Map of vectors and labels.
     */
    public default Map<Vector, Double> asMap(int datasetSize) {
        return labeled().limit(datasetSize)
            .collect(Collectors.toMap(DatasetRow::features, LabeledVector::label));
    }

    /**
     * Convert first N values from stream to {@link DatasetBuilder}.
     *
     * @param datasetSize Dataset size.
     * @param partitions Partitions count.
     * @return Dataset builder.
     */
    public default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize, int partitions) {
        return new DatasetBuilderAdapter(this, datasetSize, partitions);
    }

    /**
     * Convert first N values from stream to {@link DatasetBuilder}.
     *
     * @param datasetSize Dataset size.
     * @param filter Data filter.
     * @param partitions Partitions count.
     * @return Dataset builder.
     */
    public default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize,
        IgniteBiPredicate<Vector, Double> filter,
        int partitions) {

        return new DatasetBuilderAdapter(this, datasetSize, filter, partitions);
    }

    /**
     * Convert first N values from stream to {@link DatasetBuilder}.
     *
     * @param datasetSize Dataset size.
     * @param filter Data filter.
     * @param partitions Partitions count.
     * @param upstreamTransformerBuilder Upstream transformer builder.
     * @return Dataset builder.
     */
    public default DatasetBuilder<Vector, Double> asDatasetBuilder(int datasetSize,
        IgniteBiPredicate<Vector, Double> filter,
        int partitions, UpstreamTransformerBuilder upstreamTransformerBuilder) {

        return new DatasetBuilderAdapter(this, datasetSize, filter, partitions, upstreamTransformerBuilder);
    }

    /**
     * Fills given cache with labeled vectors from this generator and user defined mapper from vectors to keys.
     *
     * @param datasetSize Rows count to put.
     * @param cache Cache.
     * @param keyMapper Mapping from vectors to keys.
     * @param <K> Key type.
     */
    public default <K> void fillCacheWithCustomKey(int datasetSize, IgniteCache<K, LabeledVector<Double>> cache,
        Function<LabeledVector<Double>, K> keyMapper) {

        Map<K, LabeledVector<Double>> batch = new HashMap<>();
        labeled().limit(datasetSize).forEach(vec -> {
            batch.put(keyMapper.apply(vec), vec);
            if (batch.size() == FILL_CACHE_BATCH_SIZE) {
                cache.putAll(batch);
                batch.clear();
            }
        });

        if (!batch.isEmpty())
            cache.putAll(batch);
    }

    /**
     * Fills given cache with labeled vectors from this generator as values and their hashcodes as keys.
     *
     * @param datasetSize Rows count to put.
     * @param cache Cache.
     */
    public default void fillCacheWithVecHashAsKey(int datasetSize, IgniteCache<Integer, LabeledVector<Double>> cache) {
        fillCacheWithCustomKey(datasetSize, cache, LabeledVector::hashCode);
    }

    /**
     * Fills given cache with labeled vectors from this generator as values and random UUIDs as keys
     *
     * @param datasetSize Rows count to put.
     * @param cache Cache.
     */
    public default void fillCacheWithVecUUIDAsKey(int datasetSize, IgniteCache<UUID, LabeledVector<Double>> cache) {
        fillCacheWithCustomKey(datasetSize, cache, v -> UUID.randomUUID());
    }
}
