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

package org.apache.ignite.ml.trainers;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Interface for trainers. Trainer is just a function which produces model from the data.
 *
 * @param <M> Type of a produced model.
 * @param <L> Type of a label.
 */
public abstract class DatasetTrainer<M extends Model, L> {
    /** Learning Environment. */
    protected LearningEnvironment environment = LearningEnvironment.DEFAULT;

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public abstract <K, V> M fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor);

    /**
     * Trains model based on the specified data.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Ignite ignite, IgniteCache<K, V> cache,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new CacheBasedDatasetBuilder<>(ignite, cache),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param filter Filter for {@code upstream} data.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Ignite ignite, IgniteCache<K, V> cache, IgniteBiPredicate<K, V> filter,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new CacheBasedDatasetBuilder<>(ignite, cache, filter),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param data Data.
     * @param parts Number of partitions.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Map<K, V> data, int parts, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new LocalDatasetBuilder<>(data, parts),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param data Data.
     * @param filter Filter for {@code upstream} data.
     * @param parts Number of partitions.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Map<K, V> data, IgniteBiPredicate<K, V> filter, int parts,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new LocalDatasetBuilder<>(data, filter, parts),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Sets learning Environment
     * @param environment Environment.
     */
    public void setEnvironment(LearningEnvironment environment) {
        this.environment = environment;
    }
}
