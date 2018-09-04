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

package org.apache.ignite.ml.preprocessing;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Trainer for preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 * @param <T> Type of a value returned by base preprocessor.
 * @param <R> Type of a value returned by preprocessor fitted by this trainer.
 */
public interface PreprocessingTrainer<K, V, T, R> {
    /**
     * Fits preprocessor.
     *
     * @param datasetBuilder Dataset builder.
     * @param basePreprocessor Base preprocessor.
     * @return Preprocessor.
     */
    public IgniteBiFunction<K, V, R> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, T> basePreprocessor);

    /**
     * Fits preprocessor.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param basePreprocessor Base preprocessor.
     * @return Preprocessor.
     */
    public default IgniteBiFunction<K, V, R> fit(Ignite ignite, IgniteCache<K, V> cache,
        IgniteBiFunction<K, V, T> basePreprocessor) {
        return fit(
            new CacheBasedDatasetBuilder<>(ignite, cache),
            basePreprocessor
        );
    }

    /**
     * Fits preprocessor.
     *
     * @param data Data.
     * @param parts Number of partitions.
     * @param basePreprocessor Base preprocessor.
     * @return Preprocessor.
     */
    public default IgniteBiFunction<K, V, R> fit(Map<K, V> data, int parts,
        IgniteBiFunction<K, V, T> basePreprocessor) {
        return fit(
            new LocalDatasetBuilder<>(data, parts),
            basePreprocessor
        );
    }
}
