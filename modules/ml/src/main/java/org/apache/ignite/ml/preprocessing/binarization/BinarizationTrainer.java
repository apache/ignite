/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.preprocessing.binarization;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;

/**
 * Trainer of the binarization preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class BinarizationTrainer<K, V> implements PreprocessingTrainer<K, V> {
    /** Threshold. */
    private double threshold;

    /** {@inheritDoc} */
    @Override public BinarizationPreprocessor<K, V> fit(
        LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> basePreprocessor) {
        return new BinarizationPreprocessor<>(threshold, basePreprocessor);
    }

    /**
     * Get the threshold parameter value.
     *
     * @return The property value.
     */
    public double getThreshold() {
        return threshold;
    }

    /**
     * Set the threshold parameter value.
     *
     * @param threshold The given value.
     * @return The Binarization trainer.
     */
    public BinarizationTrainer<K, V> withThreshold(double threshold) {
        this.threshold = threshold;
        return this;
    }
}
