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

package org.apache.ignite.ml.preprocessing.binarization;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the binarization preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class BinarizationTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {
    /** Threshold. */
    private double threshold;

    /** {@inheritDoc} */
    @Override public BinarizationPreprocessor<K, V> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        return new BinarizationPreprocessor<>(threshold, basePreprocessor);
    }

    /**
     * Gets the threshold parameter value.
     * @return The parameter value.
     */
    public double threshold() {
        return threshold;
    }

    /**
     * Sets the threshold parameter value.
     *
     * @param threshold The given value.
     * @return The Binarization trainer.
     */
    public BinarizationTrainer<K, V> withThreshold(double threshold) {
        this.threshold = threshold;
        return this;
    }
}
