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

package org.apache.ignite.ml.preprocessing.normalization;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the Normalization preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class NormalizationTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {
    /**  Normalization in L^p space. Must be greater than 0. Default value is 2. */
    private int p = 2;

    /** {@inheritDoc} */
    @Override public NormalizationPreprocessor<K, V> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        return new NormalizationPreprocessor<>(p, basePreprocessor);
    }

    /**
     * Gets the degree of L space parameter value.
     * @return The parameter value.
     */
    public double p() {
        return p;
    }

    /**
     * Sets the p parameter value. Must be greater than 0.
     *
     * @param p The given value.
     * @return The Normalization trainer.
     */
    public NormalizationTrainer<K, V> withP(int p) {
        assert p > 0;
        this.p = p;
        return this;
    }
}
