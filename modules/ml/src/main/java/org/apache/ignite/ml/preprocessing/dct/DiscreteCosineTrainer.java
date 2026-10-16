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

package org.apache.ignite.ml.preprocessing.dct;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;

/**
 * Trainer of the Discrete Cosine preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class DiscreteCosineTrainer<K, V> implements PreprocessingTrainer<K, V> {
    /** DCT type. Must be between 1 and 4. Default is 2. */
    private int type = 2;

    /** {@inheritDoc} */
    @Override public DiscreteCosinePreprocessor<K, V> fit(LearningEnvironmentBuilder envBuilder, DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> basePreprocessor) {
        return new DiscreteCosinePreprocessor<>(basePreprocessor, type);
    }

    /**
     * Gets the DCT type.
     *
     * @return The parameter value.
     */
    public double type() {
        return type;
    }

    /**
     * Sets the DCT type. Must be between 1 and 4.
     *
     * @param type The given value.
     * @return The Discrete Cosine trainer.
     */
    public DiscreteCosineTrainer<K, V> withType(int type) {
        assert type >= 1 && type <= 4;
        this.type = type;
        return this;
    }
}
