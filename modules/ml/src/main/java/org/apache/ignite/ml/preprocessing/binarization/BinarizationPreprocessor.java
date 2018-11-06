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

import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Preprocessing function that makes binarization.
 *
 * Feature values greater than the threshold are binarized to 1.0;
 * values equal to or less than the threshold are binarized to 0.0.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class BinarizationPreprocessor<K, V> implements IgniteBiFunction<K, V, Vector> {
    /** */
    private static final long serialVersionUID = 6877811577892621239L;

    /** Threshold. */
    private final double threshold;

    /** Base preprocessor. */
    private final IgniteBiFunction<K, V, Vector> basePreprocessor;

    /**
     * Constructs a new instance of Binarization preprocessor.
     *
     * @param threshold Threshold value.
     * @param basePreprocessor Base preprocessor.
     */
    public BinarizationPreprocessor(double threshold, IgniteBiFunction<K, V, Vector> basePreprocessor) {
        this.threshold = threshold;
        this.basePreprocessor = basePreprocessor;
    }

    /**
     * Applies this preprocessor.
     *
     * @param k Key.
     * @param v Value.
     * @return Preprocessed row.
     */
    @Override public Vector apply(K k, V v) {
        Vector res = basePreprocessor.apply(k, v);

        for (int i = 0; i < res.size(); i++) {
            if(res.get(i) > threshold) res.set(i, 1.0);
            else res.set(i, 0.0);
        }

        return res;
    }

    /** Gets the threshold parameter. */
    public double threshold() {
        return threshold;
    }
}
