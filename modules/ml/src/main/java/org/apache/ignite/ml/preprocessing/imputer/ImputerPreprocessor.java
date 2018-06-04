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

package org.apache.ignite.ml.preprocessing.imputer;

import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Preprocessing function that makes imputing.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class ImputerPreprocessor<K, V> implements IgniteBiFunction<K, V, double[]> {
    /** */
    private static final long serialVersionUID = 6887800576392623469L;

    /** Filling values. */
    private final double[] imputingValues;

    /** Base preprocessor. */
    private final IgniteBiFunction<K, V, double[]> basePreprocessor;

    /**
     * Constructs a new instance of imputing preprocessor.
     *
     * @param basePreprocessor Base preprocessor.
     */
    public ImputerPreprocessor(double[] imputingValues,
        IgniteBiFunction<K, V, double[]> basePreprocessor) {
        this.imputingValues = imputingValues;
        this.basePreprocessor = basePreprocessor;
    }

    /**
     * Applies this preprocessor.
     *
     * @param k Key.
     * @param v Value.
     * @return Preprocessed row.
     */
    @Override public double[] apply(K k, V v) {
        double[] res = basePreprocessor.apply(k, v);

        assert res.length == imputingValues.length;

        for (int i = 0; i < res.length; i++) {
            if (Double.valueOf(res[i]).equals(Double.NaN))
                res[i] = imputingValues[i];
        }
        return res;
    }
}
