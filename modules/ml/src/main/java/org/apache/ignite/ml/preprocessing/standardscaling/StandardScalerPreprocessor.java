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

package org.apache.ignite.ml.preprocessing.standardscaling;

import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * The preprocessing function that makes standard scaling, transforms features to make {@code mean} equal to {@code 0}
 * and {@code variance} equal to {@code 1}. From mathematical point of view it's the following function which is applied
 * to every element in a dataset:
 *
 * {@code a_i = (a_i - mean_i) / sigma_i for all i},
 *
 * where {@code i} is a number of column, {@code mean_i} is the mean value this column and {@code sigma_i} is the
 * standard deviation in this column.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class StandardScalerPreprocessor<K, V> implements IgniteBiFunction<K, V, Vector> {
    /** */
    private static final long serialVersionUID = -5977957318991608203L;

    /** Means for each column. */
    private final double[] means;
    /** Standard deviation for each column. */
    private final double[] sigmas;

    /** Base preprocessor. */
    private final IgniteBiFunction<K, V, Vector> basePreprocessor;

    /**
     * Constructs a new instance of standardscaling preprocessor.
     *
     * @param means Means of each column.
     * @param sigmas Standard deviations in each column.
     * @param basePreprocessor Base preprocessor.
     */
    public StandardScalerPreprocessor(double[] means, double[] sigmas,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        assert means.length == sigmas.length;

        this.means = means;
        this.sigmas = sigmas;
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

        assert res.size() == means.length;

        for (int i = 0; i < res.size(); i++)
            res.set(i, (res.get(i) - means[i]) / sigmas[i]);

        return res;
    }

    /** */
    public double[] getMeans() {
        return means;
    }

    /** */
    public double[] getSigmas() {
        return sigmas;
    }
}
