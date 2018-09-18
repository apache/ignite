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

import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Preprocessing function that makes normalization.
 *
 * Normalization is the process of scaling individual samples to have unit norm.
 * This process can be useful if you plan to use a quadratic form such as the dot-product or any other kernel
 * to quantify the similarity of any pair of samples.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class NormalizationPreprocessor<K, V> implements IgniteBiFunction<K, V, Vector> {
    /** */
    private static final long serialVersionUID = 6873438115778921295L;

    /** Normalization in L^p space. Must be greater than 0. Default value is 2. */
    private int p;

    /** Base preprocessor. */
    private final IgniteBiFunction<K, V, Vector> basePreprocessor;

    /**
     * Constructs a new instance of Normalization preprocessor.
     *
     * @param p Degree of L^p space value.
     * @param basePreprocessor Base preprocessor.
     */
    public NormalizationPreprocessor(int p, IgniteBiFunction<K, V, Vector> basePreprocessor) {
        this.p = p;
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

        double pNorm = Math.pow(foldMap(res, Functions.PLUS, Functions.pow(p), 0d), 1.0 / p);

        for (int i = 0; i < res.size(); i++)
            res.set(i, res.get(i) / pNorm);

        return res;
    }

    /**
     * Folds given array into a single value.
     * @param vec The given array.
     * @param foldFun Folding function that takes two parameters: accumulator and the current value.
     * @param mapFun Mapping function that is called on each vector element before its passed to the accumulator (as its
     * second parameter).
     * @param zero Zero value for fold operation.
     * @return Folded value of this vector.
     */
    private double foldMap(Vector vec, IgniteBiFunction<Double,Double,Double> foldFun, IgniteDoubleFunction<Double> mapFun, double zero) {
        for (int i = 0;  i< vec.size(); i++)
            zero = foldFun.apply(zero, mapFun.apply(vec.get(i)));

        return zero;
    }

    /** Gets the degree of L^p space parameter value. */
    public double p() {
        return p;
    }
}
