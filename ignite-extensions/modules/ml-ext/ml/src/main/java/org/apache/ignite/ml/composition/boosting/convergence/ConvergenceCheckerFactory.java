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

package org.apache.ignite.ml.composition.boosting.convergence;

import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.preprocessing.Preprocessor;

/**
 * Factory for ConvergenceChecker.
 */
public abstract class ConvergenceCheckerFactory {
    /** Precision of error checking. If error <= precision then it is equated to 0.0 */
    protected double precision;

    /**
     * Creates an instance of ConvergenceCheckerFactory.
     *
     * @param precision Precision [0 <= precision < 1].
     */
    public ConvergenceCheckerFactory(double precision) {
        this.precision = precision;
    }

    /**
     * Create an instance of ConvergenceChecker.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param loss Loss function.
     * @param datasetBuilder Dataset builder.
     * @param vectorizer Upstream vectorizer.
     * @return ConvergenceCheckerFactory instance.
     */
    public abstract <K, V> ConvergenceChecker<K, V> create(long sampleSize,
                                                              IgniteFunction<Double, Double> externalLbToInternalMapping, Loss loss,
                                                              DatasetBuilder<K, V> datasetBuilder,
                                                              Preprocessor<K, V> vectorizer);
}
