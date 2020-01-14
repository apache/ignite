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

package org.apache.ignite.ml.composition.boosting.convergence.median;

import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceChecker;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.preprocessing.Preprocessor;

/**
 * Factory for {@link MedianOfMedianConvergenceChecker}.
 */
public class MedianOfMedianConvergenceCheckerFactory extends ConvergenceCheckerFactory {
    /**
     * @param precision Precision.
     */
    public MedianOfMedianConvergenceCheckerFactory(double precision) {
        super(precision);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ConvergenceChecker<K, V> create(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping, Loss loss, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {

        return new MedianOfMedianConvergenceChecker<>(sampleSize, externalLbToInternalMapping, loss,
            datasetBuilder, preprocessor, precision);
    }
}
