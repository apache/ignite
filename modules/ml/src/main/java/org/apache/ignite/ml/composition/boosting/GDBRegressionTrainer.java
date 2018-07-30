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

package org.apache.ignite.ml.composition.boosting;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Trainer for regressor using Gradient Boosting.
 * This algorithm uses gradient of Mean squared error loss metric [MSE] in each step of learning.
 */
public abstract class GDBRegressionTrainer extends GDBTrainer {
    /**
     * Constructs instance of GDBRegressionTrainer.
     *
     * @param gradStepSize Grad step size.
     * @param cntOfIterations Count of learning iterations.
     */
    public GDBRegressionTrainer(double gradStepSize, Integer cntOfIterations) {
        super(gradStepSize,
            cntOfIterations,
            LossGradientPerPredictionFunctions.MSE);
    }

    /** {@inheritDoc} */
    @Override protected <V, K> void learnLabels(DatasetBuilder<K, V> builder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lExtractor) {

    }

    /** {@inheritDoc} */
    @Override protected double externalLabelToInternal(double x) {
        return x;
    }

    /** {@inheritDoc} */
    @Override protected double internalLabelToExternal(double x) {
        return x;
    }
}
