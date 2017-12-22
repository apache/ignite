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

package org.apache.ignite.ml.nn.trainers.local;

import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.Losses;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.updaters.ParameterUpdater;
import org.apache.ignite.ml.nn.updaters.RPropUpdater;
import org.apache.ignite.ml.nn.updaters.RPropUpdaterParams;
import org.apache.ignite.ml.nn.updaters.UpdaterParams;

/**
 * Local batch trainer for MLP.
 *
 * @param <P> Parameter updater parameters.
 */
public class MLPLocalBatchTrainer<P extends UpdaterParams<? super MultilayerPerceptron>> extends LocalBatchTrainer<MultilayerPerceptron, P> {
    /**
     * Default loss function.
     */
    private static final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> DEFAULT_LOSS = Losses.MSE;

    /**
     * Default error threshold.
     */
    private static final double DEFAULT_ERROR_THRESHOLD = 1E-5;

    /**
     * Default maximal iterations count.
     */
    private static final int DEFAULT_MAX_ITERATIONS = 100;


    /**
     * Construct a trainer.
     *
     * @param loss Loss function.
     * @param updaterSupplier Supplier of updater function.
     * @param errorThreshold Error threshold.
     * @param maxIterations Maximal iterations count.
     */
    public MLPLocalBatchTrainer(
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        IgniteSupplier<ParameterUpdater<? super MultilayerPerceptron, P>> updaterSupplier,
        double errorThreshold, int maxIterations) {
        super(loss, updaterSupplier, errorThreshold, maxIterations);
    }

    /**
     * Get MLPLocalBatchTrainer with default parameters.
     *
     * @return MLPLocalBatchTrainer with default parameters.
     */
    public static MLPLocalBatchTrainer<RPropUpdaterParams> getDefault() {
        return new MLPLocalBatchTrainer<>(DEFAULT_LOSS,  RPropUpdater::new, DEFAULT_ERROR_THRESHOLD, DEFAULT_MAX_ITERATIONS);
    }
}
