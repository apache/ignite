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

package org.apache.ignite.ml.nn.updaters;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDiffirentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.nn.MLP;
import org.apache.ignite.ml.nn.trainers.MLPLocalBatchTrainerState;

/**
 * Interface for classes encapsulating parameters update logic.
 */
public interface MLPParameterUpdater {
    /**
     * Initializes the updater.
     *
     * @param mlp Multilayer perceptron to be trained.
     * @param learningRate Learning rate.
     * @param loss Losses function.
     */
    void init(MLP mlp, double learningRate, IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> loss);

    /**
     * Update mlp parameters and return error on updated mlp.
     *
     * @param mlp Multilayer perceptron to be updated.
     * @param trainerState State of trainer calling this updater.
     * @param inputs Inputs.
     * @param groundTruth True values.
     * @return Error on updated model.
     */
    double updateParamsAndCalculateError(MLP mlp, MLPLocalBatchTrainerState trainerState, Matrix inputs, Matrix groundTruth);
}
