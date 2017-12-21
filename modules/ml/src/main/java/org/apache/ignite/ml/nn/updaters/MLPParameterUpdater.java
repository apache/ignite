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
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.nn.MLP;

/**
 * Interface for classes encapsulating parameters update logic.
 *
 * @param <P> Type of parameters needed for this updater.
 */
public interface MLPParameterUpdater<P extends UpdaterParams> {
    /**
     * Initializes the updater.
     *
     * @param mlp Multilayer perceptron to be trained.
     * @param learningRate Learning rate.
     * @param loss Losses function.
     */
    P init(MLP mlp, double learningRate, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss);

    /**
     * Update updater parameters.
     *
     * @param mlp Multilayer perceptron to be updated.
     * @param updaterParameters Updater parameters to update.
     * @param iteration Current trainer iteration.
     * @param inputs Inputs.
     * @param groundTruth True values.
     * @return Updated parameters.
     */
    P updateParams(MLP mlp, P updaterParameters, int iteration, Matrix inputs, Matrix groundTruth);
}
