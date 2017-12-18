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
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.MLP;
import org.apache.ignite.ml.nn.trainers.MLPLocalBatchTrainerState;

/**
 * Adapter of {@link MLPParameterUpdater} for updaters performing backpropagation updates of MLP parameters.
 */
public abstract class BackpropUpdater implements MLPParameterUpdater {
    /**
     * Losses function.
     */
    protected IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> loss;

    /**
     * Learning rate.
     */
    protected double learningRate;

    /** {@inheritDoc} */
    @Override public void init(MLP mlp, double learningRate,
        IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> loss) {
        this.loss = loss;
        this.learningRate = learningRate;
    }

    /** {@inheritDoc} */
    @Override public final double updateParamsAndCalculateError(MLP mlp, MLPLocalBatchTrainerState trainerState,
        Matrix inputs, Matrix groundTruth) {
        int lastLayer = mlp.layersCount() - 1;
        int batchSize = groundTruth.columnSize();

        int i = lastLayer;

        initIterationData(mlp, trainerState, learningRate, inputs);

        while (i > 0) {
            updateLayer(mlp, trainerState, i, inputs, groundTruth);
            i--;
        }

        Matrix predicted = mlp.predict(inputs);

        return MatrixUtil.zipFoldByColumns(predicted, groundTruth, (predCol, truthCol) -> loss.apply(truthCol).apply(predCol)).sum() / batchSize;
    }

    /** {@inheritDoc} */
    protected abstract void initIterationData(MLP mlp, MLPLocalBatchTrainerState trainerState, double learningRate, Matrix inputs);

    /** {@inheritDoc} */
    protected abstract void updateLayer(MLP mlp, MLPLocalBatchTrainerState trainerState, int layer, Matrix inputs, Matrix groundTruth);
}
