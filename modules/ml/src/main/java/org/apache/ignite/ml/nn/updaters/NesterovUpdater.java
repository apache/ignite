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
import org.apache.ignite.ml.nn.trainers.local.MLPLocalBatchTrainerState;

/**
 * Class encapsulating Nesterov algorithm for MLP parameters update.
 */
public class NesterovUpdater extends SimpleGDUpdater {
    /**
     * Momentum constant.
     */
    protected double momentum;

    /**
     * Data of previous iteration.
     */
    protected NesterovUpdaterData prevData;

    /**
     * Construct NesterovUpdater.
     *
     * @param momentum Momentum constant.
     */
    public NesterovUpdater(double momentum) {
        this.momentum = momentum;
    }

    /** {@inheritDoc} */
    @Override public void init(MLP mlp, double learningRate,
        IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> loss) {
        super.init(mlp, learningRate, loss);
        prevData = new NesterovUpdaterData(mlp.layersCount());
    }

    /** {@inheritDoc} */
    @Override protected void initIterationData(MLP mlp, MLPLocalBatchTrainerState trainerState, double learningRate, Matrix inputs) {
        int lastLayer = mlp.layersCount() - 1;

        if (trainerState.currentIteration() > 0) {
            // Compute everything in next parameter estimate.
            for (int l = 1; l <= lastLayer; l++) {
                mlp.setWeights(l, mlp.weights(l).minus(prevData.prevIterationWeightsUpdates[l].times(momentum)));
                if (mlp.hasBiases(l))
                    mlp.setBiases(l, mlp.biases(l).minus(prevData.prevIterationBiasesUpdates[l].times(momentum)));
            }
        }

        mlpState = mlp.computeState(inputs);
    }

    /** {@inheritDoc} */
    @Override public void updateLayer(MLP mlp, MLPLocalBatchTrainerState state, int layer, Matrix inputs, Matrix groundTruth) {
        int batchSize = inputs.columnSize();
        double invBatchSize = 1 / (double)batchSize;
        int lastLayer = mlp.layersCount() - 1;

        if (layer + 1 <= lastLayer)
            w = mlp.weights(layer + 1);

        z = mlpState.linearOutput(layer).copy();
        dSigmaDz = differentiateNonlinearity(z, mlp.architecture().transformationLayerArchitecture(layer).activationFunction());

        if (layer == lastLayer) {
            Matrix sigma = mlpState.activatorsOutput(lastLayer).copy();
            Matrix dLossDSigma = differentiateLoss(groundTruth, sigma, loss);
            dz = elementWiseTimes(dLossDSigma, dSigmaDz);
        }
        else
            dz = w.transpose().times(dz);

        w = mlp.weights(layer);
        a = mlpState.activatorsOutput(layer - 1);
        dz = elementWiseTimes(dz, dSigmaDz);
        dw = dz.times(a.transpose()).times(learningRate / batchSize);

        if (prevData.prevIterationWeightsUpdates[layer] != null)
            dw.plus(prevData.prevIterationWeightsUpdates[layer].times(momentum));

        prevData.prevIterationWeightsUpdates[layer] = dw;

        // Update weights.
        elementWiseMinus(w, dw);

        if (mlp.hasBiases(layer)) {
            Vector db = dz.foldRows(Vector::sum).times(invBatchSize);
            if (prevData.prevIterationBiasesUpdates[layer] != null)
                db.plus(prevData.prevIterationBiasesUpdates[layer].times(momentum));
            prevData.prevIterationBiasesUpdates[layer] = db;
            mlp.biases(layer).map(db, (x, y) -> (x - learningRate * y));
        }
    }
}
