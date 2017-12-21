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
import org.apache.ignite.ml.nn.MLP;

import static org.apache.ignite.ml.math.util.MatrixUtil.elementWiseTimes;

/**
 * Class encapsulating Nesterov algorithm for MLP parameters update.
 */
public class NesterovUpdater extends BackpropUpdater<NesterovUpdaterParams> {
    /**
     * Momentum constant.
     */
    protected double momentum;

    protected Matrix dz;

    /**
     * Construct NesterovUpdater.
     *
     * @param momentum Momentum constant.
     */
    public NesterovUpdater(double momentum) {
        this.momentum = momentum;
    }

    /** {@inheritDoc} */
    @Override protected NesterovUpdaterParams initParameters(MLP mlp) {
        return new NesterovUpdaterParams(mlp.layersCount());
    }

    /** {@inheritDoc} */
    @Override protected void initIterationData(MLP mlp, NesterovUpdaterParams data, int iteration, double learningRate, Matrix inputs) {
        int lastLayer = mlp.layersCount() - 1;

        if (iteration > 0) {
            // Compute everything in next parameter estimate.
            for (int l = 1; l <= lastLayer; l++) {
                mlp.setWeights(l, mlp.weights(l).minus(data.prevIterationWeightsUpdates[l].times(momentum)));
                if (mlp.hasBiases(l))
                    mlp.setBiases(l, mlp.biases(l).minus(data.prevIterationBiasesUpdates[l].times(momentum)));
            }
        }

        mlpState = mlp.computeState(inputs);
    }

    /** {@inheritDoc} */
    @Override public NesterovUpdaterParams updateLayerData(MLP mlp, NesterovUpdaterParams data, int layer, Matrix inputs, Matrix groundTruth) {
        int batchSize = inputs.columnSize();
        double invBatchSize = 1 / (double)batchSize;
        int lastLayer = mlp.layersCount() - 1;
        double normalizer = invBatchSize * learningRate;

        Matrix z = mlpState.linearOutput(layer).copy();
        Matrix dSigmaDz = differentiateNonlinearity(z, mlp.architecture().transformationLayerArchitecture(layer).activationFunction());

        if (layer == lastLayer) {
            Matrix sigma = mlpState.activatorsOutput(lastLayer).copy();
            Matrix dLossDSigma = differentiateLoss(groundTruth, sigma, loss);
            dz = elementWiseTimes(dLossDSigma, dSigmaDz);
        }
        else
            dz = mlp.weights(layer + 1).transpose().times(dz);

        Matrix a = mlpState.activatorsOutput(layer - 1);
        dz = elementWiseTimes(dz, dSigmaDz);
        Matrix dw = dz.times(a.transpose()).times(normalizer);

        if (data.prevIterationWeightsUpdates[layer] != null)
            dw = dw.plus(data.prevIterationWeightsUpdates[layer].times(momentum));

        data.prevIterationWeightsUpdates[layer] = dw;

        if (mlp.hasBiases(layer)) {
            Vector db = dz.foldRows(Vector::sum).times(normalizer);
            if (data.prevIterationBiasesUpdates[layer] != null)
                db.plus(data.prevIterationBiasesUpdates[layer].times(momentum));
            data.prevIterationBiasesUpdates[layer] = db;
            mlp.biases(layer).map(db, (x, y) -> (x - learningRate * y));
        }

        return data;
    }
}
