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
 * Simple gradient descent parameters updater.
 */
public class SimpleGDUpdater extends BackpropUpdater<Gradients> {
    /**
     * Matrix encoding differential of (current layer linear output -> loss).
     */
    protected Matrix dz;

    @Override protected Gradients initParameters(MLP mlp) {
        return new Gradients(mlp.layersCount());
    }

    /** {@inheritDoc} */
    @Override protected void initIterationData(MLP mlp, Gradients params, int iterationData, double learningRate, Matrix inputs) {
        this.mlpState = mlp.computeState(inputs);
    }

    /** {@inheritDoc} */
    @Override public Gradients updateLayerData(MLP mlp, Gradients updaterParams, int layer, Matrix inputs, Matrix groundTruth) {
        int batchSize = inputs.columnSize();
        double invBatchSize = 1 / (double)batchSize;
        int lastLayer = mlp.layersCount() - 1;

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
        updaterParams.setWeightGradients(layer, dz.times(a.transpose()).times(learningRate / batchSize));

        if (mlp.hasBiases(layer)) {
            Vector db = dz.foldRows(Vector::sum).times(invBatchSize).times(learningRate);
            updaterParams.setBiasGradients(layer, db);
        }

        return updaterParams;
    }
}
