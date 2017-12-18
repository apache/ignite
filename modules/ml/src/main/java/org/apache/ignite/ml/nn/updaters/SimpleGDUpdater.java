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
import org.apache.ignite.ml.math.functions.IgniteDifferentiableDoubleToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.nn.MLP;
import org.apache.ignite.ml.nn.MLPState;
import org.apache.ignite.ml.nn.trainers.local.MLPLocalBatchTrainerState;

/**
 * Simple gradient descent parameters updater.
 */
public class SimpleGDUpdater extends BackpropUpdater {
    /**
     * Weights of current layer.
     */
    protected Matrix w;

    /**
     * Activators output of current layer.
     */
    protected Matrix a;

    /**
     * Linear output of current layer.
     */
    protected Matrix z;

    /**
     * Matrix encoding differential of (linear output -> activators output) of current layer.
     */
    protected Matrix dSigmaDz;

    /**
     * Matrix encoding differential of (current layer linear output -> loss).
     */
    protected Matrix dz;

    /**
     * Matrix encoding differential of (current layer weights -> loss).
     */
    protected Matrix dw;

    /**
     * State of MLP.
     */
    protected MLPState mlpState;

    /** {@inheritDoc} */
    @Override protected void initIterationData(MLP mlp, MLPLocalBatchTrainerState trainerState, double learningRate, Matrix inputs) {
        this.mlpState = mlp.computeState(inputs);
    }

    /** {@inheritDoc} */
    @Override public void updateLayer(MLP mlp, MLPLocalBatchTrainerState state, int layer, Matrix inputs, Matrix groundTruth) {
        int batchSize = inputs.columnSize();
        double invBatchSize = 1 / (double)batchSize;
        int lastLayer = mlp.layersCount() - 1;

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

        // Update weights.
        elementWiseMinus(w, dw);

        if (mlp.hasBiases(layer)) {
            Vector db = dz.foldRows(Vector::sum).times(invBatchSize);
            mlp.biases(layer).map(db, (x, y) ->  (x - learningRate * y));
        }
    }

    protected Matrix differentiateLoss(Matrix groundTruth, Matrix lastLayerOutput, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        Matrix diff = groundTruth.like(groundTruth.rowSize(), groundTruth.columnSize());

        for (int col = 0; col < groundTruth.columnSize(); col++) {
            // TODO: IGNITE-7155 Couldn't use views here because copy on views doesn't do actual copy and all changes are propagated to original.
            Vector gtCol = groundTruth.getCol(col);
            Vector predCol = lastLayerOutput.getCol(col);
            diff.assignColumn(col, loss.apply(gtCol).differential(predCol));
        }

        return diff;
    }

    protected Matrix differentiateNonlinearity(Matrix linearOut, IgniteDifferentiableDoubleToDoubleFunction nonlinearity) {
        Matrix diff = linearOut.copy();

        diff.map(nonlinearity::differential);

        return diff;
    }

    protected Matrix elementWiseTimes(Matrix mtx1, Matrix mtx2) {
        mtx1.map(mtx2, (a, b) -> a * b);

        return mtx1;
    }

    protected Matrix elementWiseMinus(Matrix mtx1, Matrix mtx2) {
        mtx1.map(mtx2, (a, b) -> a - b);

        return mtx1;
    }
}
