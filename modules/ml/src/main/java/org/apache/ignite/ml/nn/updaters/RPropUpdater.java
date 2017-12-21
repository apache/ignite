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
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.MLP;

import static org.apache.ignite.ml.math.util.MatrixUtil.elementWiseTimes;

/**
 * Class encapsulating RProp algorithm.
 * @see <a href="https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf">https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf</a>.
 */
public class RPropUpdater extends BackpropUpdater<RPropUpdaterParams> {
    /**
     * Default initial update.
     */
    private static double DFLT_INIT_UPDATE = 0.5;

    /**
     * Default acceleration rate.
     */
    private static double DFLT_ACCELERATION_RATE = 1.2;

    /**
     * Default deacceleration rate.
     */
    private static double DFLT_DEACCELERATION_RATE = 0.5;

    /**
     * Initial update.
     */
    private final double initUpdate;

    /**
     * Acceleration rate.
     */
    private final double accelerationRate;

    /**
     * Deacceleration rate.
     */
    private final double deaccelerationRate;

    /**
     * Maximal value for update.
     */
    private final static double UPDATE_MAX = 50.0;

    /**
     * Minimal value for update.
     */
    private final static double UPDATE_MIN = 1E-6;

    /**
     * Matrix encoding differential of (current layer linear output -> loss).
     */
    protected Matrix dz;

    /**
     * Construct RPropUpdater.
     *
     * @param initUpdate Initial update.
     * @param accelerationRate Acceleration rate.
     * @param deaccelerationRate Deacceleration rate.
     */
    public RPropUpdater(double initUpdate, double accelerationRate, double deaccelerationRate) {
        this.initUpdate = initUpdate;
        this.accelerationRate = accelerationRate;
        this.deaccelerationRate = deaccelerationRate;
    }

    /**
     * Construct RPropUpdater with default parameters.
     */
    public RPropUpdater() {
        this(DFLT_INIT_UPDATE, DFLT_ACCELERATION_RATE, DFLT_DEACCELERATION_RATE);
    }

    @Override protected RPropUpdaterParams initParameters(MLP mlp) {
        return new RPropUpdaterParams(mlp.architecture(), initUpdate);
    }

    /** {@inheritDoc} */
    @Override public RPropUpdaterParams updateLayerData(MLP mlp, RPropUpdaterParams updaterParams, int layer,
        Matrix inputs, Matrix groundTruth) {
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
        Matrix dw = dz.times(a.transpose()).times(learningRate / batchSize);

        updaterParams.setWeightGradients(layer, dw);
        updateWeightsParameters(layer, mlp.weights(layer), dw, updaterParams);

        if (mlp.hasBiases(layer)) {
            Vector db = dz.foldRows(Vector::sum).times(invBatchSize);
            updateBiasesParameters(layer, db, mlp.biases(layer), updaterParams);
        }

        return updaterParams;
    }

    /**
     * Update weights of MLP and updater data for layer with given index.
     *
     * @param layerIdx Layer index.
     */
    private void updateWeightsParameters(int layerIdx, Matrix w, Matrix dw, RPropUpdaterParams updaterParams) {
        Matrix prevDerivatives = updaterParams.prevIterationWeightsDerivatives(layerIdx);
        Matrix derSigns;

        if (prevDerivatives != null)
            derSigns = MatrixUtil.zipWith(prevDerivatives, dw, (x, y) -> Math.signum(x * y));
        else
            derSigns = dw.like(dw.rowSize(), dw.columnSize()).assign(1.0);

        updaterParams.weightDeltas(layerIdx).map(derSigns, (prevDelta, sign) -> {
            if (sign > 0)
                return Math.min(prevDelta * accelerationRate, UPDATE_MAX);
            else if (sign < 0)
                return Math.max(prevDelta * deaccelerationRate, UPDATE_MIN);
            else
                return prevDelta;
        });

        updaterParams.setPrevIterationWeightsUpdates(layerIdx, MatrixUtil.zipWith(dw, updaterParams.weightDeltas(layerIdx), (der, delta, coords) -> {
            int row = coords.get1();
            int col = coords.get2();

            if (derSigns.getX(row, col) >= 0)
                return -Math.signum(der) * delta;

            return updaterParams.prevIterationWeightsUpdates(layerIdx).getX(row, col);
        }));

        Matrix weightsMask = MatrixUtil.zipWith(derSigns, updaterParams.prevIterationWeightsUpdates(layerIdx), (sign, upd, coords) -> {
            int row = coords.get1();
            int col = coords.get2();

            if (sign < 0)
                dw.setX(row, col, 0.0);

            if (sign >= 0)
                return 1.0;
            else
                return -1.0;
        });

        updaterParams.setWeighsUpdatesMask(layerIdx, weightsMask);
        updaterParams.setPrevIterationWeightsDerivatives(layerIdx, dw.copy());
    }

    /**
     * Update biases of MLP and updater data for layer with given index.
     *
     * @param layerIdx Layer index.
     * @param db Differential of loss by bias of given layer.
     * @param b Bias of given layer.
     */
    private void updateBiasesParameters(int layerIdx, Vector db, Vector b, RPropUpdaterParams updaterParams) {
        Vector prevDerivatives = updaterParams.prevIterationBiasesDerivatives(layerIdx);
        Vector derSigns;
        if (prevDerivatives != null)
            derSigns = MatrixUtil.zipWith(prevDerivatives, db, (x, y) -> Math.signum(x * y));
        else
            derSigns = db.like(db.size()).assign(1.0);

        updaterParams.biasDeltas(layerIdx).map(derSigns, (prevDelta, sign) -> {
            if (sign > 0)
                return Math.min(prevDelta * accelerationRate, UPDATE_MAX);
            else if (sign < 0)
                return Math.max(prevDelta * deaccelerationRate, UPDATE_MIN);
            else
                return prevDelta;
        });

        updaterParams.setPrevIterationBiasesUpdates(layerIdx, MatrixUtil.zipWith(db, updaterParams.biasDeltas(layerIdx), (der, delta, i) -> {
            if (derSigns.getX(i) >= 0)
                return -Math.signum(der) * delta;

            return updaterParams.prevIterationBiasesUpdates(layerIdx).getX(i);
        }));

        Vector newBiases = MatrixUtil.zipWith(derSigns, updaterParams.prevIterationBiasesUpdates(layerIdx), (sign, upd, i) -> {
            if (sign < 0)
                db.setX(i, 0.0);

            if (sign >= 0)
                return 1.0;
            else
                return -1.0;
        });

        updaterParams.setBiasUpdatesMask(layerIdx, newBiases);
        updaterParams.setPrevIterationBiasesDerivatives(layerIdx, db.copy());
    }
}
