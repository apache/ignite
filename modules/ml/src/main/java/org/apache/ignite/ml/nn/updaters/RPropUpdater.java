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
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.MLP;
import org.apache.ignite.ml.nn.trainers.local.MLPLocalBatchTrainerState;

/**
 * Class encapsulating RProp algorithm.
 * @see <a href="https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf">https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf</a>.
 */
public class RPropUpdater extends SimpleGDUpdater {
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
     * Data needed for RProp updater.
     */
    protected RPropUpdaterData data;

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

    /** {@inheritDoc} */
    @Override public void init(MLP mlp, double learningRate,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        super.init(mlp, learningRate, loss);
        data = new RPropUpdaterData(mlp.architecture(), initUpdate);
    }

    /** {@inheritDoc} */
    @Override public void updateLayer(MLP mlp, MLPLocalBatchTrainerState state, int layer,
        Matrix inputs, Matrix groundTruth) {
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

        updateWeightsAndData(layer);

        if (mlp.hasBiases(layer)) {
            Vector db = dz.foldRows(Vector::sum).times(invBatchSize);
            updateBiasesAndData(layer, db, mlp.biases(layer));
        }
    }

    /**
     * Update weights of MLP and updater data for layer with given index.
     *
     * @param layerIdx Layer index.
     */
    private void updateWeightsAndData(int layerIdx) {
        Matrix prevDerivatives = data.prevIterationWeightsDerivatives(layerIdx);
        Matrix derSigns;

        if (prevDerivatives != null)
            derSigns = MatrixUtil.zipWith(prevDerivatives, dw, (x, y) -> Math.signum(x * y));
        else
            derSigns = dw.like(dw.rowSize(), dw.columnSize()).assign(1.0);

        data.weightDeltas(layerIdx).map(derSigns, (prevDelta, sign) -> {
            if (sign > 0)
                return Math.min(prevDelta * accelerationRate, UPDATE_MAX);
            else if (sign < 0)
                return Math.max(prevDelta * deaccelerationRate, UPDATE_MIN);
            else
                return prevDelta;
        });

        data.setPrevIterationWeightsUpdates(layerIdx, MatrixUtil.zipWith(dw, data.weightDeltas(layerIdx), (der, delta, coords) -> {
            int row = coords.get1();
            int col = coords.get2();

            if (derSigns.getX(row, col) >= 0)
                return -Math.signum(der) * delta;

            return data.prevIterationWeightsUpdates(layerIdx).getX(row, col);
        }));

        Matrix newWeights = MatrixUtil.zipWith(derSigns, data.prevIterationWeightsUpdates(layerIdx), (sign, upd, coords) -> {
            int row = coords.get1();
            int col = coords.get2();

            if (sign < 0)
                dw.setX(row, col, 0.0);

            if (sign >= 0)
                return w.getX(row, col) + upd;
            else
                return w.getX(row, col) - upd;
        });

        data.setPrevIterationWeightsDerivatives(layerIdx, dw.copy());
        w.assign(newWeights);
    }

    /**
     * Update biases of MLP and updater data for layer with given index.
     *
     * @param layerIdx Layer index.
     * @param db Differential of loss by bias of given layer.
     * @param b Bias of given layer.
     */
    private void updateBiasesAndData(int layerIdx, Vector db, Vector b) {
        Vector prevDerivatives = data.prevIterationBiasesDerivatives(layerIdx);
        Vector derSigns;
        if (prevDerivatives != null)
            derSigns = MatrixUtil.zipWith(prevDerivatives, db, (x, y) -> Math.signum(x * y));
        else
            derSigns = db.like(db.size()).assign(1.0);

        data.biasDeltas(layerIdx).map(derSigns, (prevDelta, sign) -> {
            if (sign > 0)
                return Math.min(prevDelta * accelerationRate, UPDATE_MAX);
            else if (sign < 0)
                return Math.max(prevDelta * deaccelerationRate, UPDATE_MIN);
            else
                return prevDelta;
        });

        data.setPrevIterationBiasesUpdates(layerIdx, MatrixUtil.zipWith(db, data.biasDeltas(layerIdx), (der, delta, i) -> {
            if (derSigns.getX(i) >= 0)
                return -Math.signum(der) * delta;

            return data.prevIterationBiasesUpdates(layerIdx).getX(i);
        }));

        Vector newBiases = MatrixUtil.zipWith(derSigns, data.prevIterationBiasesUpdates(layerIdx), (sign, upd, i) -> {
            if (sign < 0)
                db.setX(i, 0.0);

            if (sign >= 0)
                return b.getX(i) + upd;
            else
                return b.getX(i) - upd;
        });

        data.setPrevIterationBiasesDerivatives(layerIdx, db.copy());
        b.assign(newBiases);
    }
}
