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

/**
 * Class encapsulating RProp algorithm.
 * @see <a href="https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf">https://paginas.fe.up.pt/~ee02162/dissertacao/RPROP%20paper.pdf</a>.
 */
public class RPropUpdater implements MLPParameterUpdater<RPropUpdaterParams> {
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
     * Default learning rate.
     */
    private static double DFLT_LEARNING_RATE = 0.1;

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
     * Learning rate.
     */
    private final double learningRate;

    /**
     * Losses function.
     */
    protected IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Construct RPropUpdater.
     *
     * @param initUpdate Initial update.
     * @param accelerationRate Acceleration rate.
     * @param deaccelerationRate Deacceleration rate.
     */
    public RPropUpdater(double learningRate, double initUpdate, double accelerationRate, double deaccelerationRate) {
        this.learningRate = learningRate;
        this.initUpdate = initUpdate;
        this.accelerationRate = accelerationRate;
        this.deaccelerationRate = deaccelerationRate;
    }

    /**
     * Construct RPropUpdater with default parameters.
     */
    public RPropUpdater() {
        this(DFLT_LEARNING_RATE, DFLT_INIT_UPDATE, DFLT_ACCELERATION_RATE, DFLT_DEACCELERATION_RATE);
    }

    /** {@inheritDoc} */
    @Override public RPropUpdaterParams init(MLP mlp, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;
        return new RPropUpdaterParams(mlp.architecture(), learningRate);
    }

    /** {@inheritDoc} */
    @Override public RPropUpdaterParams updateParams(MLP mlp, RPropUpdaterParams updaterParams, int iteration, Matrix inputs,
        Matrix groundTruth) {
        Vector gradient = mlp.differentiateByParameters(loss, inputs, groundTruth);
        Vector prevGradient = updaterParams.prevIterationGradient();
        Vector derSigns;

        if (prevGradient != null)
            derSigns = MatrixUtil.zipWith(prevGradient, gradient, (x, y) -> Math.signum(x * y));
        else
            derSigns = gradient.like(gradient.size()).assign(1.0);

        updaterParams.deltas().map(derSigns, (prevDelta, sign) -> {
            if (sign > 0)
                return Math.min(prevDelta * accelerationRate, UPDATE_MAX);
            else if (sign < 0)
                return Math.max(prevDelta * deaccelerationRate, UPDATE_MIN);
            else
                return prevDelta;
        });

        updaterParams.setPrevIterationBiasesUpdates(MatrixUtil.zipWith(gradient, updaterParams.deltas(), (der, delta, i) -> {
            if (derSigns.getX(i) >= 0)
                return -Math.signum(der) * delta;

            return updaterParams.prevIterationUpdates().getX(i);
        }));

        Vector updatesMask = MatrixUtil.zipWith(derSigns, updaterParams.prevIterationUpdates(), (sign, upd, i) -> {
            if (sign < 0)
                gradient.setX(i, 0.0);

            if (sign >= 0)
                return 1.0;
            else
                return -1.0;
        });

        updaterParams.setUpdatesMask(updatesMask);
        updaterParams.setPrevIterationWeightsDerivatives(gradient.copy());

        return updaterParams;
    }
}
