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

package org.apache.ignite.ml.optimization.updatecalculators;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.optimization.SmoothParametrized;

/**
 * Class encapsulating RMSProp parameter update algorithm.
 *
 * Let g_i be gradient of loss function on i-th iteration.
 * E[g^2]_i = \gamma E[g^2]_{i - 1} + (1 - \gamma) g_i^2.
 * Let w_i be vector of parameters of model on i-th iteration.
 * w_{i + 1} = w_i - \eta / (\sqrt{E[g^2]_i + \epsilon}) g_i.
 */
public class RMSPropUpdateCalculator<M extends SmoothParametrized<M>> implements ParameterUpdateCalculator<M, RMSPropParameterUpdate> {
    /**
     * Gamma from formulas in class description.
     */
    private double gamma;
    /**
     * Eta from formulas in class description.
     */
    private double learningRate;
    /**
     * Epsilon from formulas in class description.
     */
    private double epsilon;
    /**
     * Loss function.
     */
    protected IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Default gamma.
     */
    private static final double DEFAULT_GAMMA = 0.25;

    /**
     * Default learning rate.
     */
    private static final double DEFAULT_LEARNING_RATE = 0.1;

    /**
     * Default epsilon.
     */
    private static final double DEFAULT_EPSILON = 1E-6;

    public RMSPropUpdateCalculator(double gamma, double learningRate, double epsilon) {
        this.gamma = gamma;
        this.learningRate = learningRate;
        this.epsilon = epsilon;
    }

    public RMSPropUpdateCalculator() {
        this(DEFAULT_GAMMA, DEFAULT_LEARNING_RATE, DEFAULT_EPSILON);
    }

    /** {@inheritDoc} */
    @Override public RMSPropParameterUpdate init(M mdl, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;

        return new RMSPropParameterUpdate(mdl.parametersCount());
    }

    /** {@inheritDoc} */
    @Override public RMSPropParameterUpdate calculateNewUpdate(M mdl, RMSPropParameterUpdate updaterParameters, int iteration,
        Matrix inputs, Matrix groundTruth) {
        Vector curGrad = mdl.differentiateByParameters(loss, inputs, groundTruth);

        Vector oldData = updaterParameters.squaresRunningAverage();
        Vector newData = curGrad.copy().map(Functions.SQUARE);

        Vector newRunningAverage = oldData.times(gamma).plus(newData.times(1 - gamma));

        // rms = \sqrt{E[g^2]_i + \epsilon}.
        Vector rms = newRunningAverage.copy().plus(epsilon).map(Math::sqrt);
        Vector invRms = rms.map(x -> 1 / x);

        return new RMSPropParameterUpdate(newRunningAverage, invRms.times(learningRate).times(curGrad));
    }

    /** {@inheritDoc} */
    @Override public <M1 extends M> M1 update(M1 mdl, RMSPropParameterUpdate update) {
        return (M1)mdl.withParameters(mdl.parameters().minus(update.update()));
    }

    /**
     * Set the learning rate.
     *
     * @param learningRate Learning rate.
     * @return This object.
     */
    public RMSPropUpdateCalculator<M> withLearningRate(double learningRate) {
        return new RMSPropUpdateCalculator<>(gamma, learningRate, epsilon);
    }

    /**
     * Set gamma.
     *
     * @param gamma Gamma.
     * @return This object.
     */
    public RMSPropUpdateCalculator<M> withGamma(double gamma) {
        return new RMSPropUpdateCalculator<>(gamma, learningRate, epsilon);
    }

    /**
     * Set epsilon.
     *
     * @param epsilon Epsilon.
     * @return This object.
     */
    public RMSPropUpdateCalculator<M> withEpsilon(double epsilon) {
        return new RMSPropUpdateCalculator<>(gamma, learningRate, epsilon);
    }
}
