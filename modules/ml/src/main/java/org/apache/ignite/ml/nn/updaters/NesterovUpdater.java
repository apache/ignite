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

/**
 * Class encapsulating Nesterov algorithm for MLP parameters updateModel.
 */
public class NesterovUpdater implements ParameterUpdater<SmoothParametrized, NesterovUpdaterParams> {
    /**
     * Learning rate.
     */
    private final double learningRate;

    /**
     * Loss function.
     */
    private IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Momentum constant.
     */
    protected double momentum;

    /**
     * Construct NesterovUpdaterBuilder.
     *
     * @param momentum Momentum constant.
     */
    public NesterovUpdater(double learningRate, double momentum) {
        this.learningRate = learningRate;
        this.momentum = momentum;
    }

    /** {@inheritDoc} */
    @Override public NesterovUpdaterParams init(SmoothParametrized mdl,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;

        return new NesterovUpdaterParams(mdl.parametersCount());
    }

    /** {@inheritDoc} */
    @Override public NesterovUpdaterParams buildModelUpdater(SmoothParametrized mdl, NesterovUpdaterParams updaterParameters,
        int iteration, Matrix inputs, Matrix groundTruth) {

        if (iteration > 0) {
            Vector curParams = mdl.parameters();
            mdl.setParameters(curParams.minus(updaterParameters.prevIterationUpdates().times(momentum)));
        }

        Vector gradient = mdl.differentiateByParameters(loss, inputs, groundTruth);
        updaterParameters.setPreviousUpdates(updaterParameters.prevIterationUpdates().plus(gradient.times(learningRate)));

        return updaterParameters;
    }
}
