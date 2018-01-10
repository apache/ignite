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
 * Class encapsulating Nesterov algorithm for MLP parameters updateCache.
 */
public class NesterovUpdateCalculator<M extends SmoothParametrized>
    implements ParameterUpdateCalculator<M, NesterovParameterUpdate> {
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
     * Construct NesterovUpdateCalculator.
     *
     * @param momentum Momentum constant.
     */
    public NesterovUpdateCalculator(double learningRate, double momentum) {
        this.learningRate = learningRate;
        this.momentum = momentum;
    }

    /** {@inheritDoc} */
    @Override public NesterovParameterUpdate calculateNewUpdate(SmoothParametrized mdl,
        NesterovParameterUpdate updaterParameters, int iteration, Matrix inputs, Matrix groundTruth) {
        // TODO:IGNITE-7350 create new updateCache object here instead of in-place change.

        if (iteration > 0) {
            Vector curParams = mdl.parameters();
            mdl.setParameters(curParams.minus(updaterParameters.prevIterationUpdates().times(momentum)));
        }

        Vector gradient = mdl.differentiateByParameters(loss, inputs, groundTruth);
        updaterParameters.setPreviousUpdates(updaterParameters.prevIterationUpdates()
            .plus(gradient.times(learningRate)));

        return updaterParameters;
    }

    /** {@inheritDoc} */
    @Override public NesterovParameterUpdate init(M mdl,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;

        return new NesterovParameterUpdate(mdl.parametersCount());
    }

    /** {@inheritDoc} */
    @Override public <M1 extends M> M1 update(M1 obj, NesterovParameterUpdate update) {
        Vector parameters = obj.parameters();
        return (M1)obj.setParameters(parameters.minus(update.prevIterationUpdates()));
    }
}
