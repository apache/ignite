/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.optimization.updatecalculators;

import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.optimization.SmoothParametrized;

/**
 * Class encapsulating Nesterov algorithm for MLP parameters updateCache.
 */
public class NesterovUpdateCalculator<M extends SmoothParametrized<M>>
    implements ParameterUpdateCalculator<M, NesterovParameterUpdate> {
    /** */
    private static final long serialVersionUID = 251066184668190622L;

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
    @Override public NesterovParameterUpdate calculateNewUpdate(M mdl,
        NesterovParameterUpdate updaterParameters, int iteration, Matrix inputs, Matrix groundTruth) {
        Vector prevUpdates = updaterParameters.prevIterationUpdates();

        M newMdl = mdl;

        if (iteration > 0)
            newMdl = mdl.withParameters(mdl.parameters().minus(prevUpdates.times(momentum)));

        Vector gradient = newMdl.differentiateByParameters(loss, inputs, groundTruth);

        return new NesterovParameterUpdate(prevUpdates.times(momentum).plus(gradient.times(learningRate)));
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
