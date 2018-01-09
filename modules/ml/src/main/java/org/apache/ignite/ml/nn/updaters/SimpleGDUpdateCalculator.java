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
 * Simple gradient descent parameters updater.
 */
public class SimpleGDUpdateCalculator<M extends SmoothParametrized> implements ParameterUpdateCalculator<M, SimpleGDParameter> {
    /**
     * Learning rate.
     */
    private double learningRate;

    /**
     * Loss function.
     */
    protected IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Construct SimpleGDUpdateCalculator.
     *
     * @param learningRate Learning rate.
     */
    public SimpleGDUpdateCalculator(double learningRate) {
        this.learningRate = learningRate;
    }

    /** {@inheritDoc} */
    @Override public SimpleGDParameter init(M mdl,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;
        return new SimpleGDParameter(mdl.parametersCount(), learningRate);
    }

    /** {@inheritDoc} */
    @Override public SimpleGDParameter calculateNewUpdate(SmoothParametrized mlp, SimpleGDParameter updaterParameters,
        int iteration, Matrix inputs, Matrix groundTruth) {
        return new SimpleGDParameter(mlp.differentiateByParameters(loss, inputs, groundTruth), learningRate);
    }

    /** {@inheritDoc} */
    @Override public <M1 extends M> M1 update(M1 obj, SimpleGDParameter update) {
        Vector params = obj.parameters();
        return (M1)obj.setParameters(params.minus(update.gradient().times(learningRate)));
    }
}
