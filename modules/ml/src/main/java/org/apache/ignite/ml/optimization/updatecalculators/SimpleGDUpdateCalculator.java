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

import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.optimization.SmoothParametrized;

/**
 * Simple gradient descent parameters updater.
 */
public class SimpleGDUpdateCalculator implements ParameterUpdateCalculator<SmoothParametrized, SimpleGDParameterUpdate> {
    /** */
    private static final long serialVersionUID = -4237332083320879334L;

    /** Learning rate. */
    private double learningRate;

    /** Loss function. */
    protected IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /** Default learning rate. */
    private static final double DEFAULT_LEARNING_RATE = 0.1;

    /** Construct instance of this class with default parameters. */
    public SimpleGDUpdateCalculator() {
        this(DEFAULT_LEARNING_RATE);
    }

    /**
     * Construct SimpleGDUpdateCalculator.
     *
     * @param learningRate Learning rate.
     */
    public SimpleGDUpdateCalculator(double learningRate) {
        this.learningRate = learningRate;
    }

    /** {@inheritDoc} */
    @Override public SimpleGDParameterUpdate init(SmoothParametrized mdl,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;
        return new SimpleGDParameterUpdate(mdl.parametersCount());
    }

    /** {@inheritDoc} */
    @Override public SimpleGDParameterUpdate calculateNewUpdate(SmoothParametrized mlp, SimpleGDParameterUpdate updaterParameters,
        int iteration, Matrix inputs, Matrix groundTruth) {
        return new SimpleGDParameterUpdate(mlp.differentiateByParameters(loss, inputs, groundTruth));
    }

    /** {@inheritDoc} */
    @Override public <M1 extends SmoothParametrized> M1 update(M1 obj, SimpleGDParameterUpdate update) {
        Vector params = obj.parameters();
        return (M1)obj.setParameters(params.minus(update.gradient().times(learningRate)));
    }

    /**
     * Create new instance of this class with same parameters as this one, but with new learning rate.
     *
     * @param learningRate Learning rate.
     * @return New instance of this class with same parameters as this one, but with new learning rate.
     */
    public SimpleGDUpdateCalculator withLearningRate(double learningRate) {
        return new SimpleGDUpdateCalculator(learningRate);
    }
}
