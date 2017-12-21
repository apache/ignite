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
import org.apache.ignite.ml.nn.MLP;

/**
 * Simple gradient descent parameters updater.
 */
public class SimpleGDUpdater implements MLPParameterUpdater<Gradients> {
    /**
     * Learning rate.
     */
    double learningRate;

    /**
     * Loss function.
     */
    protected IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    public SimpleGDUpdater(double learningRate) {
        this.learningRate = learningRate;
    }

    @Override public Gradients init(MLP mlp, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        this.loss = loss;
        return new Gradients(mlp.architecture().parametersCount(), learningRate);
    }

    @Override public Gradients updateParams(MLP mlp, Gradients updaterParameters, int iteration, Matrix inputs,
        Matrix groundTruth) {
        return new Gradients(mlp.differentiateByParameters(loss, inputs, groundTruth), learningRate);
    }
}
