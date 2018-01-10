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

package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.optimization.BarzilaiBorweinUpdater;
import org.apache.ignite.ml.optimization.GradientDescent;
import org.apache.ignite.ml.optimization.LeastSquaresGradientFunction;
import org.apache.ignite.ml.optimization.SimpleUpdater;

/**
 * Linear regression trainer based on least squares loss function and gradient descent optimization algorithm.
 */
public class LinearRegressionSGDTrainer implements Trainer<LinearRegressionModel, Matrix> {
    /**
     * Gradient descent optimizer.
     */
    private final GradientDescent gradientDescent;

    /** */
    public LinearRegressionSGDTrainer(GradientDescent gradientDescent) {
        this.gradientDescent = gradientDescent;
    }

    /** */
    public LinearRegressionSGDTrainer(int maxIterations, double convergenceTol) {
        this.gradientDescent = new GradientDescent(new LeastSquaresGradientFunction(), new BarzilaiBorweinUpdater())
            .withMaxIterations(maxIterations)
            .withConvergenceTol(convergenceTol);
    }

    /** */
    public LinearRegressionSGDTrainer(int maxIterations, double convergenceTol, double learningRate) {
        this.gradientDescent = new GradientDescent(new LeastSquaresGradientFunction(), new SimpleUpdater(learningRate))
            .withMaxIterations(maxIterations)
            .withConvergenceTol(convergenceTol);
    }

    /**
     * {@inheritDoc}
     */
    @Override public LinearRegressionModel train(Matrix data) {
        Vector variables = gradientDescent.optimize(data, data.likeVector(data.columnSize()));
        Vector weights = variables.viewPart(1, variables.size() - 1);

        double intercept = variables.get(0);

        return new LinearRegressionModel(weights, intercept);
    }
}
