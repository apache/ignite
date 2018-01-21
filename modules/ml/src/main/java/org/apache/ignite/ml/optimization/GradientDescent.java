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

package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.FunctionVector;
import org.apache.ignite.ml.optimization.util.SparseDistributedMatrixMapReducer;

/**
 * Gradient descent optimizer.
 */
public class GradientDescent {
    /**
     * Function which computes gradient of the loss function at any given point.
     */
    private final GradientFunction lossGradient;

    /**
     * Weights updater applied on every gradient descent step to decide how weights should be changed.
     */
    private final Updater updater;

    /**
     * Max number of gradient descent iterations.
     */
    private int maxIterations = 1000;

    /**
     * Convergence tolerance is condition which decides iteration termination.
     */
    private double convergenceTol = 1e-8;

    /**
     * New gradient descent instance based of loss function and updater.
     *
     * @param lossGradient Function which computes gradient of the loss function at any given point
     * @param updater Weights updater applied on every gradient descent step to decide how weights should be changed
     */
    public GradientDescent(GradientFunction lossGradient, Updater updater) {
        this.lossGradient = lossGradient;
        this.updater = updater;
    }

    /**
     * Sets max number of gradient descent iterations.
     *
     * @param maxIterations Max number of gradient descent iterations
     * @return This gradient descent instance
     */
    public GradientDescent withMaxIterations(int maxIterations) {
        assert maxIterations >= 0;

        this.maxIterations = maxIterations;

        return this;
    }

    /**
     * Sets convergence tolerance.
     *
     * @param convergenceTol Condition which decides iteration termination
     * @return This gradient descent instance
     */
    public GradientDescent withConvergenceTol(double convergenceTol) {
        assert convergenceTol >= 0;

        this.convergenceTol = convergenceTol;

        return this;
    }

    /**
     * Computes point where loss function takes minimal value.
     *
     * @param data Inputs parameters of loss function
     * @param initWeights Initial weights
     * @return Point where loss function takes minimal value
     */
    public Vector optimize(Matrix data, Vector initWeights) {
        Vector weights = initWeights, oldWeights = null, oldGradient = null;
        IgniteFunction<Vector, Vector> gradientFunction = getLossGradientFunction(data);

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            Vector gradient = gradientFunction.apply(weights);
            Vector newWeights = updater.compute(oldWeights, oldGradient, weights, gradient, iteration);

            if (isConverged(weights, newWeights))
                return newWeights;
            else {
                oldGradient = gradient;
                oldWeights = weights;
                weights = newWeights;
            }
        }
        return weights;
    }

    /**
     * Calculates gradient based in distributed matrix using {@link SparseDistributedMatrixMapReducer}.
     *
     * @param data Distributed matrix
     * @param weights Point to calculate gradient
     * @return Gradient
     */
    private Vector calculateDistributedGradient(SparseDistributedMatrix data, Vector weights) {
        SparseDistributedMatrixMapReducer mapReducer = new SparseDistributedMatrixMapReducer(data);
        return mapReducer.mapReduce(
            (matrix, args) -> {
                Matrix inputs = extractInputs(matrix);
                Vector groundTruth = extractGroundTruth(matrix);

                return lossGradient.compute(inputs, groundTruth, args);
            },
            gradients -> {
                int cnt = 0;
                Vector resGradient = new DenseLocalOnHeapVector(data.columnSize());

                for (Vector gradient : gradients) {
                    if (gradient != null) {
                        resGradient = resGradient.plus(gradient);
                        cnt++;
                    }
                }

                return resGradient.divide(cnt);
            },
            weights);
    }

    /**
     * Tests if gradient descent process converged.
     *
     * @param weights Weights
     * @param newWeights New weights
     * @return {@code true} if process has converged, otherwise {@code false}
     */
    private boolean isConverged(Vector weights, Vector newWeights) {
        if (convergenceTol == 0)
            return false;
        else {
            double solutionVectorDiff = weights.minus(newWeights).kNorm(2.0);
            return solutionVectorDiff < convergenceTol * Math.max(newWeights.kNorm(2.0), 1.0);
        }
    }

    /**
     * Extracts first column with ground truth from the data set matrix.
     *
     * @param data data to build model
     * @return Ground truth vector
     */
    private Vector extractGroundTruth(Matrix data) {
        return data.getCol(0);
    }

    /**
     * Extracts all inputs from data set matrix and updates matrix so that first column contains value 1.0.
     *
     * @param data data to build model
     * @return Inputs matrix
     */
    private Matrix extractInputs(Matrix data) {
        data = data.copy();
        data.assignColumn(0, new FunctionVector(data.rowSize(), row -> 1.0));
        return data;
    }

    /** Makes carrying of the gradient function and fixes data matrix. */
    private IgniteFunction<Vector, Vector> getLossGradientFunction(Matrix data) {
        if (data instanceof SparseDistributedMatrix) {
            SparseDistributedMatrix distributedMatrix = (SparseDistributedMatrix)data;

            if (distributedMatrix.getStorage().storageMode() == StorageConstants.ROW_STORAGE_MODE)
                return weights -> calculateDistributedGradient(distributedMatrix, weights);
        }

        Matrix inputs = extractInputs(data);
        Vector groundTruth = extractGroundTruth(data);

        return weights -> lossGradient.compute(inputs, groundTruth, weights);
    }
}
