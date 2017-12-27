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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.FunctionVector;
import org.apache.ignite.ml.util.Utils;

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
     * Fraction of data to be used for each SGD iteration.
     */
    private double miniBatchFraction = 1.0;

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
        if (maxIterations < 0)
            throw new IllegalArgumentException("Number of iterations must be non-negative but got " + maxIterations);
        this.maxIterations = maxIterations;
        return this;
    }

    /**
     * Set fraction of data to be used for each SGD iteration.
     *
     * @param miniBatchFraction Fraction of data to be used for each SGD iteration
     * @return This gradient descent instance
     */
    public GradientDescent withMiniBatchFraction(double miniBatchFraction) {
        if (miniBatchFraction <= 0 || miniBatchFraction > 1.0)
            throw new IllegalArgumentException("Fraction for mini-batch SGD must be in range (0, 1] but got "
                + miniBatchFraction);
        this.miniBatchFraction = miniBatchFraction;
        return this;
    }

    /**
     * Sets convergence tolerance.
     *
     * @param convergenceTol Condition which decides iteration termination
     * @return This gradient descent instance
     */
    public GradientDescent withConvergenceTol(double convergenceTol) {
        if (convergenceTol < 0)
            throw new IllegalArgumentException("Convergence tolerance must be non-negative but got " + convergenceTol);
        this.convergenceTol = convergenceTol;
        return this;
    }

    /**
     * Computes point where loss function takes minimal value.
     *
     * @param data Inputs parameters of loss function
     * @param initialWeights Initial weights
     * @return Point where loss function takes minimal value
     */
    public Vector optimize(Matrix data, Vector initialWeights) {
        Vector weights = initialWeights, oldWeights = null, oldGradient = null;
        if (data instanceof SparseDistributedMatrix) {
            for (int iteration = 0; iteration < maxIterations; iteration++) {
                Vector gradient = calculateDistributedGradient((SparseDistributedMatrix) data, weights);
                Vector newWeights = updater.compute(oldWeights, oldGradient, weights, gradient, iteration);
                if (isConverged(weights, newWeights))
                    return newWeights;
                else {
                    oldGradient = gradient;
                    oldWeights = weights;
                    weights = newWeights;
                }
            }
        }
        else {
            Matrix inputs = extractInputs(data);
            Vector groundTruth = extractGroundTruth(data);
            for (int iteration = 0; iteration < maxIterations; iteration++) {
                Vector gradient = lossGradient.compute(inputs, groundTruth, weights);
                Vector newWeights = updater.compute(oldWeights, oldGradient, weights, gradient, iteration);
                if (isConverged(weights, newWeights))
                    return newWeights;
                else {
                    oldGradient = gradient;
                    oldWeights = weights;
                    weights = newWeights;
                }
            }
        }
        return weights;
    }

    private Vector calculateDistributedGradient(SparseDistributedMatrix data, Vector weights) {
        Ignite ignite = Ignition.localIgnite();
        SparseDistributedMatrixStorage storage = (SparseDistributedMatrixStorage)data.getStorage();
        String cacheName = storage.cacheName();
        int columnSize = data.columnSize();
        Collection<Vector> results = ignite
            .compute(ignite.cluster().forDataNodes(cacheName))
            .broadcast(point -> {
                Ignite ig = Ignition.localIgnite();
                Affinity<RowColMatrixKey> affinity = ig.affinity(cacheName);
                ClusterNode locNode = ig.cluster().localNode();
                Map<ClusterNode, Collection<RowColMatrixKey>> keysCToNodes = affinity.mapKeysToNodes(storage.getAllKeys());
                Collection<RowColMatrixKey> locKeys = keysCToNodes.get(locNode);
                if (locKeys != null) {
                    Matrix inputs = new DenseLocalOnHeapMatrix(locKeys.size(), columnSize);
                    Vector groundTruth = new DenseLocalOnHeapVector(locKeys.size());
                    int index = 0;
                    for (RowColMatrixKey key : locKeys) {
                        Map<Integer, Double> row = storage.cache().get(key);
                        inputs.set(index, 0, 1.0);
                        for (Map.Entry<Integer,Double> cell : row.entrySet()) {
                            if (cell.getKey().equals(0))
                                groundTruth.set(index, cell.getValue());
                            else
                                inputs.set(index, cell.getKey(), cell.getValue());
                        }
                        index++;
                    }
                    LeastSquaresGradientFunction gradientFunction = new LeastSquaresGradientFunction();
                    return gradientFunction.compute(inputs, groundTruth, point);
                }
                return null;
            }, weights);
        Vector result = new DenseLocalOnHeapVector(data.columnSize());
        int count = 0;
        for (Vector v : results) {
            if (v != null) {
                result = result.plus(v);
                count++;
            }
        }
        return result.divide(count);
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
     * Extracts subset of inputs and ground truth parameters in accordance with {@code stochastic} flag.
     *
     * @param inputs Inputs parameters of loss function
     * @param groundTruth Ground truth parameters of loss function
     * @return Batch with inputs and ground truth parameters
     */
    private Batch computeBatch(Matrix inputs, Vector groundTruth) {
        Matrix batchInputs = inputs;
        Vector batchGroundTruth = groundTruth;
        if (inputs.rowSize() > 0 && miniBatchFraction < 1.0) {
            int miniBatchSize = (int)(inputs.rowSize() * miniBatchFraction);
            if (miniBatchSize == 0)
                miniBatchSize = 1;
            int[] indexes = Utils.selectKDistinct(inputs.rowSize(), miniBatchSize);
            batchInputs = batchInputs.like(miniBatchSize, batchInputs.columnSize());
            batchGroundTruth = batchGroundTruth.like(miniBatchSize);
            for (int i = 0; i < miniBatchSize; i++) {
                batchInputs.assignRow(i, inputs.getRow(indexes[i]));
                batchGroundTruth.set(i, groundTruth.get(indexes[i]));
            }
        }
        return new Batch(batchInputs, batchGroundTruth);
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

    /**
     * Batch which encapsulates subset in input data (inputs and ground truth).
     */
    private static class Batch {

        /** */
        private final Matrix inputs;

        /** */
        private final Vector groundTruth;

        /** */
        public Batch(Matrix inputs, Vector groundTruth) {
            this.inputs = inputs;
            this.groundTruth = groundTruth;
        }

        /** */
        public Matrix getInputs() {
            return inputs;
        }

        /** */
        public Vector getGroundTruth() {
            return groundTruth;
        }
    }
}
