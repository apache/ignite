package org.apache.ignite.ml.optimization;

import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;

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
     * Fraction of the data used on every gradient descent step as a batch. If batch fraction is less than 1 stochastic
     * gradient descent is used.
     */
    private double batchFraction = 1.0;

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
        if (maxIterations < 0) {
            throw new IllegalArgumentException("Number of iterations must be non-negative but got " + maxIterations);
        }
        this.maxIterations = maxIterations;
        return this;
    }

    /**
     * Sets batch fraction.
     *
     * @param batchFraction Fraction of the data used on every gradient descent step as a batch
     * @return This gradient descent instance
     */
    public GradientDescent withBatchFraction(double batchFraction) {
        if (batchFraction <= 0 || batchFraction > 1) {
            throw new IllegalArgumentException("Fraction for batch SGD must be in range (0, 1] but got "
                + batchFraction);
        }
        this.batchFraction = batchFraction;
        return this;
    }

    /**
     * Sets convergence tolerance.
     *
     * @param convergenceTol Condition which decides iteration termination
     * @return This gradient descent instance
     */
    public GradientDescent withConvergenceTol(double convergenceTol) {
        if (convergenceTol < 0) {
            throw new IllegalArgumentException("Convergence tolerance must be non-negative but got " + convergenceTol);
        }
        this.convergenceTol = convergenceTol;
        return this;
    }

    /**
     * Computes point where loss function takes minimal value.
     *
     * @param inputs Inputs parameters of loss function
     * @param groundTruth Ground truth parameters of loss function
     * @param initialWeights Initial weights
     * @return Point where loss function takes minimal value
     */
    public Vector optimize(Matrix inputs, Vector groundTruth, Vector initialWeights) {
        Vector weights = initialWeights, oldWeights = null, oldGradient = null;
        for (int iteration = 0; iteration < maxIterations; iteration++) {
            IgniteBiTuple<Matrix, Vector> batch = batch(inputs, groundTruth, iteration);
            Vector gradient = lossGradient.compute(batch.get1(), batch.get2(), weights);
            Vector newWeights = updater.compute(oldWeights, oldGradient, weights, gradient, iteration);
            if (newWeights.minus(weights).kNorm(2.0) <= convergenceTol) {
                return newWeights;
            }
            else {
                oldGradient = gradient;
                oldWeights = weights;
                weights = newWeights;
            }
        }
        return weights;
    }

    /**
     * Extracts subset of inputs and ground truth parameters in accordance with specified batch fraction.
     *
     * @param inputs Inputs parameters of loss function
     * @param groundTruth Ground truth parameters of loss function
     * @param iteration Number of current iteration
     * @return Batch with inputs and ground truth parameters
     */
    IgniteBiTuple<Matrix, Vector> batch(Matrix inputs, Vector groundTruth, int iteration) {
        Matrix batchInputs = inputs;
        Vector batchGroundTruth = groundTruth;
        if (batchFraction != 1.0) {
            int batchSize = (int)(inputs.rowSize() * batchFraction);
            batchSize = batchSize == 0 ? 1 : batchSize;
            int nvars = inputs.columnSize();
            int nobs = inputs.rowSize();
            batchInputs = inputs.like(batchSize, nvars);
            batchGroundTruth = groundTruth.like(batchSize);
            int from = (batchSize * iteration) % nobs;
            int to = (from + batchSize) % nobs;
            if (to > from) {
                for (int i = 0; i < batchSize; i++) {
                    batchInputs.assignRow(i, inputs.getRow(from + i));
                    batchGroundTruth.set(i, groundTruth.get(from + i));
                }
            }
            else {
                int i = 0;
                for (; i < nobs - from; i++) {
                    batchInputs.assignRow(i, inputs.getRow(from + i));
                    batchGroundTruth.set(i, groundTruth.get(from + i));
                }
                for (; i < batchSize; i++) {
                    batchInputs.assignRow(i, inputs.getRow(i - nobs + from));
                    batchGroundTruth.set(i, groundTruth.get(i - nobs + from));
                }
            }
        }
        return new IgniteBiTuple<>(batchInputs, batchGroundTruth);
    }
}
