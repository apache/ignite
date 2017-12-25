package org.apache.ignite.ml.optimization;

import java.util.Random;
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
     * Flag which reflects if stochastic gradient descent will be used.
     */
    private boolean stochastic = false;

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
     * Sets batch fraction.
     *
     * @param stochastic Flag which reflects if stochastic gradient descent will be used
     * @return This gradient descent instance
     */
    public GradientDescent withStochastic(boolean stochastic) {
        this.stochastic = stochastic;
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
     * @param inputs Inputs parameters of loss function
     * @param groundTruth Ground truth parameters of loss function
     * @param initialWeights Initial weights
     * @return Point where loss function takes minimal value
     */
    public Vector optimize(Matrix inputs, Vector groundTruth, Vector initialWeights) {
        Vector weights = initialWeights, oldWeights = null, oldGradient = null;
        for (int iteration = 0; iteration < maxIterations; iteration++) {
            Batch batch = computeBatch(inputs, groundTruth);
            Vector gradient = lossGradient.compute(batch.getInputs(), batch.getGroundTruth(), weights);
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
        if (stochastic && inputs.rowSize() > 0) {
            Random random = new Random();
            int index = random.nextInt(inputs.rowSize());
            batchInputs = batchInputs.like(1, batchInputs.columnSize());
            batchGroundTruth = batchGroundTruth.like(1);
            batchInputs.assignRow(0, inputs.getRow(index));
            batchGroundTruth.set(0, groundTruth.get(index));
        }
        return new Batch(batchInputs, batchGroundTruth);
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
