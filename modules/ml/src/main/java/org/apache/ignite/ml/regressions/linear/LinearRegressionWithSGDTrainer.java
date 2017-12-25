package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.FunctionVector;
import org.apache.ignite.ml.optimization.BarzilaiBorweinUpdater;
import org.apache.ignite.ml.optimization.GradientDescent;
import org.apache.ignite.ml.optimization.LeastSquaresGradientFunction;

/**
 * Linear regression trainer based on gradient descent algorithm.
 */
public class LinearRegressionWithSGDTrainer implements Trainer<LinearRegressionModel, Matrix> {

    /**
     * Gradient descent optimizer.
     */
    private final GradientDescent gradientDescent;

    /** */
    public LinearRegressionWithSGDTrainer(GradientDescent gradientDescent) {
        this.gradientDescent = gradientDescent;
    }

    /** */
    public LinearRegressionWithSGDTrainer(int maxIterations, double batchFraction, double convergenceTol) {
        this(new GradientDescent(new LeastSquaresGradientFunction(), new BarzilaiBorweinUpdater())
            .withMaxIterations(maxIterations)
            .withBatchFraction(batchFraction)
            .withConvergenceTol(convergenceTol));
    }

    /**
     * {@inheritDoc}
     */
    @Override public LinearRegressionModel train(Matrix data) {
        Vector groundTruth = extractGroundTruth(data);
        Matrix inputs = extractInputs(data);
        Vector variables = gradientDescent.optimize(inputs, groundTruth, new DenseLocalOnHeapVector(inputs.columnSize()));
        Vector weights = variables.viewPart(1, variables.size() - 1);
        double intercept = variables.get(0);
        return new LinearRegressionModel(weights, intercept);
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
        data.assignColumn(0, new FunctionVector(data.rowSize(), row -> 1.0));
        return data;
    }
}
