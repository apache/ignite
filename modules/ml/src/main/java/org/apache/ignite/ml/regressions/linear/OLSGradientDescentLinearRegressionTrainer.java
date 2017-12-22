package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.FunctionVector;
import org.apache.ignite.ml.regressions.linear.gradient.DifferentiableFunction;
import org.apache.ignite.ml.regressions.linear.gradient.GradientDescentOptimizer;
import org.apache.ignite.ml.regressions.linear.gradient.GradientDescentOptimizerWithFixedLearningRate;

/**
 * Trainer which builds {@link LinearRegressionModel} based on {@link OLSLinearRegressionLossFunction} loss function and
 * {@link GradientDescentOptimizerWithFixedLearningRate} optimization method. Be aware that convergence in this method
 * is not guaranteed.
 */
public class OLSGradientDescentLinearRegressionTrainer implements Trainer<LinearRegressionModel, Matrix> {

    /**
     * Termination tolerance for loss function.
     */
    private final double tolerance;

    /**
     * Learning Rate used on every gradient descent step to choose distance to move.
     */
    private final double learningRate;

    /**
     * Maximal number of iterations.
     */
    private final int maxIterations;

    /** */
    public OLSGradientDescentLinearRegressionTrainer(double tolerance, double learningRate, int maxIterations) {
        this.tolerance = tolerance;
        this.learningRate = learningRate;
        this.maxIterations = maxIterations;
    }

    /**
     * Accepts train data set as a matrix where first column contains result (Y) and the other columns contains X and
     * produces {@link LinearRegressionModel}.
     *
     * @param data data to build model
     * @return linear regression model
     */
    @Override public LinearRegressionModel train(Matrix data) {
        Vector y = extractY(data);
        Matrix x = extractX(data);

        DifferentiableFunction objectiveFunction = new OLSLinearRegressionLossFunction(x, y);
        GradientDescentOptimizer gdo = new GradientDescentOptimizerWithFixedLearningRate(objectiveFunction, tolerance,
            maxIterations, learningRate);
        Vector weights = gdo.minimize();

        return new LinearRegressionModel(weights);
    }

    /**
     * Extracts first column with result (Y) from the data set matrix.
     *
     * @param data data to build model
     * @return Y vector
     */
    private Vector extractY(Matrix data) {
        return data.getCol(0);
    }

    /**
     * Extracts all X from data set matrix and updates matrix so that first column contains value 1.0.
     *
     * @param data data to build model
     * @return X matrix
     */
    private Matrix extractX(Matrix data) {
        data.assignColumn(0, new FunctionVector(data.rowSize(), row -> 1.0));
        return data;
    }

    /**
     * Loss function used in Ordinary Least Square regression: || Y - X*w || ^ 2.
     */
    private static class OLSLinearRegressionLossFunction implements DifferentiableFunction {

        /** */
        private final Matrix x;

        /** */
        private final Vector y;

        /** */
        OLSLinearRegressionLossFunction(Matrix x, Vector y) {
            this.x = x;
            this.y = y;
        }

        /**
         * {@inheritDoc}
         */
        @Override public double calculate(Vector vars) {
            return Math.pow(y.minus(x.times(vars)).kNorm(2.0), 2.0);
        }

        /**
         * {@inheritDoc}
         */
        @Override public Vector gradient(Vector vars) {
            return x.transpose().times(y.minus(x.times(vars))).times(-2.0);
        }

        /**
         * {@inheritDoc}
         */
        @Override public int getNvars() {
            return x.columnSize();
        }
    }
}