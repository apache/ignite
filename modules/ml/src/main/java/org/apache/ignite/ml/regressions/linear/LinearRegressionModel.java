package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;

/**
 * Simple linear regression model which predicts result Y as a linear combination of input variables: Y = b * X.
 */
public class LinearRegressionModel implements Model<Vector, Double> {

    /** */
    private final Vector weights;

    private final double intercept;

    /** */
    LinearRegressionModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Double apply(Vector input) {
        return input.dot(weights) + intercept;
    }

    /** */
    public Vector getWeights() {
        return weights;
    }

    /** */
    public double getIntercept() {
        return intercept;
    }
}