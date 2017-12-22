package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;

/**
 * Simple linear regression model which predicts result Y as a linear combination of input variables: Y = b * X.
 */
public class LinearRegressionModel implements Model<Vector, Double> {

    /** */
    private final Vector wVector;

    /** */
    LinearRegressionModel(Vector wVector) {
        this.wVector = wVector;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Double predict(Vector val) {
        Vector result = val.toMatrixPlusOne(true, 1.0).times(wVector);
        return result.get(0);
    }
}