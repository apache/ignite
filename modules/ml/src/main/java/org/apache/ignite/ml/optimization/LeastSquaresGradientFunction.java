package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;

/**
 * Function which computes gradient of least square loss function.
 */
public class LeastSquaresGradientFunction implements GradientFunction {

    /**
     * {@inheritDoc}
     */
    @Override public Vector compute(Matrix inputs, Vector groundTruth, Vector point) {
        return inputs.transpose().times(inputs.times(point).minus(groundTruth));
    }
}