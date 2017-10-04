package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;


public class DistributedOLSMultipleLinearRegression extends AbstractMultipleLinearRegression {
    @Override
    protected Vector calculateBeta() {
        return null;
    }

    @Override
    protected Matrix calculateBetaVariance() {
        return null;
    }
}
