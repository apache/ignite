package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;

/**
 * Tests for {@link LinearRegressionWithSGDTrainer}.
 */
public class LinearRegressionWithSGDTrainerTest extends AbstractLinearRegressionTrainerTest {

    /** */
    @Override Trainer<LinearRegressionModel, Matrix> trainer() {
        return new LinearRegressionWithSGDTrainer(100_000, false, 1e-12);
    }

    /** */
    @Override double getPrecision() {
        return 1e-2;
    }
}
