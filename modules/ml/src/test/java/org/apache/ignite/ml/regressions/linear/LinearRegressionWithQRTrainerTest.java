package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;

/**
 * Tests for {@link LinearRegressionWithQRTrainer}.
 */
public class LinearRegressionWithQRTrainerTest extends AbstractLinearRegressionTrainerTest {

    /** */
    @Override Trainer<LinearRegressionModel, Matrix> trainer() {
        return new LinearRegressionWithQRTrainer();
    }
}