package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link LinearRegressionModel}.
 */
public class LinearRegressionModelTest {

    /** */
    private static final double PRECISION = 1e-6;

    /** */
    @Test
    public void testPredict() {
        Vector weights = new DenseLocalOnHeapVector(new double[]{2.0, 3.0});
        LinearRegressionModel model = new LinearRegressionModel(weights, 1.0);

        Vector observation = new DenseLocalOnHeapVector(new double[]{1.0, 1.0});
        TestUtils.assertEquals(1.0 + 2.0 * 1.0 + 3.0 * 1.0, model.apply(observation), PRECISION);

        observation = new DenseLocalOnHeapVector(new double[]{2.0, 1.0});
        TestUtils.assertEquals(1.0 + 2.0 * 2.0 + 3.0 * 1.0, model.apply(observation), PRECISION);

        observation = new DenseLocalOnHeapVector(new double[]{1.0, 2.0});
        TestUtils.assertEquals(1.0 + 2.0 * 1.0 + 3.0 * 2.0, model.apply(observation), PRECISION);

        observation = new DenseLocalOnHeapVector(new double[]{-2.0, 1.0});
        TestUtils.assertEquals(1.0 - 2.0 * 2.0 + 3.0 * 1.0, model.apply(observation), PRECISION);

        observation = new DenseLocalOnHeapVector(new double[]{1.0, -2.0});
        TestUtils.assertEquals(1.0 + 2.0 * 1.0 - 3.0 * 2.0, model.apply(observation), PRECISION);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void testPredictOnAnObservationWithWrongCardinality() {
        Vector weights = new DenseLocalOnHeapVector(new double[]{2.0, 3.0});
        LinearRegressionModel model = new LinearRegressionModel(weights, 1.0);
        Vector observation = new DenseLocalOnHeapVector(new double[]{1.0});
        model.apply(observation);
    }
}
