package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link GradientDescent}.
 */
public class GradientDescentTest {
    /** */
    private static final double PRECISION = 1e-6;

    /**
     * Test gradient descent optimization on function y = x^2 with gradient function 2 * x.
     */
    @Test
    public void testOptimize() {
        GradientDescent gradientDescent = new GradientDescent(
            (inputs, groundTruth, point) -> point.times(2),
            new SimpleUpdater(0.01)
        );
        Vector result = gradientDescent.optimize(new DenseLocalOnHeapMatrix(new double[1][1]),
            new DenseLocalOnHeapVector(new double[]{ 2.0 }));
        TestUtils.assertEquals(0, result.get(0), PRECISION);
    }

    /**
     * Test gradient descent optimization on function y = (x - 2)^2 with gradient function 2 * (x - 2).
     */
    @Test
    public void testOptimizeWithOffset() {
        GradientDescent gradientDescent = new GradientDescent(
            (inputs, groundTruth, point) -> point.minus(new DenseLocalOnHeapVector(new double[]{ 2.0 })).times(2.0),
            new SimpleUpdater(0.01)
        );
        Vector result = gradientDescent.optimize(new DenseLocalOnHeapMatrix(new double[1][1]),
            new DenseLocalOnHeapVector(new double[]{ 2.0 }));
        TestUtils.assertEquals(2, result.get(0), PRECISION);
    }
}
