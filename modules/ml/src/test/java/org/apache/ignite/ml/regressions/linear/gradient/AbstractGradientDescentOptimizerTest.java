package org.apache.ignite.ml.regressions.linear.gradient;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link AbstractGradientDescentOptimizer}.
 */
public class AbstractGradientDescentOptimizerTest {

    /** */
    private static final double TOLERANCE = 1e-13;

    /** */
    private static final Vector[] vars = new Vector[] {
        new DenseLocalOnHeapVector(new double[] {0, 0, 0, 0, 0}),
        new DenseLocalOnHeapVector(new double[] {1, 2, 3, 4, 5}),
        new DenseLocalOnHeapVector(new double[] {2, 3, 4, 5, 6}),
        new DenseLocalOnHeapVector(new double[] {3, 4, 5, 6, 7}),
        new DenseLocalOnHeapVector(new double[] {4, 5, 6, 7, 8}),
        new DenseLocalOnHeapVector(new double[] {5, 6, 7, 8, 9}),
        new DenseLocalOnHeapVector(new double[] {0, 0, 0, 0, 0}),
        new DenseLocalOnHeapVector(new double[] {0, 0, 0, 0, 0}),
        new DenseLocalOnHeapVector(new double[] {1, 1, 1, 1, 1}),
    };

    /**
     * Test checks that gradient descent stops when number of iterations exceeded and returns correct variables.
     */
    @Test
    public void testOptimizeAllIterations() {
        AbstractGradientDescentOptimizer gradientDescentOptimizer = new TestAbstractGradientDescentOptimizer(
            new TestDifferentialFunction(), TOLERANCE, 6);
        Vector result = gradientDescentOptimizer.optimize(true);
        TestUtils.assertEquals(vars[5].toMatrix(true), result.toMatrix(true));
    }

    /**
     * Test checks that gradient descent stops when the tolerance is satisfied.
     */
    @Test
    public void testOptimizeTolerance() {
        AbstractGradientDescentOptimizer gradientDescentOptimizer = new TestAbstractGradientDescentOptimizer(
            new TestDifferentialFunction(), TOLERANCE, 100);
        Vector result = gradientDescentOptimizer.optimize(true);
        TestUtils.assertEquals(vars[7].toMatrix(true), result.toMatrix(true));
    }

    /**
     * Testing implementation of {@link AbstractGradientDescentOptimizer} used in tests in this class.
     */
    private static class TestAbstractGradientDescentOptimizer extends AbstractGradientDescentOptimizer {

        /** */
        TestAbstractGradientDescentOptimizer(
            DifferentiableFunction function, double tolerance, int maxIterations) {
            super(function, tolerance, maxIterations);
        }

        /** */
        @Override protected Vector makeGradientStep(boolean maximize, int iteration) {
            Matrix expectedVector;
            if (iteration == 0) {
                expectedVector = new DenseLocalOnHeapVector(new double[5]).toMatrix(true);
            }
            else {
                expectedVector = vars[iteration - 1].toMatrix(true);
            }
            TestUtils.assertEquals(expectedVector, getVars().toMatrix(true));
            return vars[iteration];
        }
    }

    /**
     * Testing implementation of differential function used in tests in this class.
     */
    private static class TestDifferentialFunction implements DifferentiableFunction {

        /** */
        @Override public double calculate(Vector vars) {
            return vars.kNorm(2.0);
        }

        /** */
        @Override public Vector gradient(Vector vars) {
            return new DenseLocalOnHeapVector(new double[] {0, 0, 0, 0, 0});
        }

        /** */
        @Override public int getNvars() {
            return 5;
        }
    }
}
