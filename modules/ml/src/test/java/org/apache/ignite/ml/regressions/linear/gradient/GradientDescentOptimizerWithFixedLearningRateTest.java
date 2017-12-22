package org.apache.ignite.ml.regressions.linear.gradient;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link GradientDescentOptimizerWithFixedLearningRate}.
 */
public class GradientDescentOptimizerWithFixedLearningRateTest {

    /** */
    private static double TOLERANCE = 1e-20;

    /** */
    private static int MAX_ITERATIONS = 10000;

    /** */
    private static double LEARNING_RATE = 1e-2;

    /**
     * Test checks that optimizer finds a minimum of function {@code (x - 2) ^ 2}.
     */
    @Test
    public void testMinimize() {
        GradientDescentOptimizerWithFixedLearningRate optimizer = new GradientDescentOptimizerWithFixedLearningRate(
            new TestDifferentialFunction1(), TOLERANCE, MAX_ITERATIONS, LEARNING_RATE);
        Vector result = optimizer.minimize();
        TestUtils.assertEquals(result.get(0), 2, 1e-6);
    }

    /**
     * Test checks that optimizer finds a minimum of function {@code -(x - 2) ^ 2}.
     */
    @Test
    public void testMaximize() {
        GradientDescentOptimizerWithFixedLearningRate optimizer = new GradientDescentOptimizerWithFixedLearningRate(
            new TestDifferentialFunction2(), TOLERANCE, MAX_ITERATIONS, LEARNING_RATE);
        Vector result = optimizer.maximize();
        TestUtils.assertEquals(result.get(0), 2, 1e-6);
    }

    /**
     * Testing implementation of differential function {@code (x - 2) ^ 2}.
     */
    private static class TestDifferentialFunction1 implements DifferentiableFunction {

        /** */
        @Override public double calculate(Vector vars) {
            double x = vars.get(0);
            return Math.pow(x - 2, 2);
        }

        /** */
        @Override public Vector gradient(Vector vars) {
            double x = vars.get(0);
            return new DenseLocalOnHeapVector(new double[] {2 * (x - 2)});
        }

        /** */
        @Override public int getNvars() {
            return 1;
        }
    }

    /**
     * Testing implementation of differential function {@code -(x - 2) ^ 2}.
     */
    private static class TestDifferentialFunction2 implements DifferentiableFunction {

        /** */
        @Override public double calculate(Vector vars) {
            double x = vars.get(0);
            return -Math.pow(x - 2, 2);
        }

        /** */
        @Override public Vector gradient(Vector vars) {
            double x = vars.get(0);
            return new DenseLocalOnHeapVector(new double[] {-2 * (x - 2)});
        }

        /** */
        @Override public int getNvars() {
            return 1;
        }
    }
}
