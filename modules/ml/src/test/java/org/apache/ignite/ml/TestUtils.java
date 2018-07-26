/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml;

import java.util.stream.IntStream;
import org.apache.ignite.ml.math.Precision;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.junit.Assert;

import static org.junit.Assert.assertTrue;

/** */
public class TestUtils {
    /**
     * Collection of static methods used in math unit tests.
     */
    private TestUtils() {
        super();
    }

    /**
     * Verifies that expected and actual are within delta, or are both NaN or
     * infinities of the same sign.
     *
     * @param exp Expected value.
     * @param actual Actual value.
     * @param delta Maximum allowed delta between {@code exp} and {@code actual}.
     */
    public static void assertEquals(double exp, double actual, double delta) {
        Assert.assertEquals(null, exp, actual, delta);
    }

    /**
     * Verifies that expected and actual are within delta, or are both NaN or
     * infinities of the same sign.
     */
    public static void assertEquals(String msg, double exp, double actual, double delta) {
        // Check for NaN.
        if (Double.isNaN(exp))
            Assert.assertTrue("" + actual + " is not NaN.", Double.isNaN(actual));
        else
            Assert.assertEquals(msg, exp, actual, delta);
    }

    /**
     * Verifies that two double arrays have equal entries, up to tolerance.
     */
    public static void assertEquals(double exp[], double observed[], double tolerance) {
        assertEquals("Array comparison failure", exp, observed, tolerance);
    }

    /**
     * Asserts that all entries of the specified vectors are equal to within a
     * positive {@code delta}.
     *
     * @param msg The identifying message for the assertion error (can be {@code null}).
     * @param exp Expected value.
     * @param actual Actual value.
     * @param delta The maximum difference between the entries of the expected and actual vectors for which both entries
     * are still considered equal.
     */
    public static void assertEquals(final String msg,
        final double[] exp, final Vector actual, final double delta) {
        final String msgAndSep = msg.equals("") ? "" : msg + ", ";

        Assert.assertEquals(msgAndSep + "dimension", exp.length, actual.size());

        for (int i = 0; i < exp.length; i++)
            Assert.assertEquals(msgAndSep + "entry #" + i, exp[i], actual.getX(i), delta);
    }

    /**
     * Asserts that all entries of the specified vectors are equal to within a
     * positive {@code delta}.
     *
     * @param msg The identifying message for the assertion error (can be {@code null}).
     * @param exp Expected value.
     * @param actual Actual value.
     * @param delta The maximum difference between the entries of the expected and actual vectors for which both entries
     * are still considered equal.
     */
    public static void assertEquals(final String msg,
        final Vector exp, final Vector actual, final double delta) {
        final String msgAndSep = msg.equals("") ? "" : msg + ", ";

        Assert.assertEquals(msgAndSep + "dimension", exp.size(), actual.size());

        final int dim = exp.size();
        for (int i = 0; i < dim; i++)
            Assert.assertEquals(msgAndSep + "entry #" + i, exp.getX(i), actual.getX(i), delta);
    }

    /**
     * Verifies that two matrices are close (1-norm).
     *
     * @param msg The identifying message for the assertion error.
     * @param exp Expected matrix.
     * @param actual Actual matrix.
     * @param tolerance Comparison tolerance value.
     */
    public static void assertEquals(String msg, Matrix exp, Matrix actual, double tolerance) {
        Assert.assertNotNull(msg + "\nObserved should not be null", actual);

        if (exp.columnSize() != actual.columnSize() || exp.rowSize() != actual.rowSize()) {
            String msgBuff = msg + "\nObserved has incorrect dimensions." +
                "\nobserved is " + actual.rowSize() +
                " x " + actual.columnSize() +
                "\nexpected " + exp.rowSize() +
                " x " + exp.columnSize();

            Assert.fail(msgBuff);
        }

        Matrix delta = exp.minus(actual);

        if (TestUtils.maximumAbsoluteRowSum(delta) >= tolerance) {
            String msgBuff = msg + "\nExpected: " + exp +
                "\nObserved: " + actual +
                "\nexpected - observed: " + delta;

            Assert.fail(msgBuff);
        }
    }

    /**
     * Verifies that two matrices are equal.
     *
     * @param exp Expected matrix.
     * @param actual Actual matrix.
     */
    public static void assertEquals(Matrix exp, Matrix actual) {
        Assert.assertNotNull("Observed should not be null", actual);

        if (exp.columnSize() != actual.columnSize() || exp.rowSize() != actual.rowSize()) {
            String msgBuff = "Observed has incorrect dimensions." +
                "\nobserved is " + actual.rowSize() +
                " x " + actual.columnSize() +
                "\nexpected " + exp.rowSize() +
                " x " + exp.columnSize();

            Assert.fail(msgBuff);
        }

        for (int i = 0; i < exp.rowSize(); ++i)
            for (int j = 0; j < exp.columnSize(); ++j) {
                double eij = exp.getX(i, j);
                double aij = actual.getX(i, j);

                // TODO: IGNITE-5824, Check precision here.
                Assert.assertEquals(eij, aij, 0.0);
            }
    }

    /**
     * Verifies that two double arrays are close (sup norm).
     *
     * @param msg The identifying message for the assertion error.
     * @param exp Expected array.
     * @param actual Actual array.
     * @param tolerance Comparison tolerance value.
     */
    public static void assertEquals(String msg, double[] exp, double[] actual, double tolerance) {
        StringBuilder out = new StringBuilder(msg);

        if (exp.length != actual.length) {
            out.append("\n Arrays not same length. \n");
            out.append("expected has length ");
            out.append(exp.length);
            out.append(" observed length = ");
            out.append(actual.length);
            Assert.fail(out.toString());
        }

        boolean failure = false;

        for (int i = 0; i < exp.length; i++)
            if (!Precision.equalsIncludingNaN(exp[i], actual[i], tolerance)) {
                failure = true;
                out.append("\n Elements at index ");
                out.append(i);
                out.append(" differ. ");
                out.append(" expected = ");
                out.append(exp[i]);
                out.append(" observed = ");
                out.append(actual[i]);
            }

        if (failure)
            Assert.fail(out.toString());
    }

    /**
     * Verifies that two float arrays are close (sup norm).
     *
     * @param msg The identifying message for the assertion error.
     * @param exp Expected array.
     * @param actual Actual array.
     * @param tolerance Comparison tolerance value.
     */
    public static void assertEquals(String msg, float[] exp, float[] actual, float tolerance) {
        StringBuilder out = new StringBuilder(msg);

        if (exp.length != actual.length) {
            out.append("\n Arrays not same length. \n");
            out.append("expected has length ");
            out.append(exp.length);
            out.append(" observed length = ");
            out.append(actual.length);
            Assert.fail(out.toString());
        }

        boolean failure = false;

        for (int i = 0; i < exp.length; i++)
            if (!Precision.equalsIncludingNaN(exp[i], actual[i], tolerance)) {
                failure = true;
                out.append("\n Elements at index ");
                out.append(i);
                out.append(" differ. ");
                out.append(" expected = ");
                out.append(exp[i]);
                out.append(" observed = ");
                out.append(actual[i]);
            }

        if (failure)
            Assert.fail(out.toString());
    }

    /** */
    public static double maximumAbsoluteRowSum(Matrix mtx) {
        return IntStream.range(0, mtx.rowSize()).mapToObj(mtx::viewRow).map(v -> Math.abs(v.sum())).reduce(Math::max).get();
    }

    /** */
    public static void checkIsInEpsilonNeighbourhood(Vector[] v1s, Vector[] v2s, double epsilon) {
        for (int i = 0; i < v1s.length; i++) {
            assertTrue("Not in epsilon neighbourhood (index " + i + ") ",
                v1s[i].minus(v2s[i]).kNorm(2) < epsilon);
        }
    }

    /** */
    public static void checkIsInEpsilonNeighbourhood(Vector v1, Vector v2, double epsilon) {
        checkIsInEpsilonNeighbourhood(new Vector[] {v1}, new Vector[] {v2}, epsilon);
    }

    /** */
    public static boolean checkIsInEpsilonNeighbourhoodBoolean(Vector v1, Vector v2, double epsilon) {
        try {
            checkIsInEpsilonNeighbourhood(new Vector[] {v1}, new Vector[] {v2}, epsilon);
        } catch (Throwable e) {
            return false;
        }

        return true;
    }
}
