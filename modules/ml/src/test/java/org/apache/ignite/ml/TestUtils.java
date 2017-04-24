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
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Precision;
import org.apache.ignite.ml.math.Vector;
import org.junit.Assert;

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
     * @param exp expected value.
     * @param actual actual value.
     * @param delta maximum allowed delta between {@code exp} and {@code actual}.
     */
    public static void assertEquals(double exp, double actual, double delta) {
        Assert.assertEquals(null, exp, actual, delta);
    }

    /**
     * Verifies that expected and actual are within delta, or are both NaN or
     * infinities of the same sign.
     */
    public static void assertEquals(String msg, double exp, double actual, double delta) {
        // check for NaN
        if(Double.isNaN(exp)){
            Assert.assertTrue("" + actual + " is not NaN.",
                Double.isNaN(actual));
        } else
            Assert.assertEquals(msg, exp, actual, delta);
    }

    /**
     * Verifies that two double arrays have equal entries, up to tolerance
     */
    public static void assertEquals(double exp[], double observed[], double tolerance) {
        assertEquals("Array comparison failure", exp, observed, tolerance);
    }

    /**
     * Asserts that all entries of the specified vectors are equal to within a
     * positive {@code delta}.
     *
     * @param msg the identifying message for the assertion error (can be
     * {@code null})
     * @param exp expected value
     * @param actual actual value
     * @param delta the maximum difference between the entries of the expected
     * and actual vectors for which both entries are still considered equal
     */
    public static void assertEquals(final String msg,
        final double[] exp, final Vector actual, final double delta) {
        final String msgAndSep = msg.equals("") ? "" : msg + ", ";
        Assert.assertEquals(msgAndSep + "dimension", exp.length,
            actual.size());
        for (int i = 0; i < exp.length; i++) {
            Assert.assertEquals(msgAndSep + "entry #" + i, exp[i],
                actual.getX(i), delta);
        }
    }

    /**
     * Asserts that all entries of the specified vectors are equal to within a
     * positive {@code delta}.
     *
     * @param msg the identifying message for the assertion error (can be
     * {@code null})
     * @param exp expected value
     * @param actual actual value
     * @param delta the maximum difference between the entries of the expected
     * and actual vectors for which both entries are still considered equal
     */
    public static void assertEquals(final String msg,
        final Vector exp, final Vector actual, final double delta) {
        final String msgAndSep = msg.equals("") ? "" : msg + ", ";
        Assert.assertEquals(msgAndSep + "dimension", exp.size(),
            actual.size());
        final int dim = exp.size();
        for (int i = 0; i < dim; i++) {
            Assert.assertEquals(msgAndSep + "entry #" + i,
                exp.getX(i), actual.getX(i), delta);
        }
    }

    /** verifies that two matrices are close (1-norm) */
    public static void assertEquals(String msg, Matrix exp, Matrix observed, double tolerance) {

        Assert.assertNotNull(msg + "\nObserved should not be null",observed);

        if (exp.columnSize() != observed.columnSize() ||
                exp.rowSize() != observed.rowSize()) {
            StringBuilder msgBuff = new StringBuilder(msg);
            msgBuff.append("\nObserved has incorrect dimensions.");
            msgBuff.append("\nobserved is " + observed.rowSize() +
                    " x " + observed.columnSize());
            msgBuff.append("\nexpected " + exp.rowSize() +
                    " x " + exp.columnSize());
            Assert.fail(msgBuff.toString());
        }

        Matrix delta = exp.minus(observed);
        if (TestUtils.maximumAbsoluteRowSum(delta) >= tolerance) {
            StringBuilder msgBuff = new StringBuilder(msg);
            msgBuff.append("\nExpected: " + exp);
            msgBuff.append("\nObserved: " + observed);
            msgBuff.append("\nexpected - observed: " + delta);
            Assert.fail(msgBuff.toString());
        }
    }

    /** verifies that two matrices are equal */
    public static void assertEquals(Matrix exp,
                                    Matrix act) {

        Assert.assertNotNull("Observed should not be null",act);

        if (exp.columnSize() != act.columnSize() ||
                exp.rowSize() != act.rowSize()) {
            StringBuilder msgBuff = new StringBuilder();
            msgBuff.append("Observed has incorrect dimensions.");
            msgBuff.append("\nobserved is " + act.rowSize() +
                    " x " + act.columnSize());
            msgBuff.append("\nexpected " + exp.rowSize() +
                    " x " + exp.columnSize());
            Assert.fail(msgBuff.toString());
        }

        for (int i = 0; i < exp.rowSize(); ++i) {
            for (int j = 0; j < exp.columnSize(); ++j) {
                double eij = exp.getX(i, j);
                double aij = act.getX(i, j);
                // TODO: Check precision here.
                Assert.assertEquals(eij, aij, 0.0);
            }
        }
    }

    /** verifies that two arrays are close (sup norm) */
    public static void assertEquals(String msg, double[] exp, double[] observed, double tolerance) {
        StringBuilder out = new StringBuilder(msg);
        if (exp.length != observed.length) {
            out.append("\n Arrays not same length. \n");
            out.append("expected has length ");
            out.append(exp.length);
            out.append(" observed length = ");
            out.append(observed.length);
            Assert.fail(out.toString());
        }
        boolean failure = false;
        for (int i=0; i < exp.length; i++) {
            if (!Precision.equalsIncludingNaN(exp[i], observed[i], tolerance)) {
                failure = true;
                out.append("\n Elements at index ");
                out.append(i);
                out.append(" differ. ");
                out.append(" expected = ");
                out.append(exp[i]);
                out.append(" observed = ");
                out.append(observed[i]);
            }
        }
        if (failure)
            Assert.fail(out.toString());
    }
    
    /** verifies that two arrays are close (sup norm) */
    public static void assertEquals(String msg, float[] exp, float[] observed, float tolerance) {
        StringBuilder out = new StringBuilder(msg);
        if (exp.length != observed.length) {
            out.append("\n Arrays not same length. \n");
            out.append("expected has length ");
            out.append(exp.length);
            out.append(" observed length = ");
            out.append(observed.length);
            Assert.fail(out.toString());
        }
        boolean failure = false;
        for (int i=0; i < exp.length; i++) {
            if (!Precision.equalsIncludingNaN(exp[i], observed[i], tolerance)) {
                failure = true;
                out.append("\n Elements at index ");
                out.append(i);
                out.append(" differ. ");
                out.append(" expected = ");
                out.append(exp[i]);
                out.append(" observed = ");
                out.append(observed[i]);
            }
        }
        if (failure)
            Assert.fail(out.toString());
    }

    /** */
    public static double maximumAbsoluteRowSum(Matrix mtx) {
        return IntStream.range(0, mtx.rowSize()).mapToObj(mtx::viewRow).map(v -> Math.abs(v.sum())).reduce(Math::max).get();
    }
}
