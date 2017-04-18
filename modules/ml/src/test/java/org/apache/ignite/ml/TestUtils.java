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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
     */
    public static void assertEquals(double expected, double actual, double delta) {
        Assert.assertEquals(null, expected, actual, delta);
    }

    /**
     * Verifies that expected and actual are within delta, or are both NaN or
     * infinities of the same sign.
     */
    public static void assertEquals(String msg, double expected, double actual, double delta) {
        // check for NaN
        if(Double.isNaN(expected)){
            Assert.assertTrue("" + actual + " is not NaN.",
                Double.isNaN(actual));
        } else
            Assert.assertEquals(msg, expected, actual, delta);
    }

    /**
     * Verifies that the two arguments are exactly the same, either
     * both NaN or infinities of same sign, or identical floating point values.
     */
    public static void assertSame(double expected, double actual) {
     Assert.assertEquals(expected, actual, 0);
    }

    /**
     * Verifies that two double arrays have equal entries, up to tolerance
     */
    public static void assertEquals(double expected[], double observed[], double tolerance) {
        assertEquals("Array comparison failure", expected, observed, tolerance);
    }

    /**
     * Serializes an object to a bytes array and then recovers the object from the bytes array.
     * Returns the deserialized object.
     *
     * @param o  object to serialize and recover
     * @return  the recovered, deserialized object
     */
    public static Object serializeAndRecover(Object o) {
        try {
            // serialize the Object
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream so = new ObjectOutputStream(bos);
            so.writeObject(o);

            // deserialize the Object
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream si = new ObjectInputStream(bis);
            return si.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies that serialization preserves equals and hashCode.
     * Serializes the object, then recovers it and checks equals and hash code.
     *
     * @param object  the object to serialize and recover
     */
    public static void checkSerializedEquality(Object object) {
        Object object2 = serializeAndRecover(object);
        Assert.assertEquals("Equals check", object, object2);
        Assert.assertEquals("HashCode check", object.hashCode(), object2.hashCode());
    }

    /**
     * Verifies that the relative error in actual vs. expected is less than or
     * equal to relativeError.  If expected is infinite or NaN, actual must be
     * the same (NaN or infinity of the same sign).
     *
     * @param expected expected value
     * @param actual  observed value
     * @param relativeError  maximum allowable relative error
     */
    public static void assertRelativelyEquals(double expected, double actual,
            double relativeError) {
        assertRelativelyEquals(null, expected, actual, relativeError);
    }

    /**
     * Verifies that the relative error in actual vs. expected is less than or
     * equal to relativeError.  If expected is infinite or NaN, actual must be
     * the same (NaN or infinity of the same sign).
     *
     * @param msg  message to return with failure
     * @param expected expected value
     * @param actual  observed value
     * @param relativeError  maximum allowable relative error
     */
    public static void assertRelativelyEquals(String msg, double expected,
            double actual, double relativeError) {
        if (Double.isNaN(expected))
            Assert.assertTrue(msg, Double.isNaN(actual));
        else if (Double.isNaN(actual))
            Assert.assertTrue(msg, Double.isNaN(expected));
        else if (Double.isInfinite(actual) || Double.isInfinite(expected))
            Assert.assertEquals(expected, actual, relativeError);
        else if (expected == 0.0)
            Assert.assertEquals(msg, actual, expected, relativeError);
        else {
            double absError = Math.abs(expected) * relativeError;
            Assert.assertEquals(msg, expected, actual, absError);
        }
    }

    /**
     * Fails iff values does not contain a number within epsilon of x.
     *
     * @param msg  message to return with failure
     * @param values double array to search
     * @param x value sought
     * @param epsilon  tolerance
     */
    public static void assertContains(String msg, double[] values,
            double x, double epsilon) {
        for (double value : values) {
            if (Precision.equals(value, x, epsilon))
                return;
        }
        Assert.fail(msg + " Unable to find " + x);
    }

    /**
     * Fails iff values does not contain a number within epsilon of x.
     *
     * @param values double array to search
     * @param x value sought
     * @param epsilon  tolerance
     */
    public static void assertContains(double[] values, double x,
            double epsilon) {
       assertContains(null, values, x, epsilon);
    }

    /**
     * Asserts that all entries of the specified vectors are equal to within a
     * positive {@code delta}.
     *
     * @param message the identifying message for the assertion error (can be
     * {@code null})
     * @param expected expected value
     * @param actual actual value
     * @param delta the maximum difference between the entries of the expected
     * and actual vectors for which both entries are still considered equal
     */
    public static void assertEquals(final String message,
        final double[] expected, final Vector actual, final double delta) {
        final String msgAndSep = message.equals("") ? "" : message + ", ";
        Assert.assertEquals(msgAndSep + "dimension", expected.length,
            actual.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(msgAndSep + "entry #" + i, expected[i],
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
    public static void assertEquals(String msg, Matrix expected, Matrix observed, double tolerance) {

        Assert.assertNotNull(msg + "\nObserved should not be null",observed);

        if (expected.columnSize() != observed.columnSize() ||
                expected.rowSize() != observed.rowSize()) {
            StringBuilder messageBuffer = new StringBuilder(msg);
            messageBuffer.append("\nObserved has incorrect dimensions.");
            messageBuffer.append("\nobserved is " + observed.rowSize() +
                    " x " + observed.columnSize());
            messageBuffer.append("\nexpected " + expected.rowSize() +
                    " x " + expected.columnSize());
            Assert.fail(messageBuffer.toString());
        }

        Matrix delta = expected.minus(observed);
        if (TestUtils.maximumAbsoluteRowSum(delta) >= tolerance) {
            StringBuilder messageBuffer = new StringBuilder(msg);
            messageBuffer.append("\nExpected: " + expected);
            messageBuffer.append("\nObserved: " + observed);
            messageBuffer.append("\nexpected - observed: " + delta);
            Assert.fail(messageBuffer.toString());
        }
    }

    /** verifies that two matrices are equal */
    public static void assertEquals(Matrix exp,
                                    Matrix act) {

        Assert.assertNotNull("Observed should not be null",act);

        if (exp.columnSize() != act.columnSize() ||
                exp.rowSize() != act.rowSize()) {
            StringBuilder messageBuffer = new StringBuilder();
            messageBuffer.append("Observed has incorrect dimensions.");
            messageBuffer.append("\nobserved is " + act.rowSize() +
                    " x " + act.columnSize());
            messageBuffer.append("\nexpected " + exp.rowSize() +
                    " x " + exp.columnSize());
            Assert.fail(messageBuffer.toString());
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
    public static void assertEquals(String msg, double[] expected, double[] observed, double tolerance) {
        StringBuilder out = new StringBuilder(msg);
        if (expected.length != observed.length) {
            out.append("\n Arrays not same length. \n");
            out.append("expected has length ");
            out.append(expected.length);
            out.append(" observed length = ");
            out.append(observed.length);
            Assert.fail(out.toString());
        }
        boolean failure = false;
        for (int i=0; i < expected.length; i++) {
            if (!Precision.equalsIncludingNaN(expected[i], observed[i], tolerance)) {
                failure = true;
                out.append("\n Elements at index ");
                out.append(i);
                out.append(" differ. ");
                out.append(" expected = ");
                out.append(expected[i]);
                out.append(" observed = ");
                out.append(observed[i]);
            }
        }
        if (failure)
            Assert.fail(out.toString());
    }
    
    /** verifies that two arrays are close (sup norm) */
    public static void assertEquals(String msg, float[] expected, float[] observed, float tolerance) {
        StringBuilder out = new StringBuilder(msg);
        if (expected.length != observed.length) {
            out.append("\n Arrays not same length. \n");
            out.append("expected has length ");
            out.append(expected.length);
            out.append(" observed length = ");
            out.append(observed.length);
            Assert.fail(out.toString());
        }
        boolean failure = false;
        for (int i=0; i < expected.length; i++) {
            if (!Precision.equalsIncludingNaN(expected[i], observed[i], tolerance)) {
                failure = true;
                out.append("\n Elements at index ");
                out.append(i);
                out.append(" differ. ");
                out.append(" expected = ");
                out.append(expected[i]);
                out.append(" observed = ");
                out.append(observed[i]);
            }
        }
        if (failure)
            Assert.fail(out.toString());
    }

    /**
     * Computes the sum of squared deviations of <values> from <target>
     * @param values array of deviates
     * @param target value to compute deviations from
     *
     * @return sum of squared deviations
     */
    public static double sumSquareDev(double[] values, double target) {
        double sumsq = 0d;
        for (int i = 0; i < values.length; i++) {
            final double dev = values[i] - target;
            sumsq += (dev * dev);
        }
        return sumsq;
    }

    /** */
    public static double maximumAbsoluteRowSum(Matrix mtx) {
        return IntStream.range(0, mtx.rowSize()).mapToObj(mtx::viewRow).map(v -> Math.abs(v.sum())).reduce(Math::max).get();
    }
}
