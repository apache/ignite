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

import java.io.Serializable;
import java.util.stream.IntStream;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.junit.Assert;

import static org.junit.Assert.assertTrue;

/** */
public class TestUtils {
    /**
     * Collection of static methods used in math unit tests.
     */
    private TestUtils() {
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

        if (maximumAbsoluteRowSum(delta) >= tolerance) {
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
     * Verifies that two vectors are equal.
     *
     * @param exp Expected vector.
     * @param observed Actual vector.
     */
    public static void assertEquals(Vector exp, Vector observed, double eps) {
        Assert.assertNotNull("Observed should not be null", observed);

        if (exp.size() != observed.size()) {
            String msgBuff = "Observed has incorrect dimensions." +
                "\nobserved is " + observed.size() +
                " x " + observed.size();

            Assert.fail(msgBuff);
        }

        for (int i = 0; i < exp.size(); ++i) {
            double eij = exp.getX(i);
            double aij = observed.getX(i);

            Assert.assertEquals(eij, aij, eps);
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
        }
        catch (Throwable e) {
            return false;
        }

        return true;
    }

    /** */
    private static class Precision {
        /** Offset to order signed double numbers lexicographically. */
        private static final long SGN_MASK = 0x8000000000000000L;

        /** Positive zero bits. */
        private static final long POSITIVE_ZERO_DOUBLE_BITS = Double.doubleToRawLongBits(+0.0);

        /** Negative zero bits. */
        private static final long NEGATIVE_ZERO_DOUBLE_BITS = Double.doubleToRawLongBits(-0.0);

        /**
         * Returns true if the arguments are both NaN, are equal or are within the range
         * of allowed error (inclusive).
         *
         * @param x first value
         * @param y second value
         * @param eps the amount of absolute error to allow.
         * @return {@code true} if the values are equal or within range of each other, or both are NaN.
         * @since 2.2
         */
        static boolean equalsIncludingNaN(double x, double y, double eps) {
            return equalsIncludingNaN(x, y) || (Math.abs(y - x) <= eps);
        }

        /**
         * Returns true if the arguments are both NaN or they are
         * equal as defined by {@link #equals(double, double, int) equals(x, y, 1)}.
         *
         * @param x first value
         * @param y second value
         * @return {@code true} if the values are equal or both are NaN.
         * @since 2.2
         */
        private static boolean equalsIncludingNaN(double x, double y) {
            return (x != x || y != y) ? !(x != x ^ y != y) : equals(x, y, 1);
        }

        /**
         * Returns true if the arguments are equal or within the range of allowed
         * error (inclusive).
         * <p>
         * Two float numbers are considered equal if there are {@code (maxUlps - 1)}
         * (or fewer) floating point numbers between them, i.e. two adjacent
         * floating point numbers are considered equal.
         * </p>
         * <p>
         * Adapted from <a
         * href="http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/">
         * Bruce Dawson</a>. Returns {@code false} if either of the arguments is NaN.
         * </p>
         *
         * @param x first value
         * @param y second value
         * @param maxUlps {@code (maxUlps - 1)} is the number of floating point values between {@code x} and {@code y}.
         * @return {@code true} if there are fewer than {@code maxUlps} floating point values between {@code x} and {@code
         * y}.
         */
        private static boolean equals(final double x, final double y, final int maxUlps) {

            final long xInt = Double.doubleToRawLongBits(x);
            final long yInt = Double.doubleToRawLongBits(y);

            final boolean isEqual;
            if (((xInt ^ yInt) & SGN_MASK) == 0L) {
                // number have same sign, there is no risk of overflow
                isEqual = Math.abs(xInt - yInt) <= maxUlps;
            }
            else {
                // number have opposite signs, take care of overflow
                final long deltaPlus;
                final long deltaMinus;
                if (xInt < yInt) {
                    deltaPlus = yInt - POSITIVE_ZERO_DOUBLE_BITS;
                    deltaMinus = xInt - NEGATIVE_ZERO_DOUBLE_BITS;
                }
                else {
                    deltaPlus = xInt - POSITIVE_ZERO_DOUBLE_BITS;
                    deltaMinus = yInt - NEGATIVE_ZERO_DOUBLE_BITS;
                }

                if (deltaPlus > maxUlps)
                    isEqual = false;
                else
                    isEqual = deltaMinus <= (maxUlps - deltaPlus);

            }

            return isEqual && !Double.isNaN(x) && !Double.isNaN(y);

        }
    }

    /**
     * Gets test learning environment builder.
     *
     * @return Test learning environment builder.
     */
    public static LearningEnvironmentBuilder testEnvBuilder() {
        return testEnvBuilder(123L);
    }

    /**
     * Gets test learning environment builder with a given seed.
     *
     * @param seed Seed.
     * @return Test learning environment builder.
     */
    public static LearningEnvironmentBuilder testEnvBuilder(long seed) {
        return LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(seed);
    }

    /**
     * Simple wrapper class which adds {@link AutoCloseable} to given type.
     *
     * @param <T> Type to wrap.
     */
    public static class DataWrapper<T> implements AutoCloseable {
        /**
         * Value to wrap.
         */
        T val;

        /**
         * Wrap given value in {@link AutoCloseable}.
         *
         * @param val Value to wrap.
         * @param <T> Type of value to wrap.
         * @return Value wrapped as {@link AutoCloseable}.
         */
        public static <T> DataWrapper<T> of(T val) {
            return new DataWrapper<>(val);
        }

        /**
         * Construct instance of this class from given value.
         *
         * @param val Value to wrap.
         */
        public DataWrapper(T val) {
            this.val = val;
        }

        /**
         * Get wrapped value.
         *
         * @return Wrapped value.
         */
        public T val() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            if (val instanceof AutoCloseable)
                ((AutoCloseable)val).close();
        }
    }

    /**
     * Return model which returns given constant.
     *
     * @param v Constant value.
     * @param <T> Type of input.
     * @param <V> Type of output.
     * @return Model which returns given constant.
     */
    public static <T, V> IgniteModel<T, V> constantModel(V v) {
        return t -> v;
    }

    /**
     * Returns trainer which independently of dataset outputs given model.
     *
     * @param ml Model.
     * @param <I> Type of model input.
     * @param <O> Type of model output.
     * @param <M> Type of model.
     * @param <L> Type of dataset labels.
     * @return Trainer which independently of dataset outputs given model.
     */
    public static <I, O, M extends IgniteModel<I, O>, L> DatasetTrainer<M, L> constantTrainer(M ml) {
        return new DatasetTrainer<M, L>() {
            /** */
            public <K, V, C extends Serializable> M fit(DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L> extractor) {
                return ml;
            }

            @Override public <K, V> M fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor) {
                return null;
            }

            /** {@inheritDoc} */
            @Override public boolean isUpdateable(M mdl) {
                return true;
            }

            @Override protected <K, V> M updateModel(M mdl, DatasetBuilder<K, V> datasetBuilder,
                Preprocessor<K, V> preprocessor) {
                return null;
            }

            /** */
            public <K, V, C extends Serializable> M updateModel(M mdl, DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L> extractor) {
                return ml;
            }
        };
    }
}
