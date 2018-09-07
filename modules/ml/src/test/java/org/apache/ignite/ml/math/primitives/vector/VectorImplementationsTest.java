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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.ml.math.ExternalizeTest;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.primitives.vector.impl.DelegatingVector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** See also: {@link AbstractVectorTest} and {@link VectorToMatrixTest}. */
public class VectorImplementationsTest { // TODO: IGNTIE-5723, split this to smaller cohesive test classes
    /** */
    @Test
    public void setGetTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            vec.set(idx, val);

            return val;
        }));
    }

    /** */
    @Test
    public void setXTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            vec.setX(idx, val);

            return val;
        }));
    }

    /** */
    @Test
    public void incrementTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            double old = vec.get(idx);

            vec.increment(idx, val);

            return old + val;
        }));
    }

    /** */
    @Test
    public void incrementXTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            double old = vec.getX(idx);

            vec.incrementX(idx, val);

            return old + val;
        }));
    }

    /** */
    @Test
    public void operateXOutOfBoundsTest() {
        consumeSampleVectors((v, desc) -> {
            if (v instanceof SparseVector)
                return; // TODO: IGNTIE-5723, find out if it's OK to skip by instances here

            boolean expECaught = false;

            try {
                v.getX(-1);
            }
            catch (ArrayIndexOutOfBoundsException | IgniteException e) {
                expECaught = true;
            }

            if (!getXOutOfBoundsOK(v))
                assertTrue("Expect exception at negative index getX in " + desc, expECaught);

            expECaught = false;

            try {
                v.setX(-1, 0);
            }
            catch (ArrayIndexOutOfBoundsException | IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at negative index setX in " + desc, expECaught);

            expECaught = false;

            try {
                v.incrementX(-1, 1);
            }
            catch (ArrayIndexOutOfBoundsException | IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at negative index incrementX in " + desc, expECaught);

            expECaught = false;

            try {
                v.getX(v.size());
            }
            catch (ArrayIndexOutOfBoundsException | IgniteException e) {
                expECaught = true;
            }

            if (!getXOutOfBoundsOK(v))
                assertTrue("Expect exception at too large index getX in " + desc, expECaught);

            expECaught = false;

            try {
                v.setX(v.size(), 1);
            }
            catch (ArrayIndexOutOfBoundsException | IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at too large index setX in " + desc, expECaught);

            expECaught = false;

            try {
                v.incrementX(v.size(), 1);
            }
            catch (ArrayIndexOutOfBoundsException | IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at too large index incrementX in " + desc, expECaught);
        });
    }

    /** */
    @Test
    public void sizeTest() {
        final AtomicReference<Integer> expSize = new AtomicReference<>(0);

        consumeSampleVectors(
            expSize::set,
            (v, desc) -> Assert.assertEquals("Expected size for " + desc,
                (int)expSize.get(), v.size())
        );
    }

    /** */
    @Test
    public void getElementTest() {
        consumeSampleVectors((v, desc) -> new ElementsChecker(v, desc).assertCloseEnough(v));
    }

    /** */
    @Test
    public void copyTest() {
        consumeSampleVectors((v, desc) -> new ElementsChecker(v, desc).assertCloseEnough(v.copy()));
    }

    /** */
    @Test
    public void divideTest() {
        operationTest((val, operand) -> val / operand, Vector::divide);
    }

    /** */
    @Test
    public void likeTest() {
        for (int card : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            consumeSampleVectors((v, desc) -> {
                Class<? extends Vector> expType = expLikeType(v);

                if (expType == null) {
                    try {
                        v.like(card);
                    }
                    catch (UnsupportedOperationException uoe) {
                        return;
                    }

                    fail("Expected exception wasn't caught for " + desc);

                    return;
                }

                Vector vLike = v.like(card);

                assertNotNull("Expect non-null like vector for " + expType.getSimpleName() + " in " + desc, vLike);
                assertEquals("Expect size equal to cardinality at " + desc, card, vLike.size());

                Class<? extends Vector> actualType = vLike.getClass();

                assertTrue("Actual vector type " + actualType.getSimpleName()
                        + " should be assignable from expected type " + expType.getSimpleName() + " in " + desc,
                    actualType.isAssignableFrom(expType));
            });
    }

    /** */
    @Test
    public void minusTest() {
        operationVectorTest((operand1, operand2) -> operand1 - operand2, Vector::minus);
    }

    /** */
    @Test
    public void plusVectorTest() {
        operationVectorTest((operand1, operand2) -> operand1 + operand2, Vector::plus);
    }

    /** */
    @Test
    public void plusDoubleTest() {
        operationTest((val, operand) -> val + operand, Vector::plus);
    }

    /** */
    @Test
    public void timesVectorTest() {
        operationVectorTest((operand1, operand2) -> operand1 * operand2, Vector::times);
    }

    /** */
    @Test
    public void timesDoubleTest() {
        operationTest((val, operand) -> val * operand, Vector::times);
    }

    /** */
    @Test
    public void viewPartTest() {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();
            final double[] ref = new double[size];
            final int delta = size > 32 ? 3 : 1; // IMPL NOTE this is for faster test execution

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);

            for (int off = 0; off < size; off += delta)
                for (int len = 1; len < size - off; len += delta)
                    checker.assertCloseEnough(v.viewPart(off, len), Arrays.copyOfRange(ref, off, off + len));
        });
    }

    /** */
    @Test
    public void sumTest() {
        toDoubleTest(
            ref -> Arrays.stream(ref).sum(),
            Vector::sum);
    }

    /** */
    @Test
    public void minValueTest() {
        toDoubleTest(
            ref -> Arrays.stream(ref).min().getAsDouble(),
            Vector::minValue);
    }

    /** */
    @Test
    public void maxValueTest() {
        toDoubleTest(
            ref -> Arrays.stream(ref).max().getAsDouble(),
            Vector::maxValue);
    }

    /** */
    @Test
    public void sortTest() {
        consumeSampleVectors((v, desc) -> {
            if (readOnly() || !v.isArrayBased()) {
                boolean expECaught = false;

                try {
                    v.sort();
                }
                catch (UnsupportedOperationException uoe) {
                    expECaught = true;
                }

                assertTrue("Expected exception was not caught for sort in " + desc, expECaught);

                return;
            }

            final int size = v.size();
            final double[] ref = new double[size];

            new ElementsChecker(v, ref, desc).assertCloseEnough(v.sort(), Arrays.stream(ref).sorted().toArray());
        });
    }

    /** */
    @Test
    public void metaAttributesTest() {
        consumeSampleVectors((v, desc) -> {
            assertNotNull("Null meta storage in " + desc, v.getMetaStorage());

            final String key = "test key";
            final String val = "test value";
            final String details = "key [" + key + "] for " + desc;

            v.setAttribute(key, val);
            assertTrue("Expect to have meta attribute for " + details, v.hasAttribute(key));
            assertEquals("Unexpected meta attribute value for " + details, val, v.getAttribute(key));

            v.removeAttribute(key);
            assertFalse("Expect not to have meta attribute for " + details, v.hasAttribute(key));
            assertNull("Unexpected meta attribute value for " + details, v.getAttribute(key));
        });
    }

    /** */
    @Test
    public void assignDoubleTest() {
        consumeSampleVectors((v, desc) -> {
            if (readOnly())
                return;

            for (double val : new double[] {0, -1, 0, 1}) {
                v.assign(val);

                for (int idx = 0; idx < v.size(); idx++) {
                    final Metric metric = new Metric(val, v.get(idx));

                    assertTrue("Not close enough at index " + idx + ", val " + val + ", " + metric
                        + ", " + desc, metric.closeEnough());
                }
            }
        });
    }

    /** */
    @Test
    public void assignDoubleArrTest() {
        consumeSampleVectors((v, desc) -> {
            if (readOnly())
                return;

            final int size = v.size();
            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);

            for (int idx = 0; idx < size; idx++)
                ref[idx] = -ref[idx];

            v.assign(ref);

            checker.assertCloseEnough(v, ref);

            assignDoubleArrWrongCardinality(v, desc);
        });
    }

    /** */
    @Test
    public void assignVectorTest() {
        consumeSampleVectors((v, desc) -> {
            if (readOnly())
                return;

            final int size = v.size();
            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);

            for (int idx = 0; idx < size; idx++)
                ref[idx] = -ref[idx];

            v.assign(new DenseVector(ref));

            checker.assertCloseEnough(v, ref);

            assignVectorWrongCardinality(v, desc);
        });
    }

    /** */
    @Test
    public void assignFunctionTest() {
        consumeSampleVectors((v, desc) -> {
            if (readOnly())
                return;

            final int size = v.size();
            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);

            for (int idx = 0; idx < size; idx++)
                ref[idx] = -ref[idx];

            v.assign((idx) -> ref[idx]);

            checker.assertCloseEnough(v, ref);
        });
    }

    /** */
    @Test
    public void minElementTest() {
        consumeSampleVectors((v, desc) -> {
            final ElementsChecker checker = new ElementsChecker(v, desc);

            final Vector.Element minE = v.minElement();

            final int minEIdx = minE.index();

            assertTrue("Unexpected index from minElement " + minEIdx + ", " + desc,
                minEIdx >= 0 && minEIdx < v.size());

            final Metric metric = new Metric(minE.get(), v.minValue());

            assertTrue("Not close enough minElement at index " + minEIdx + ", " + metric
                + ", " + desc, metric.closeEnough());

            checker.assertNewMinElement(v);
        });
    }

    /** */
    @Test
    public void maxElementTest() {
        consumeSampleVectors((v, desc) -> {
            final ElementsChecker checker = new ElementsChecker(v, desc);

            final Vector.Element maxE = v.maxElement();

            final int minEIdx = maxE.index();

            assertTrue("Unexpected index from minElement " + minEIdx + ", " + desc,
                minEIdx >= 0 && minEIdx < v.size());

            final Metric metric = new Metric(maxE.get(), v.maxValue());

            assertTrue("Not close enough maxElement at index " + minEIdx + ", " + metric
                + ", " + desc, metric.closeEnough());

            checker.assertNewMaxElement(v);
        });
    }

    /** */
    @Test
    public void externalizeTest() {
        (new ExternalizeTest<Vector>() {
            /** {@inheritDoc} */
            @Override public void externalizeTest() {
                consumeSampleVectors((v, desc) -> externalizeTest(v));
            }
        }).externalizeTest();
    }

    /** */
    @Test
    public void hashCodeTest() {
        consumeSampleVectors((v, desc) -> assertTrue("Zero hash code for " + desc, v.hashCode() != 0));
    }

    /** */
    private boolean getXOutOfBoundsOK(Vector v) {
        // TODO: IGNTIE-5723, find out if this is indeed OK
        return false;
    }

    /** */
    private void mutateAtIdxTest(Vector v, String desc, MutateAtIdx operation) {
        if (readOnly()) {
            if (v.size() < 1)
                return;

            boolean expECaught = false;

            try {
                operation.apply(v, 0, 1);
            }
            catch (UnsupportedOperationException uoe) {
                expECaught = true;
            }

            assertTrue("Expect exception at attempt to mutate element in " + desc, expECaught);

            return;
        }

        for (double val : new double[] {0, -1, 0, 1})
            for (int idx = 0; idx < v.size(); idx++) {
                double exp = operation.apply(v, idx, val);

                final Metric metric = new Metric(exp, v.get(idx));

                assertTrue("Not close enough at index " + idx + ", val " + val + ", " + metric
                    + ", " + desc, metric.closeEnough());
            }
    }

    /** */
    private Class<? extends Vector> expLikeType(Vector v) {
        Class<? extends Vector> clazz = v.getClass();

        if (clazz.isAssignableFrom(VectorizedViewMatrix.class) || clazz.isAssignableFrom(DelegatingVector.class))
            return DenseVector.class; // IMPL NOTE per fixture

        return clazz;
    }

    /** */
    private void toDoubleTest(Function<double[], Double> calcRef, Function<Vector, Double> calcVec) {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();
            final double[] ref = new double[size];

            new ElementsChecker(v, ref, desc); // IMPL NOTE this initialises vector and reference array

            final Metric metric = new Metric(calcRef.apply(ref), calcVec.apply(v));

            assertTrue("Not close enough at " + desc
                + ", " + metric, metric.closeEnough());
        });
    }

    /** */
    private void operationVectorTest(BiFunction<Double, Double, Double> operation,
        BiFunction<Vector, Vector, Vector> vecOperation) {
        consumeSampleVectors((v, desc) -> {
            // TODO : IGNTIE-5723, find out if more elaborate testing scenario is needed or it's okay as is.
            final int size = v.size();
            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);
            final Vector operand = v.copy();

            for (int idx = 0; idx < size; idx++)
                ref[idx] = operation.apply(ref[idx], ref[idx]);

            checker.assertCloseEnough(vecOperation.apply(v, operand), ref);

            assertWrongCardinality(v, desc, vecOperation);
        });
    }

    /** */
    private void assignDoubleArrWrongCardinality(Vector v, String desc) {
        boolean expECaught = false;

        try {
            v.assign(new double[v.size() + 1]);
        }
        catch (CardinalityException ce) {
            expECaught = true;
        }

        assertTrue("Expect exception at too large size in " + desc, expECaught);

        if (v.size() < 2)
            return;

        expECaught = false;

        try {
            v.assign(new double[v.size() - 1]);
        }
        catch (CardinalityException ce) {
            expECaught = true;
        }

        assertTrue("Expect exception at too small size in " + desc, expECaught);
    }

    /** */
    private void assignVectorWrongCardinality(Vector v, String desc) {
        boolean expECaught = false;

        try {
            v.assign(new DenseVector(v.size() + 1));
        }
        catch (CardinalityException ce) {
            expECaught = true;
        }

        assertTrue("Expect exception at too large size in " + desc, expECaught);

        if (v.size() < 2)
            return;

        expECaught = false;

        try {
            v.assign(new DenseVector(v.size() - 1));
        }
        catch (CardinalityException ce) {
            expECaught = true;
        }

        assertTrue("Expect exception at too small size in " + desc, expECaught);
    }

    /** */
    private void assertWrongCardinality(
        Vector v, String desc, BiFunction<Vector, Vector, Vector> vecOperation) {
        boolean expECaught = false;

        try {
            vecOperation.apply(v, new DenseVector(v.size() + 1));
        }
        catch (CardinalityException ce) {
            expECaught = true;
        }

        assertTrue("Expect exception at too large size in " + desc, expECaught);

        if (v.size() < 2)
            return;

        expECaught = false;

        try {
            vecOperation.apply(v, new DenseVector(v.size() - 1));
        }
        catch (CardinalityException ce) {
            expECaught = true;
        }

        assertTrue("Expect exception at too small size in " + desc, expECaught);
    }

    /** */
    private void operationTest(BiFunction<Double, Double, Double> operation,
        BiFunction<Vector, Double, Vector> vecOperation) {
        for (double val : new double[] {0, 0.1, 1, 2, 10})
            consumeSampleVectors((v, desc) -> {
                final int size = v.size();
                final double[] ref = new double[size];

                final ElementsChecker checker = new ElementsChecker(v, ref, "val " + val + ", " + desc);

                for (int idx = 0; idx < size; idx++)
                    ref[idx] = operation.apply(ref[idx], val);

                checker.assertCloseEnough(vecOperation.apply(v, val), ref);
            });
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        consumeSampleVectors(null, consumer);
    }

    /** */
    private void consumeSampleVectors(Consumer<Integer> paramsConsumer, BiConsumer<Vector, String> consumer) {
        new VectorImplementationsFixtures().consumeSampleVectors(paramsConsumer, consumer);
    }

    /** */
    private static boolean readOnly() {
        return false;
    }

    /** */
    private interface MutateAtIdx {
        /** */
        double apply(Vector v, int idx, double val);
    }

    /** */
    static class ElementsChecker {
        /** */
        private final String fixtureDesc;

        /** */
        private final double[] refReadOnly;

        /** */
        private final boolean nonNegative;

        /** */
        ElementsChecker(Vector v, double[] ref, String fixtureDesc, boolean nonNegative) {
            this.fixtureDesc = fixtureDesc;

            this.nonNegative = nonNegative;

            refReadOnly = readOnly() && ref == null ? new double[v.size()] : null;

            init(v, ref);
        }

        /** */
        ElementsChecker(Vector v, double[] ref, String fixtureDesc) {
            this(v, ref, fixtureDesc, false);
        }

        /** */
        ElementsChecker(Vector v, String fixtureDesc) {
            this(v, null, fixtureDesc);
        }

        /** */
        void assertCloseEnough(Vector obtained, double[] exp) {
            final int size = obtained.size();

            for (int i = 0; i < size; i++) {
                final Vector.Element e = obtained.getElement(i);

                if (refReadOnly != null && exp == null)
                    exp = refReadOnly;

                final Metric metric = new Metric(exp == null ? generated(i) : exp[i], e.get());

                assertEquals("Unexpected vector index at " + fixtureDesc, i, e.index());
                assertTrue("Not close enough at index " + i + ", size " + size + ", " + metric
                    + ", " + fixtureDesc, metric.closeEnough());
            }
        }

        /** */
        void assertCloseEnough(Vector obtained) {
            assertCloseEnough(obtained, null);
        }

        /** */
        void assertNewMinElement(Vector v) {
            if (readOnly())
                return;

            int exp = v.size() / 2;

            v.set(exp, -(v.size() * 2 + 1));

            assertEquals("Unexpected minElement index at " + fixtureDesc, exp, v.minElement().index());
        }

        /** */
        void assertNewMaxElement(Vector v) {
            if (readOnly())
                return;

            int exp = v.size() / 2;

            v.set(exp, v.size() * 2 + 1);

            assertEquals("Unexpected minElement index at " + fixtureDesc, exp, v.maxElement().index());
        }

        /** */
        private void init(Vector v, double[] ref) {
            if (readOnly()) {
                initReadonly(v, ref);

                return;
            }

            for (Vector.Element e : v.all()) {
                int idx = e.index();

                // IMPL NOTE introduce negative values because their absence
                //    blocked catching an ugly bug in AbstractVector#kNorm
                int val = generated(idx);

                e.set(val);

                if (ref != null)
                    ref[idx] = val;
            }
        }

        /** */
        private void initReadonly(Vector v, double[] ref) {
            if (refReadOnly != null)
                for (Vector.Element e : v.all())
                    refReadOnly[e.index()] = e.get();

            if (ref != null)
                for (Vector.Element e : v.all())
                    ref[e.index()] = e.get();
        }

        /** */
        private int generated(int idx) {
            return nonNegative || (idx & 1) == 0 ? idx : -idx;
        }
    }

    /** */
    static class Metric { //TODO: IGNITE-5824, consider if softer tolerance (like say 0.1 or 0.01) would make sense here
        /** */
        private final double exp;

        /** */
        private final double obtained;

        /** **/
        Metric(double exp, double obtained) {
            this.exp = exp;
            this.obtained = obtained;
        }

        /** */
        boolean closeEnough() {
            return new Double(exp).equals(obtained) || closeEnoughToZero();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Metric{" + "expected=" + exp +
                ", obtained=" + obtained +
                '}';
        }

        /** */
        private boolean closeEnoughToZero() {
            return (new Double(exp).equals(0.0) && new Double(obtained).equals(-0.0))
                || (new Double(exp).equals(-0.0) && new Double(obtained).equals(0.0));
        }
    }
}
