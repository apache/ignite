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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.IgniteException;
import org.apache.ignite.math.ExternalizeTest;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.junit.Assert.*;

/** See also: {@link AbstractVectorTest} and {@link VectorToMatrixTest}. */
public class VectorImplementationsTest {
    /** */ @Test
    public void vectorImplementationsFixturesTest() {
        new VectorImplementationsFixtures().selfTest();
    }

    /** */ @Test
    public void setGetTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            vec.set(idx, val);

            return val;
        }));
    }

    /** */ @Test
    public void setXTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            vec.setX(idx, val);

            return val;
        }));
    }

    /** */ @Test
    public void incrementTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            double old = vec.get(idx);

            vec.increment(idx, val);

            return old + val;
        }));
    }

    /** */ @Test
    public void incrementXTest() {
        consumeSampleVectors((v, desc) -> mutateAtIdxTest(v, desc, (vec, idx, val) -> {
            double old = vec.getX(idx);

            vec.incrementX(idx, val);

            return old + val;
        }));
    }

    /** */ @Test
    public void operateXOutOfBoundsTest() {
        consumeSampleVectors((v, desc) -> {
            if (v instanceof DenseLocalOffHeapVector || v instanceof SparseLocalVector)
                return; // todo find out if it's OK to skip by instances here

            boolean expECaught = false;

            try {
                v.getX(-1);
            } catch (ArrayIndexOutOfBoundsException|IgniteException e) {
                expECaught = true;
            }

            if (!getXOutOfBoundsOK(v))
                assertTrue("Expect exception at negative index getX in " + desc, expECaught);

            expECaught = false;

            try {
                v.setX(-1, 0);
            } catch (ArrayIndexOutOfBoundsException|IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at negative index setX in " + desc, expECaught);

            expECaught = false;

            try {
                v.incrementX(-1, 1);
            } catch (ArrayIndexOutOfBoundsException|IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at negative index incrementX in " + desc, expECaught);

            expECaught = false;

            try {
                v.getX(v.size());
            } catch (ArrayIndexOutOfBoundsException|IgniteException e) {
                expECaught = true;
            }

            if (!getXOutOfBoundsOK(v))
                assertTrue("Expect exception at too large index getX in " + desc, expECaught);

            expECaught = false;

            try {
                v.setX(v.size(), 1);
            } catch (ArrayIndexOutOfBoundsException|IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at too large index setX in " + desc, expECaught);

            expECaught = false;

            try {
                v.incrementX(v.size(), 1);
            } catch (ArrayIndexOutOfBoundsException|IgniteException e) {
                expECaught = true;
            }

            assertTrue("Expect exception at too large index incrementX in " + desc, expECaught);
        });
    }

    /** */ @Test
    public void sizeTest() {
        final AtomicReference<Integer> expSize = new AtomicReference<>(0);

        consumeSampleVectors(
            expSize::set,
            (v, desc) -> assertEquals("Expected size for " + desc,
                (int) expSize.get(), v.size())
        );
    }

    /** */ @Test
    public void getElementTest() {
        consumeSampleVectors((v, desc) -> new ElementsChecker(v, desc).assertCloseEnough(v));
    }

    /** */ @Test
    public void copyTest() {
        consumeSampleVectors((v, desc) -> new ElementsChecker(v, desc).assertCloseEnough(v.copy()));
    }

    /** */ @Test
    public void divideTest() {
        operationTest((val, operand) -> val / operand, Vector::divide);
    }

    /** */ @Test
    public void likeTest() {
        for (int card : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            consumeSampleVectors((v, desc) -> {
                Class<? extends Vector> expType = expLikeType(v);

                if (expType == null)
                    return;

                Vector vLike = v.like(card);

                assertNotNull("Expect non-null like vector for " + expType.getSimpleName() + " in " + desc, vLike);
                assertEquals("Expect size equal to cardinality at " + desc, card, vLike.size());

                Class<? extends Vector> actualType = vLike.getClass();

                assertTrue("Actual vector type " + actualType.getSimpleName()
                        + " should be assignable from expected type " + expType.getSimpleName() + " in " + desc,
                    actualType.isAssignableFrom(expType));
        });
    }

    /** */ @Test
    public void minusTest() {
        operationVectorTest((operand1, operand2) -> operand1 - operand2, Vector::minus);
    }

    /** */ @Test
    public void normalizeTest() {
        normalizeTest(2, (val, len) -> val / len, Vector::normalize);
    }

    /** */ @Test
    public void normalizePowerTest() {
        for (double pow : new double[] {0, 0.5, 1, 2, 2.5, Double.POSITIVE_INFINITY})
            normalizeTest(pow, (val, norm) -> val / norm, (v) -> v.normalize(pow));
    }

    /** */ @Test
    public void logNormalizeTest() {
        normalizeTest(2, (val, len) -> Math.log1p(val) / (len * Math.log(2)), Vector::logNormalize);
    }

    /** */ @Test
    public void logNormalizePowerTest() {
        for (double pow : new double[] {1.1, 2, 2.5})
            normalizeTest(pow, (val, norm) -> Math.log1p(val) / (norm * Math.log(pow)), (v) -> v.logNormalize(pow));
    }

    /** */ @Test
    public void kNormTest() {
        for (double pow : new double[] {0, 0.5, 1, 2, 2.5, Double.POSITIVE_INFINITY})
            toDoubleTest(pow, ref -> new Norm(ref, pow).calculate(), v -> v.kNorm(pow));
    }

    /** */ @Test
    public void plusVectorTest() {
        operationVectorTest((operand1, operand2) -> operand1 + operand2, Vector::plus);
    }

    /** */ @Test
    public void plusDoubleTest() {
        operationTest((val, operand) -> val + operand, Vector::plus);
    }

    /** */ @Test
    public void timesVectorTest() {
        operationVectorTest((operand1, operand2) -> operand1 * operand2, Vector::times);
    }

    /** */ @Test
    public void timesDoubleTest() {
        operationTest((val, operand) -> val * operand, Vector::times);
    }

    /** */ @Test
    public void viewPartTest() {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();
            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);

            for (int off = 0; off < size; off++)
                for (int len = 1; len < size - off; len++)
                    checker.assertCloseEnough(v.viewPart(off, len), Arrays.copyOfRange(ref, off, off + len));
        });
    }

    /** */ @Test
    public void sumTest() {
        toDoubleTest(null,
            ref -> Arrays.stream(ref).sum(),
            Vector::sum);
    }

    /** */ @Test
    public void minValueTest() {
        toDoubleTest(null,
            ref -> Arrays.stream(ref).min().getAsDouble(),
            v -> v.minValue().get());
    }

    /** */ @Test
    public void maxValueTest() {
        toDoubleTest(null,
            ref -> Arrays.stream(ref).max().getAsDouble(),
            v -> v.maxValue().get());
    }

    /** */
    @Test
    public void getLengthSquaredTest() {
        toDoubleTest(2.0, ref -> new Norm(ref, 2).sumPowers(), Vector::getLengthSquared);
    }

    /** */
    @Test
    public void getDistanceSquared() {
        consumeSampleVectors((v, desc) -> {
            new ElementsChecker(v, desc); // IMPL NOTE this initialises vector

            final int size = v.size();
            final Vector vOnHeap = new DenseLocalOnHeapVector(size);
            final Vector vOffHeap = new DenseLocalOffHeapVector(size);

            for (Vector.Element e : v.all()) {
                final int idx = size - 1 - e.index();
                final double val = e.get();

                vOnHeap.set(idx, val);
                vOffHeap.set(idx, val);
            }

            for (int idx = 0; idx < size; idx++) {
                final double exp = v.get(idx);
                final int idxMirror = size - 1 - idx;

                assertTrue("On heap vector difference at " + desc + ", idx " + idx,
                    exp - vOnHeap.get(idxMirror) == 0);
                assertTrue("Off heap vector difference at " + desc + ", idx " + idx,
                    exp - vOffHeap.get(idxMirror) == 0);
            }

            final double exp = vOnHeap.minus(v).getLengthSquared(); // IMPL NOTE this won't mutate vOnHeap
            final Metric metric = new Metric(exp, v.getDistanceSquared(vOnHeap));

            assertTrue("On heap vector not close enough at " + desc + ", " + metric,
                metric.closeEnough());

            final Metric metric1 = new Metric(exp, v.getDistanceSquared(vOffHeap));

            assertTrue("Off heap vector not close enough at " + desc + ", " + metric1,
                metric1.closeEnough());
        });
    }

    /** */ @Test
    public void sortTest() {
        consumeSampleVectors((v, desc) -> {
            if(readOnly(v) || !v.isArrayBased()) {
                boolean expECaught = false;

                try {
                    v.sort();
                } catch (UnsupportedOperationException uoe) {
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
    private boolean getXOutOfBoundsOK(Vector v) {
        // todo find out if this is indeed OK
        return v instanceof RandomVector || v instanceof  ConstantVector
            || v instanceof SingleElementVector || v instanceof SingleElementVectorView;
    }

    /** */
    private void mutateAtIdxTest(Vector v, String desc, MutateAtIdx operation) {
        if (readOnly(v))
            return;

        for (double val : new double[] {0, -1, 0, 1})
            for (int idx = 0; idx < v.size(); idx++) {
                double exp = operation.apply(v, idx, val);

                final Metric metric = new Metric(exp, v.get(idx));

                assertTrue("Not close enough at index " + idx + ", val " + val + ", " + metric
                    + ", " + desc, metric.closeEnough());
            }
    }

    /** */
    private Class<?extends Vector> expLikeType(Vector v) {
        Class<?extends Vector> clazz = v.getClass();

        if(clazz.isAssignableFrom(PivotedVectorView.class) || clazz.isAssignableFrom(SingleElementVectorView.class))
            return null;

        if (clazz.isAssignableFrom(MatrixVectorView.class) || clazz.isAssignableFrom(DelegatingVector.class))
            return DenseLocalOnHeapVector.class; // IMPL NOTE per fixture

        return clazz;
    }

    /** */
    private void toDoubleTest(Double val, Function<double[], Double> calcRef, Function<Vector, Double> calcVec) {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();
            final double[] ref = new double[size];

            new ElementsChecker(v, ref, desc); // IMPL NOTE this initialises vector and reference array

            final double exp = calcRef.apply(ref);
            final double obtained = calcVec.apply(v);
            final Metric metric = new Metric(exp, obtained);

            assertTrue("Not close enough at " + desc
                + (val == null ? "" : ", value " + val) + ", " + metric, metric.closeEnough());
        });
    }

    /** */
    private void normalizeTest(double pow, BiFunction<Double, Double, Double> operation,
        Function<Vector, Vector> vecOperation) {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();
            final double[] ref = new double[size];
            final boolean nonNegative = pow != (int)pow;

            final ElementsChecker checker = new ElementsChecker(v, ref, desc + ", pow = " + pow, nonNegative);
            final double norm = new Norm(ref, pow).calculate();

            for (int idx = 0; idx < size; idx++)
                ref[idx] = operation.apply(ref[idx], norm);

            checker.assertCloseEnough(vecOperation.apply(v), ref);
        });
    }

    /** */
    private void operationVectorTest(BiFunction<Double, Double, Double> operation,
        BiFunction<Vector, Vector, Vector> vecOperation) {
        consumeSampleVectors((v, desc) -> {
            // TODO find out if more elaborate testing scenario is needed or it's okay as is.
            final int size = v.size();
            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);
            final Vector operand = v.copy();

            for (int idx = 0; idx < size; idx++)
                ref[idx] = operation.apply(ref[idx], ref[idx]);

            checker.assertCloseEnough(vecOperation.apply(v, operand), ref);
        });
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
    private static boolean readOnly(Vector v) {
        return v instanceof RandomVector || v instanceof ConstantVector;
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
    private interface MutateAtIdx {
        /** */
        double apply(Vector v, int idx, double val);
    }

    /** */
    private static class Norm {
        /** */
        private final double[] arr;

        /** */
        private final Double pow;

        /** */
        Norm(double[] arr, double pow) {
            this.arr = arr;
            this.pow = pow;
        }

        /** */
        double calculate() {
            if (pow.equals(0.0))
                return countNonZeroes(); // IMPL NOTE this is beautiful if you think of it

            if (pow.equals(Double.POSITIVE_INFINITY))
                return maxAbs();

            return Math.pow(sumPowers(), 1 / pow);
        }

        /** */
        double sumPowers() {
            if (pow.equals(0.0))
                return countNonZeroes();

            double norm = 0;

            for (double val : arr)
                norm += pow == 1 ? Math.abs(val) : Math.pow(val, pow);

            return norm;
        }

        /** */
        private int countNonZeroes() {
            int cnt = 0;

            final Double zero = 0.0;

            for (double val : arr)
                if (!zero.equals(val))
                    cnt++;

            return cnt;
        }

        /** */
        private double maxAbs() {
            double res = 0;

            for (double val : arr) {
                final double abs = Math.abs(val);

                if (abs > res)
                    res = abs;
            }

            return res;
        }
    }

    /** */
    private static class ElementsChecker {
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

            refReadOnly = readOnly(v) && ref == null ? new double[v.size()] : null;

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
        private void init(Vector v, double[] ref) {
            if (readOnly(v)) {
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
    private static class Metric { // todo consider if softer tolerance (like say 0.1 or 0.01) would make sense here
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
