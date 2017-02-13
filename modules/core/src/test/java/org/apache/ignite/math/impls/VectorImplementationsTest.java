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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.Vector;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.junit.Assert.*;

/** See also: {@link AbstractVectorTest}. */
public class VectorImplementationsTest {
    /** */ @Test
    public void vectorImplementationsFixturesTest() {
        new VectorImplementationsFixtures().selfTest();
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
    public void isDenseTest() {
        alwaysTrueAttributeTest(DenseLocalOnHeapVector::isDense);
    }

    /** */ @Test
    public void isSequentialAccessTest() {
        alwaysTrueAttributeTest(DenseLocalOnHeapVector::isSequentialAccess);
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
            consumeSampleVectors((v, desc) -> assertEquals("Expect size equal to cardinality.", card, v.like(card).size()));
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
            toDoubleTest(ref -> new Norm(ref, pow).calculate(), v -> v.kNorm(pow));
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
                for (int len = 0; len < size - off; len++)
                    checker.assertCloseEnough(v.viewPart(off, len), Arrays.copyOfRange(ref, off, off + len));
        });
    }

    /** */ @Test
    public void sumTest() {
        toDoubleTest(
            ref -> {
                double sum = 0;

                for (double val : ref)
                    sum += val;

                return sum;
            },
            Vector::sum);
    }

    /** */ @Test
    public void crossTest() { // TODO write tests for this and other Vector methods involving Matrix

    }

    /** */ @Test
    public void getLookupCostTest() {
        alwaysTrueAttributeTest(v -> v.getLookupCost() == 0);
    }

    /** */ @Test
    public void isAddConstantTimeTest() {
        alwaysTrueAttributeTest(DenseLocalOnHeapVector::isAddConstantTime);
    }

    /** */ @Test
    public void clusterGroupTest() {
        alwaysTrueAttributeTest(v -> v.clusterGroup() == null);
    }

    /** */ @Test
    public void guidTest() {
        alwaysTrueAttributeTest(v -> v.guid() != null);
    }

    /** */
    private void toDoubleTest(Function<double[], Double> calcRef, Function<Vector, Double> calcVec) {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();

            final double[] ref = new double[size];

            new ElementsChecker(v, ref, desc); // IMPL NOTE this initialises vector and reference array

            final double exp = calcRef.apply(ref);

            final double obtained = calcVec.apply(v);

            final Metric metric = new Metric(exp, obtained);

            assertTrue("Not close enough at size " + size + ", " + metric,
                metric.closeEnough());
        });
    }

    /** */
    private void normalizeTest(double pow, BiFunction<Double, Double, Double> operation,
        Function<Vector, Vector> vecOperation) {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();

            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref, desc);

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
    private void alwaysTrueAttributeTest(Predicate<DenseLocalOnHeapVector> pred) {
        assertTrue("Default size for null args.",
            pred.test(new DenseLocalOnHeapVector((Map<String, Object>)null)));

        assertTrue("Size from args.",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{ put("size", 99); }})));

        final double[] test = new double[99];

        assertTrue("Size from array in args.",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("shallowCopy", false);
            }})));

        assertTrue("Size from array in args, shallow copy.",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("shallowCopy", true);
            }})));

        assertTrue("Default constructor.",
            pred.test(new DenseLocalOnHeapVector()));

        assertTrue("Null array shallow copy.",
            pred.test(new DenseLocalOnHeapVector(null, true)));

        assertTrue("0 size shallow copy.",
            pred.test(new DenseLocalOnHeapVector(new double[0], true)));

        assertTrue("0 size.",
            pred.test(new DenseLocalOnHeapVector(new double[0], false)));

        assertTrue("1 size shallow copy.",
            pred.test(new DenseLocalOnHeapVector(new double[1], true)));

        assertTrue("1 size.",
            pred.test(new DenseLocalOnHeapVector(new double[1], false)));

        assertTrue("0 size default copy.",
            pred.test(new DenseLocalOnHeapVector(new double[0])));

        assertTrue("1 size default copy",
            pred.test(new DenseLocalOnHeapVector(new double[1])));
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        consumeSampleVectors(null, consumer);
    }

    /** */
    private void consumeSampleVectors(Consumer<Integer> paramsConsumer, BiConsumer<Vector, String> consumer) {
        for (Supplier<Iterable<Vector>> fixtureSupplier : VectorImplementationsFixtures.suppliers) {
            final Iterable<Vector> fixture = fixtureSupplier.get();

            for (Vector v : fixture) {
                if (paramsConsumer != null)
                    paramsConsumer.accept(v.size());

                consumer.accept(v, fixture.toString());
            }
        }
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

            double norm = 0;

            for (double val : arr)
                norm += Math.pow(val, pow);

            return Math.pow(norm, 1 / pow);
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
        ElementsChecker(Vector v, double[] ref, String fixtureDesc) {
            this.fixtureDesc = fixtureDesc;
            init(v, ref);
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

                final Metric metric = new Metric(exp == null ? i : exp[i], e.get());

                assertEquals("Vector index.", i, e.index());

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
            for (Vector.Element e : v.all()) {
                int idx = e.index();

                e.set(e.index());

                if (ref != null)
                    ref[idx] = idx;
            }
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
            return new Double(exp).equals(obtained);
        }

        /** @{inheritDoc} */
        @Override public String toString() {
            return "Metric{" + "expected=" + exp +
                ", obtained=" + obtained +
                '}';
        }
    }
}
