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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.junit.Assert.*;

/** See also: {@link AbstractVectorTest}. */
public class DenseLocalOnHeapVectorTest {
    /** */ @Test
    public void sizeTest() {
        final AtomicReference<Integer> expSize = new AtomicReference<>(0);

        final AtomicReference<String> desc = new AtomicReference<>("");

        consumeSampleVectors(
            (expSizeParam, descParam) -> {
                expSize.set(expSizeParam);

                desc.set(descParam);
            },
            (v) -> assertEquals("Expected size for " + desc.get(),
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
        consumeSampleVectors(v -> new ElementsChecker(v).assertCloseEnough(v));
    }

    /** */ @Test
    public void copyTest() {
        consumeSampleVectors(v -> new ElementsChecker(v).assertCloseEnough(v.copy()));
    }

    /** */ @Test
    public void divideTest() {
        operationTest((val, operand) -> val / operand, Vector::divide);
    }

    /** */ @Test
    public void likeTest() {
        for (int card : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            consumeSampleVectors(v -> assertEquals("Expect size equal to cardinality.", card, v.like(card).size()));
    }

    /** */ @Test
    public void minusTest() {
        operationVectorTest((operand1, operand2) -> operand1 - operand2, Vector::minus);
    }

    /** */ @Test
    public void normalizeTest() {
        normalizeTest((val, len) -> val / len, Vector::normalize);
    }

    /** */ @Test
    public void normalizePowerTest() { // TODO write test

    }

    /** */ @Test
    public void logNormalizeTest() {
        normalizeTest((val, len) -> Math.log1p(val) / (len * Math.log(2)), Vector::logNormalize);
    }

    /** */ @Test
    public void logNormalizePowerTest() { // TODO write test

    }

    /** */ @Test
    public void kNormTest() { // TODO write test

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
    public void viewPartTest() { // TODO write test

    }

    /** */ @Test
    public void sumTest() { // TODO write test

    }

    /** */ @Test
    public void crossTest() { // TODO write test

    }

    /** */ @Test
    public void getLookupCostTest() { // TODO write test

    }

    /** */ @Test
    public void isAddConstantTimeTest() {
        alwaysTrueAttributeTest(DenseLocalOnHeapVector::isAddConstantTime);
    }

    /** */ @Test
    public void clusterGroupTest() { // TODO write test

    }

    /** */ @Test
    public void guidTest() { // TODO write test

    }

    /** */
    private void normalizeTest(BiFunction<Double, Double, Double> operation,
        Function<Vector, Vector> vecOperation) {
        consumeSampleVectors(v -> {
            final int size = v.size();

            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref);

            double len = 0;

            for (double val : ref)
                len += val * val;

            len = Math.sqrt(len);

            for (int idx = 0; idx < size; idx++)
                ref[idx] = operation.apply(ref[idx], len);

            checker.assertCloseEnough(vecOperation.apply(v), ref);
        });
    }



    /** */
    private void operationVectorTest(BiFunction<Double, Double, Double> operation,
        BiFunction<Vector, Vector, Vector> vecOperation) {
        consumeSampleVectors(v -> {
            // TODO find out if more elaborate testing scenario is needed or it's okay as is.

            final int size = v.size();

            final double[] ref = new double[size];

            final ElementsChecker checker = new ElementsChecker(v, ref);

            final Vector operand = v.copy();

            for (int idx = 0; idx < size; idx++)
                ref[idx] = operation.apply(ref[idx], ref[idx]);

            checker.assertCloseEnough(vecOperation.apply(v, operand), ref);
        });
    }

    /** */
    private void operationTest(BiFunction<Double, Double, Double> operation,
        BiFunction<Vector, Double, Vector> vecOperation) {
        for (double value : new double[] {0, 0.1, 1, 2, 10})
            consumeSampleVectors(v -> {
                final int size = v.size();

                final double[] ref = new double[size];

                final ElementsChecker checker = new ElementsChecker(v, ref);

                for (int idx = 0; idx < size; idx++)
                    ref[idx] = operation.apply(ref[idx], value);

                checker.assertCloseEnough(vecOperation.apply(v, value), ref);
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
    private void consumeSampleVectors(Consumer<DenseLocalOnHeapVector> consumer) {
        consumeSampleVectors(null, consumer);
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Integer, String> paramsConsumer,
        Consumer<DenseLocalOnHeapVector> consumer) {
        for (int size : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            for (int delta : new int[] {-1, 0, 1})
                for (boolean shallowCopy : new boolean[] {false, true}) {
                    final int expSize = size + delta;

                    if (paramsConsumer != null)
                        paramsConsumer.accept(expSize, "size " + expSize + ", shallow copy " + shallowCopy);

                    consumer.accept(new DenseLocalOnHeapVector(new double[expSize], shallowCopy));
                }
    }

    /** */
    private static class ElementsChecker {
        /** */
        ElementsChecker(Vector v, double[] ref) {
            init(v, ref);
        }

        /** */
        ElementsChecker(Vector v) {
            this(v, null);
        }

        /** */
        void assertCloseEnough(Vector obtained, double[] exp) {
            final int size = obtained.size();

            for (int i = 0; i < size; i++) {
                final Vector.Element e = obtained.getElement(i);

                final Metric metric = new Metric(exp == null ? i : exp[i], e.get());

                assertEquals("Vector index.", i, e.index());

                assertTrue("Not close enough at index " + i + ", size " + size + ", " + metric, metric.closeEnough());
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

