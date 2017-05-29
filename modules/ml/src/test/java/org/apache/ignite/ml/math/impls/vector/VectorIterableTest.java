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

package org.apache.ignite.ml.math.impls.vector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.junit.Test;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** */
public class VectorIterableTest {
    /** */
    @Test
    public void allTest() {
        consumeSampleVectors(
            (v, desc) -> {
                int expIdx = 0;

                for (Vector.Element e : v.all()) {
                    int actualIdx = e.index();

                    assertEquals("Unexpected index for " + desc,
                        expIdx, actualIdx);

                    expIdx++;
                }

                assertEquals("Unexpected amount of elements for " + desc,
                    expIdx, v.size());
            }
        );
    }

    /** */
    @Test
    public void allTestBound() {
        consumeSampleVectors(
            (v, desc) -> iteratorTestBound(v.all().iterator(), desc)
        );
    }

    /** */
    @Test
    public void nonZeroesTestBasic() {
        final int size = 5;

        final double[] nonZeroesOddData = new double[size], nonZeroesEvenData = new double[size];

        for (int idx = 0; idx < size; idx++) {
            final boolean odd = (idx & 1) == 1;

            nonZeroesOddData[idx] = odd ? 1 : 0;

            nonZeroesEvenData[idx] = odd ? 0 : 1;
        }

        assertTrue("Arrays failed to initialize.",
            !isZero(nonZeroesEvenData[0])
                && isZero(nonZeroesEvenData[1])
                && isZero(nonZeroesOddData[0])
                && !isZero(nonZeroesOddData[1]));

        final Vector nonZeroesEvenVec = new DenseLocalOnHeapVector(nonZeroesEvenData),
            nonZeroesOddVec = new DenseLocalOnHeapVector(nonZeroesOddData);

        assertTrue("Vectors failed to initialize.",
            !isZero(nonZeroesEvenVec.getElement(0).get())
                && isZero(nonZeroesEvenVec.getElement(1).get())
                && isZero(nonZeroesOddVec.getElement(0).get())
                && !isZero(nonZeroesOddVec.getElement(1).get()));

        assertTrue("Iterator(s) failed to start.",
            nonZeroesEvenVec.nonZeroes().iterator().next() != null
                && nonZeroesOddVec.nonZeroes().iterator().next() != null);

        int nonZeroesActual = 0;

        for (Vector.Element e : nonZeroesEvenVec.nonZeroes()) {
            final int idx = e.index();

            final boolean odd = (idx & 1) == 1;

            final double val = e.get();

            assertTrue("Not an even index " + idx + ", for value " + val, !odd);

            assertTrue("Zero value " + val + " at even index " + idx, !isZero(val));

            nonZeroesActual++;
        }

        final int nonZeroesOddExp = (size + 1) / 2;

        assertEquals("Unexpected num of iterated odd non-zeroes.", nonZeroesOddExp, nonZeroesActual);

        assertEquals("Unexpected nonZeroElements of odd.", nonZeroesOddExp, nonZeroesEvenVec.nonZeroElements());

        nonZeroesActual = 0;

        for (Vector.Element e : nonZeroesOddVec.nonZeroes()) {
            final int idx = e.index();

            final boolean odd = (idx & 1) == 1;

            final double val = e.get();

            assertTrue("Not an odd index " + idx + ", for value " + val, odd);

            assertTrue("Zero value " + val + " at even index " + idx, !isZero(val));

            nonZeroesActual++;
        }

        final int nonZeroesEvenExp = size / 2;

        assertEquals("Unexpected num of iterated even non-zeroes", nonZeroesEvenExp, nonZeroesActual);

        assertEquals("Unexpected nonZeroElements of even", nonZeroesEvenExp, nonZeroesOddVec.nonZeroElements());
    }

    /** */
    @Test
    public void nonZeroesTest() {
        // todo make RandomVector constructor that accepts a function and use it here
        //  in order to *reliably* test non-zeroes in there
        consumeSampleVectors(
            (v, desc) -> consumeSampleVectorsWithZeroes(v, (vec, numZeroes)
                -> {
                int numZeroesActual = vec.size();

                for (Vector.Element e : vec.nonZeroes()) {
                    numZeroesActual--;

                    assertTrue("Unexpected zero at " + desc + ", index " + e.index(), !isZero(e.get()));
                }

                assertEquals("Unexpected num zeroes at " + desc, (int)numZeroes, numZeroesActual);
            }));
    }

    /** */
    @Test
    public void nonZeroesTestBound() {
        consumeSampleVectors(
            (v, desc) -> consumeSampleVectorsWithZeroes(v, (vec, numZeroes)
                -> iteratorTestBound(vec.nonZeroes().iterator(), desc)));
    }

    /** */
    @Test
    public void nonZeroElementsTest() {
        consumeSampleVectors(
            (v, desc) -> consumeSampleVectorsWithZeroes(v, (vec, numZeroes)
                -> assertEquals("Unexpected num zeroes at " + desc,
                (int)numZeroes, vec.size() - vec.nonZeroElements())));
    }

    /** */
    @Test
    public void allSpliteratorTest() {
        consumeSampleVectors(
            (v, desc) -> {
                final String desc1 = " " + desc;

                Spliterator<Double> spliterator = v.allSpliterator();

                assertNotNull(MathTestConstants.NULL_VAL + desc1, spliterator);

                assertNull(MathTestConstants.NOT_NULL_VAL + desc1, spliterator.trySplit());

                assertTrue(MathTestConstants.UNEXPECTED_VAL + desc1, spliterator.hasCharacteristics(ORDERED | SIZED));

                if (!readOnly(v))
                    fillWithNonZeroes(v);

                spliterator = v.allSpliterator();

                assertNotNull(MathTestConstants.NULL_VAL + desc1, spliterator);

                assertEquals(MathTestConstants.VAL_NOT_EQUALS + desc1, spliterator.estimateSize(), v.size());

                assertEquals(MathTestConstants.VAL_NOT_EQUALS + desc1, spliterator.getExactSizeIfKnown(), v.size());

                assertTrue(MathTestConstants.UNEXPECTED_VAL + desc1, spliterator.hasCharacteristics(ORDERED | SIZED));

                Spliterator<Double> secondHalf = spliterator.trySplit();

                assertNull(MathTestConstants.NOT_NULL_VAL + desc1, secondHalf);

                spliterator.tryAdvance(x -> {
                });
            }
        );
    }

    /** */
    @Test
    public void nonZeroSpliteratorTest() {
        consumeSampleVectors(
            (v, desc) -> consumeSampleVectorsWithZeroes(v, (vec, numZeroes)
                -> {
                final String desc1 = " Num zeroes " + numZeroes + " " + desc;

                Spliterator<Double> spliterator = vec.nonZeroSpliterator();

                assertNotNull(MathTestConstants.NULL_VAL + desc1, spliterator);

                assertNull(MathTestConstants.NOT_NULL_VAL + desc1, spliterator.trySplit());

                assertTrue(MathTestConstants.UNEXPECTED_VAL + desc1, spliterator.hasCharacteristics(ORDERED | SIZED));

                spliterator = vec.nonZeroSpliterator();

                assertNotNull(MathTestConstants.NULL_VAL + desc1, spliterator);

                assertEquals(MathTestConstants.VAL_NOT_EQUALS + desc1, spliterator.estimateSize(), vec.size() - numZeroes);

                assertEquals(MathTestConstants.VAL_NOT_EQUALS + desc1, spliterator.getExactSizeIfKnown(), vec.size() - numZeroes);

                assertTrue(MathTestConstants.UNEXPECTED_VAL + desc1, spliterator.hasCharacteristics(ORDERED | SIZED));

                Spliterator<Double> secondHalf = spliterator.trySplit();

                assertNull(MathTestConstants.NOT_NULL_VAL + desc1, secondHalf);

                double[] data = new double[vec.size()];

                for (Vector.Element e : vec.all())
                    data[e.index()] = e.get();

                spliterator = vec.nonZeroSpliterator();

                assertNotNull(MathTestConstants.NULL_VAL + desc1, spliterator);

                assertEquals(MathTestConstants.VAL_NOT_EQUALS + desc1, spliterator.estimateSize(),
                    Arrays.stream(data).filter(x -> x != 0d).count());

                assertEquals(MathTestConstants.VAL_NOT_EQUALS + desc1, spliterator.getExactSizeIfKnown(),
                    Arrays.stream(data).filter(x -> x != 0d).count());

                assertTrue(MathTestConstants.UNEXPECTED_VAL + desc1, spliterator.hasCharacteristics(ORDERED | SIZED));

                secondHalf = spliterator.trySplit();

                assertNull(MathTestConstants.NOT_NULL_VAL + desc1, secondHalf);

                if (!spliterator.tryAdvance(x -> {
                }))
                    fail(MathTestConstants.NO_NEXT_ELEMENT + desc1);
            }));
    }

    /** */
    private void iteratorTestBound(Iterator<Vector.Element> it, String desc) {
        while (it.hasNext())
            assertNotNull(it.next());

        boolean expECaught = false;

        try {
            it.next();
        }
        catch (NoSuchElementException e) {
            expECaught = true;
        }

        assertTrue("Expected exception missed for " + desc,
            expECaught);
    }

    /** */
    private void consumeSampleVectorsWithZeroes(Vector sample,
        BiConsumer<Vector, Integer> consumer) {
        if (readOnly(sample)) {
            int numZeroes = 0;

            for (Vector.Element e : sample.all())
                if (isZero(e.get()))
                    numZeroes++;

            consumer.accept(sample, numZeroes);

            return;
        }

        fillWithNonZeroes(sample);

        consumer.accept(sample, 0);

        final int sampleSize = sample.size();

        if (sampleSize == 0)
            return;

        for (Vector.Element e : sample.all())
            e.set(0);

        consumer.accept(sample, sampleSize);

        fillWithNonZeroes(sample);

        for (int testIdx : new int[] {0, sampleSize / 2, sampleSize - 1}) {
            final Vector.Element e = sample.getElement(testIdx);

            final double backup = e.get();

            e.set(0);

            consumer.accept(sample, 1);

            e.set(backup);
        }

        if (sampleSize < 3)
            return;

        sample.getElement(sampleSize / 3).set(0);

        sample.getElement((2 * sampleSize) / 3).set(0);

        consumer.accept(sample, 2);
    }

    /** */
    private void fillWithNonZeroes(Vector sample) {
        int idx = 0;

        for (Vector.Element e : sample.all())
            e.set(1 + idx++);

        assertEquals("Not all filled with non-zeroes", idx, sample.size());
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        new VectorImplementationsFixtures().consumeSampleVectors(null, consumer);
    }

    /** */
    private boolean isZero(double val) {
        return val == 0.0;
    }

    /** */
    private boolean readOnly(Vector v) {
        return v instanceof RandomVector || v instanceof ConstantVector;
    }
}

