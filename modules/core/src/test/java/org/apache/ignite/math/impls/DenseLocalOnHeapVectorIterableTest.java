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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/** */
public class DenseLocalOnHeapVectorIterableTest {
    /** */ @Test
    public void allTest() {
        final AtomicReference<Integer> expSize = new AtomicReference<>(0);

        final AtomicReference<Boolean> shallowCp = new AtomicReference<>(false);

        consumeSampleVectors(
            (expSizeParam, shallowCopyParam) -> {
                expSize.set(expSizeParam);

                shallowCp.set(shallowCopyParam);
            }, (v) -> {
                final String desc = "size " + expSize.get() + ", shallow copy " + shallowCp.get();

                int expIdx = 0;

                for (Vector.Element e : v.all()) {
                    int actualIdx = e.index();

                    assertEquals("unexpected index for " + desc,
                        expIdx, actualIdx);

                    expIdx++;
                }

                assertEquals("unexpected amount of elements for " + desc,
                    expIdx, v.size());
            }
        );
    }

    /** */ @Test
    public void allTestBound() {
        final AtomicReference<Integer> expSize = new AtomicReference<>(0);

        final AtomicReference<Boolean> shallowCp = new AtomicReference<>(false);

        consumeSampleVectors(
            (expSizeParam, shallowCopyParam) -> {
                expSize.set(expSizeParam);

                shallowCp.set(shallowCopyParam);
            },
            v -> iteratorTestBound(v.all().iterator(), expSize.get(), shallowCp.get())
        );
    }

    /** */
    private void iteratorTestBound(Iterator<Vector.Element> it, int expSize, boolean shallowCp) {
        while (it.hasNext())
            assertNotNull(it.next());

        boolean expECaught = false;

        try {
            it.next();
        } catch (NoSuchElementException e) {
            expECaught = true;
        }

        assertTrue("expected exception missed for size " + expSize + ", shallow copy " + shallowCp,
            expECaught);
    }

    /** */ @Test
    public void nonZeroesTestBasic() {
        final int size = 5;

        final double[] nonZeroesOddData = new double[size], nonZeroesEvenData = new double[size];

        for (int idx = 0; idx < size; idx++) {
            final boolean odd = (idx & 1) == 1;

            nonZeroesOddData[idx] = odd ? 1 : 0;

            nonZeroesEvenData[idx] = odd ? 0 : 1;
        }

        assertTrue("arrays failed to initialize",
            !isZero(nonZeroesEvenData[0])
                && isZero(nonZeroesEvenData[1])
                && isZero(nonZeroesOddData[0])
                && !isZero(nonZeroesOddData[1]));

        final DenseLocalOnHeapVector nonZeroesEvenVec = new DenseLocalOnHeapVector(nonZeroesEvenData),
            nonZeroesOddVec = new DenseLocalOnHeapVector(nonZeroesOddData);

        assertTrue("vectors failed to initialize",
            !isZero(nonZeroesEvenVec.getElement(0).get())
                && isZero(nonZeroesEvenVec.getElement(1).get())
                && isZero(nonZeroesOddVec.getElement(0).get())
                && !isZero(nonZeroesOddVec.getElement(1).get()));

        assertTrue("iterator(s) failed to start",
            nonZeroesEvenVec.nonZeroes().iterator().next() != null
                && nonZeroesOddVec.nonZeroes().iterator().next() != null);

        int nonZeroesActual = 0;

        for (Vector.Element e : nonZeroesEvenVec.nonZeroes()) {
            final int idx = e.index();

            final boolean odd = (idx & 1) == 1;

            final double val = e.get();

            assertTrue("not an even index " + idx + ", for value " + val, !odd);

            assertTrue("zero value " + val + " at even index " + idx, !isZero(val));

            nonZeroesActual++;
        }

        final int nonZeroesOddExp = (size + 1) / 2;

        assertEquals("unexpected num of iterated odd non-zeroes", nonZeroesOddExp, nonZeroesActual);

        assertEquals("unexpected nonZeroElements of odd", nonZeroesOddExp, nonZeroesEvenVec.nonZeroElements());

        nonZeroesActual = 0;

        for (Vector.Element e : nonZeroesOddVec.nonZeroes()) {
            final int idx = e.index();

            final boolean odd = (idx & 1) == 1;

            final double val = e.get();

            assertTrue("not an odd index " + idx + ", for value " + val, odd);

            assertTrue("zero value " + val + " at even index " + idx, !isZero(val));

            nonZeroesActual++;
        }

        final int nonZeroesEvenExp = size / 2;

        assertEquals("unexpected num of iterated even non-zeroes", nonZeroesEvenExp, nonZeroesActual);

        assertEquals("unexpected nonZeroElements of even", nonZeroesEvenExp, nonZeroesOddVec.nonZeroElements());
    }

    /** */ @Test
    public void nonZeroesTest() {
        consumeSampleVectors(
            v -> consumeSampleVectorsWithZeroes(v, (vec, numZeroes)
                -> {
                final int size = vec.size();

                int numZeroesActual = size;

                for (Vector.Element e : vec.nonZeroes()) {
                    numZeroesActual--;

                    assertTrue("unexpected zero at index " + e.index(), !isZero(e.get()));
                }

                assertEquals("unexpected num zeroes at size " + size, (int)numZeroes, numZeroesActual);
            }));
    }

    /** */ @Test
    public void nonZeroesTestBound() {
        final AtomicReference<Integer> expSize = new AtomicReference<>(0);

        final AtomicReference<Boolean> shallowCp = new AtomicReference<>(false);

        consumeSampleVectors(
            (expSizeParam, shallowCopyParam) -> {
                expSize.set(expSizeParam);

                shallowCp.set(shallowCopyParam);
            },
            v -> consumeSampleVectorsWithZeroes(v, (vec, numZeroes)
                -> iteratorTestBound(vec.nonZeroes().iterator(), expSize.get(), shallowCp.get())));
    }

    /** */ @Test
    public void nonZeroElementsTest() {
        consumeSampleVectors(
            v -> consumeSampleVectorsWithZeroes(v, (vec, numZeroes)
                -> assertEquals("unexpected num zeroes at size " + vec.size(),
                (int)numZeroes, vec.size() - vec.nonZeroElements())));
    }

    /** */
    private void consumeSampleVectorsWithZeroes(DenseLocalOnHeapVector sample,
        BiConsumer<DenseLocalOnHeapVector, Integer> consumer) {
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
    private void fillWithNonZeroes(DenseLocalOnHeapVector sample) {
        int idx = 0;

        for (Vector.Element e : sample.all())
            e.set(1 + idx++);

        assertEquals("not all filled with non-zeroes", idx, sample.size());
    }

    /** */
    private void consumeSampleVectors(Consumer<DenseLocalOnHeapVector> consumer) {
        consumeSampleVectors(null, consumer);
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Integer, Boolean> paramsConsumer,
        Consumer<DenseLocalOnHeapVector> consumer) {
        for (int size : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            for (int delta : new int[] {-1, 0, 1})
                for (boolean shallowCopy : new boolean[] {false, true}) {
                    final int expSize = size + delta;

                    if (paramsConsumer != null)
                        paramsConsumer.accept(expSize, shallowCopy);

                    consumer.accept(new DenseLocalOnHeapVector(new double[expSize], shallowCopy));
                }
    }

    /** */
    private boolean isZero(double val) {
        return val == 0.0;
    }
}

