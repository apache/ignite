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

import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class SingleElementVectorViewConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    private static final SampleHelper helper = new SampleHelper();

    /** */
    @Test(expected = AssertionError.class)
    public void nullVecParamTest() {
        assertEquals("Expect exception due to null vector param.", IMPOSSIBLE_SIZE,
            new SingleElementVectorView(null, helper.idx).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void negativeIdxParamTest() {
        assertEquals("Expect exception due to negative index param.", IMPOSSIBLE_SIZE,
            new SingleElementVectorView(helper.vec, -1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void tooLargeIdxParamTest() {
        assertEquals("Expect exception due to too large index param.", IMPOSSIBLE_SIZE,
            new SingleElementVectorView(helper.vec, helper.vec.size()).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void emptyVecParamTest() {
        assertEquals("Expect exception due to empty vector param.", IMPOSSIBLE_SIZE,
            new SingleElementVectorView(helper.vecEmpty, 0).size());
    }

    /** */
    @Test
    public void basicTest() {
        final int[] sizes = new int[] {1, 4, 8};

        for (int size : sizes)
            for (int idx = 0; idx < size; idx++)
                basicTest(size, idx);
    }

    /** */
    private void basicTest(int size, int idx) {
        final Double expVal = (double)(size - idx);

        Vector orig = helper.newSample(size, idx, expVal);

        SingleElementVectorView svv = new SingleElementVectorView(orig, idx);

        assertEquals("Size differs from expected", size, svv.size());

        assertTrue("Expect value " + expVal + " at index " + idx + " for size " + size,
            expVal.equals(svv.get(idx)));

        final double delta = 1.0;

        svv.set(idx, expVal - delta);

        assertTrue("Expect value " + expVal + " at index " + idx + " for size " + size,
            expVal.equals(orig.get(idx) + delta));

        final Double zero = 0.0;

        for (int i = 0; i < size; i++) {
            if (i == idx)
                continue;

            assertTrue("Expect zero at index " + i + " for size " + size,
                zero.equals(svv.get(i)));

            boolean eCaught = false;

            try {
                svv.set(i, 1.0);
            }
            catch (UnsupportedOperationException uoe) {
                eCaught = true;
            }

            assertTrue("Expect " + UnsupportedOperationException.class.getSimpleName()
                + " at index " + i + " for size " + size, eCaught);
        }
    }

    /** */
    private static class SampleHelper {
        /** */
        final double[] data = new double[] {0, 1};
        /** */
        final Vector vec = new DenseLocalOnHeapVector(data);
        /** */
        final Vector vecEmpty = new DenseLocalOnHeapVector(new double[] {});
        /** */
        final int idx = 0;

        /** */
        Vector newSample(int size, int idx, double expVal) {
            final Vector v = new DenseLocalOnHeapVector(size);

            for (int i = 0; i < size; i++)
                v.set(i, i == idx ? expVal : i);

            return v;
        }
    }
}
