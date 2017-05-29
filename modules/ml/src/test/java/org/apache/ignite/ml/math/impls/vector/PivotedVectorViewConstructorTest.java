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
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class PivotedVectorViewConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    private static final SampleParams sampleParams = new SampleParams();

    /** */
    @Test(expected = NullPointerException.class)
    public void nullVecParamTest() {
        assertEquals("Expect exception due to null vector param.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(null, sampleParams.pivot).size());
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void nullVecParam2Test() {
        assertEquals("Expect exception due to null vector param, with unpivot.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(null, sampleParams.pivot, sampleParams.unpivot).size());
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void nullPivotParamTest() {
        assertEquals("Expect exception due to null pivot param.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, null).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullPivotParam2Test() {
        assertEquals("Expect exception due to null pivot param, with unpivot.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, null, sampleParams.unpivot).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullUnpivotParam2Test() {
        assertEquals("Expect exception due to null unpivot param.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, sampleParams.pivot, null).size());
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void emptyPivotTest() {
        assertEquals("Expect exception due to empty pivot param.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, new int[] {}).size());
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void emptyPivot2Test() {
        assertEquals("Expect exception due to empty pivot param, with unpivot.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, new int[] {}, sampleParams.unpivot).size());
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void wrongPivotTest() {
        assertEquals("Expect exception due to wrong pivot param.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, new int[] {0}).size());
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void wrongPivot2Test() {
        assertEquals("Expect exception due to wrong pivot param, with unpivot.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, new int[] {0}, sampleParams.unpivot).size());
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void emptyUnpivotTest() {
        assertEquals("Expect exception due to empty unpivot param.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, sampleParams.pivot, new int[] {}).size());
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void wrongUnpivotTest() {
        assertEquals("Expect exception due to wrong unpivot param, with unpivot.", IMPOSSIBLE_SIZE,
            new PivotedVectorView(sampleParams.vec, sampleParams.pivot, new int[] {0}).size());
    }

    /** */
    @Test
    public void basicPivotTest() {
        final PivotedVectorView pvv = new PivotedVectorView(sampleParams.vec, sampleParams.pivot);

        final int size = sampleParams.vec.size();

        assertEquals("View size differs from expected.", size, pvv.size());

        assertSame("Base vector differs from expected.", sampleParams.vec, pvv.getBaseVector());

        for (int idx = 0; idx < size; idx++) {
            assertEquals("Sample pivot and unpivot differ from expected",
                idx, sampleParams.unpivot[sampleParams.pivot[idx]]);

            assertEquals("Pivot differs from expected at index " + idx,
                sampleParams.pivot[idx], pvv.pivot(idx));

            assertEquals("Default unpivot differs from expected at index " + idx,
                sampleParams.unpivot[idx], pvv.unpivot(idx));

            final Metric metric = new Metric(sampleParams.vec.get(idx), pvv.get(pvv.pivot(idx)));

            assertTrue("Not close enough at index " + idx + ", " + metric, metric.closeEnough());
        }

        for (int idx = 0; idx < size; idx++) {
            sampleParams.vec.set(idx, sampleParams.vec.get(idx) + idx + 1);

            final Metric metric = new Metric(sampleParams.vec.get(idx), pvv.get(pvv.pivot(idx)));

            assertTrue("Modified value not close enough at index " + idx + ", " + metric, metric.closeEnough());
        }
    }

    /** */
    @Test
    public void basicUnpivotTest() {
        final PivotedVectorView pvv = new PivotedVectorView(sampleParams.vec, sampleParams.pivot, sampleParams.unpivot);

        final int size = sampleParams.vec.size();

        assertEquals("View size differs from expected.", size, pvv.size());

        for (int idx = 0; idx < size; idx++) {
            assertEquals("Unpivot differs from expected at index " + idx,
                sampleParams.unpivot[idx], pvv.unpivot(idx));

            final Metric metric = new Metric(sampleParams.vec.get(idx), pvv.get(pvv.unpivot(idx)));

            assertTrue("Not close enough at index " + idx + ", " + metric, metric.closeEnough());
        }
    }

    /** */
    private static class SampleParams {
        /** */
        final double[] data = new double[] {0, 1};
        /** */
        final Vector vec = new DenseLocalOnHeapVector(data);
        /** */
        final int[] pivot = new int[] {1, 0};
        /** */
        final int[] unpivot = new int[] {1, 0};
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
