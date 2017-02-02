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
import java.util.function.Predicate;

import static org.junit.Assert.*;

/** */
public class DenseLocalOnHeapVectorTest {
    /** */ @Test
    public void sizeTest() {
        for (int size : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            for (int delta : new int[] {-1, 0, 1})
                for (boolean shallowCopy : new boolean[] {false, true}) {
                    final int expSize = size + delta;

                    assertEquals("expected size " + expSize + ", shallow copy " + shallowCopy, expSize,
                        new DenseLocalOnHeapVector(new double[expSize], shallowCopy).size());
                }
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
    public void cloneTest() {
        assertFalse("expect not Cloneable", Cloneable.class.isAssignableFrom(DenseLocalOnHeapVector.class));

        for (DenseLocalOnHeapVector orig : new DenseLocalOnHeapVector[] {
            new DenseLocalOnHeapVector(),
            new DenseLocalOnHeapVector(null, true),
            new DenseLocalOnHeapVector(new double[0], true),
            new DenseLocalOnHeapVector(new double[0], false),
            new DenseLocalOnHeapVector(new double[] {1}, true),
            new DenseLocalOnHeapVector(new double[] {1}, false)
        })

        cloneTest(orig);
    }

    /** */ @Test
    public void allTest() { // TODO write test

    }

    /** */ @Test
    public void nonZeroesTest() { // TODO write test

    }

    /** */ @Test
    public void getElementTest() {
        for (int size : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            for (int delta : new int[] {-1, 0, 1})
                for (boolean shallowCopy : new boolean[] {false, true}) {
                    final int expSize = size + delta;

                    final DenseLocalOnHeapVector v = new DenseLocalOnHeapVector(new double[expSize], shallowCopy);

                    for (Vector.Element e : v.all())
                        e.set(e.index());

                    assertCloseEnough(v);
                }
    }

    /** */ @Test
    public void assignTest() { // TODO write test

    }

    /** */ @Test
    public void assign1Test() { // TODO write test

    }

    /** */ @Test
    public void assign2Test() { // TODO write test

    }

    /** */ @Test
    public void mapTest() { // TODO write test

    }

    /** */ @Test
    public void map1Test() { // TODO write test

    }

    /** */ @Test
    public void map2Test() { // TODO write test

    }

    /** */ @Test
    public void divideTest() { // TODO write test

    }

    /** */ @Test
    public void dotTest() { // TODO write test

    }

    /** */ @Test
    public void getTest() { // TODO write test

    }

    /** */ @Test
    public void getXTest() { // TODO write test

    }

    /** */ @Test
    public void likeTest() { // TODO write test

    }

    /** */ @Test
    public void minusTest() { // TODO write test

    }

    /** */ @Test
    public void normalizeTest() { // TODO write test

    }

    /** */ @Test
    public void normalize1Test() { // TODO write test

    }

    /** */ @Test
    public void logNormalizeTest() { // TODO write test

    }

    /** */ @Test
    public void logNormalize1Test() { // TODO write test

    }

    /** */ @Test
    public void normTest() { // TODO write test

    }

    /** */ @Test
    public void minValueTest() { // TODO write test

    }

    /** */ @Test
    public void maxValueTest() { // TODO write test

    }

    /** */ @Test
    public void plusTest() { // TODO write test

    }

    /** */ @Test
    public void plus1Test() { // TODO write test

    }

    /** */ @Test
    public void setTest() { // TODO write test

    }

    /** */ @Test
    public void setXTest() { // TODO write test

    }

    /** */ @Test
    public void incrementXTest() { // TODO write test

    }

    /** */ @Test
    public void nonDefaultElementsTest() { // TODO write test

    }

    /** */ @Test
    public void nonZeroElementsTest() { // TODO write test

    }

    /** */ @Test
    public void timesTest() { // TODO write test

    }

    /** */ @Test
    public void times1Test() { // TODO write test

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
    public void foldMapTest() { // TODO write test

    }

    /** */ @Test
    public void getLengthSquaredTest() { // TODO write test

    }

    /** */ @Test
    public void getDistanceSquaredTest() { // TODO write test

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

    /** */ @Test
    public void writeExternalTest() { // TODO write test

    }

    /** */ @Test
    public void readExternalTest() { // TODO write test

    }

    /** */
    private void cloneTest(DenseLocalOnHeapVector orig) {
        final DenseLocalOnHeapVector clone = orig.clone();

        assertNotSame(orig, clone);

        assertSame(orig.getClass(), clone.getClass());

        assertEquals(orig, clone);
    }

    /** */
    private void alwaysTrueAttributeTest(Predicate<DenseLocalOnHeapVector> pred) {
        assertTrue("default size for null args",
            pred.test(new DenseLocalOnHeapVector((Map<String, Object>)null)));

        assertTrue("size from args",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{ put("size", 99); }})));

        final double[] test = new double[99];

        assertTrue("size from array in args",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("shallowCopy", false);
            }})));

        assertTrue("size from array in args, shallow copy",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("shallowCopy", true);
            }})));

        assertTrue("default constructor",
            pred.test(new DenseLocalOnHeapVector()));

        assertTrue("null array shallow copy",
            pred.test(new DenseLocalOnHeapVector(null, true)));

        assertTrue("0 size shallow copy",
            pred.test(new DenseLocalOnHeapVector(new double[0], true)));

        assertTrue("0 size",
            pred.test(new DenseLocalOnHeapVector(new double[0], false)));

        assertTrue("1 size shallow copy",
            pred.test(new DenseLocalOnHeapVector(new double[1], true)));

        assertTrue("1 size",
            pred.test(new DenseLocalOnHeapVector(new double[1], false)));

        assertTrue("0 size default copy",
            pred.test(new DenseLocalOnHeapVector(new double[0])));

        assertTrue("1 size default copy",
            pred.test(new DenseLocalOnHeapVector(new double[1])));
    }

    /** */
    private void assertCloseEnough(DenseLocalOnHeapVector obtained) {
        final int size = obtained.size();

        for (int i = 0; i < size; i++) {
            final Vector.Element e = obtained.getElement(i);

            final Metric metric = new Metric(i, e.get());

            assertEquals("vector index", i, e.index());

            assertTrue("not close enough at index " + i + ", size " + size + ", " + metric, metric.closeEnough());
        }
    }

    /** */
    private static class Metric {
        /** */
        private static final double tolerance = 0.1;

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
            return Math.abs(exp - obtained) < tolerance;
        }

        /** @{inheritDoc} */
        @Override public String toString() {
            return "Metric{" + "expected=" + exp +
                ", obtained=" + obtained +
                ", tolerance=" + tolerance +
                '}';
        }
    }
}
