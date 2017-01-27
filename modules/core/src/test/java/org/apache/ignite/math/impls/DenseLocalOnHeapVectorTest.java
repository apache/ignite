package org.apache.ignite.math.impls;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/** */
public class DenseLocalOnHeapVectorTest {
    /** */ private static final int IMPOSSIBLE_SIZE = -1;

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapConstructorInvalidTest() {
        assertEquals("expect exception due to invalid args",IMPOSSIBLE_SIZE,
                new DenseLocalOnHeapVector(new HashMap<String, Object>(){{put("invalid", 99);}}).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapConstructorMissingTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr",  new double[0]);

            put("shallowCopyMissing", "whatever");
        }};

        assertEquals("expect exception due to missing args",
                -1, new DenseLocalOnHeapVector(test).size());
    }

    /** */ @Test(expected = ClassCastException.class)
    public void mapConstructorInvalidArrTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new int[0]);

            put("shallowCopy", true);
        }};

        assertEquals("expect exception due to invalid arr type", IMPOSSIBLE_SIZE,
                new DenseLocalOnHeapVector(test).size());
    }

    /** */ @Test(expected = ClassCastException.class)
    public void mapConstructorInvalidCopyTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new double[0]);

            put("shallowCopy", 0);
        }};

        assertEquals("expect exception due to invalid copy type", IMPOSSIBLE_SIZE,
                new DenseLocalOnHeapVector(test).size());
    }

    /** */ @Test
    public void mapConstructorTest() {
        assertEquals("default size for null args",100,
                new DenseLocalOnHeapVector((Map<String, Object>)null).size());

        assertEquals("size from args", 99,
                new DenseLocalOnHeapVector(new HashMap<String, Object>(){{ put("size", 99); }}).size());

        final double[] test = new double[99];

        assertEquals("size from array in args", test.length,
                new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                    put("arr", test);
                    put("shallowCopy", false);
                }}).size());

        assertEquals("size from array in args", test.length,
                new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                    put("arr", test);
                    put("shallowCopy", true);
                }}).size());
    }

    /** */ @Test(expected = NegativeArraySizeException.class)
    public void negativeSizeConstructorTest() {
        assertEquals("negative size", IMPOSSIBLE_SIZE,
                new DenseLocalOnHeapVector(-1).size());
    }

    /** */ @Test(expected = NullPointerException.class)
    public void nullCopyConstructorTest() {
        assertEquals("null array to non-shallow copy", IMPOSSIBLE_SIZE,
                new DenseLocalOnHeapVector(null, false).size());
    }

    /** */ @Test(expected = NullPointerException.class)
    public void nullDefaultCopyConstructorTest() {
        assertEquals("null array default copy", IMPOSSIBLE_SIZE,
                new DenseLocalOnHeapVector((double[])null).size());
    }

    /** */ @Test
    public void primitiveConstructorTest() {
        assertEquals("default constructor", 100,
                new DenseLocalOnHeapVector().size());

        assertEquals("null array shallow copy", 0,
                new DenseLocalOnHeapVector(null, true).size());

        assertEquals("0 size shallow copy", 0,
                new DenseLocalOnHeapVector(new double[0], true).size());

        assertEquals("0 size", 0,
                new DenseLocalOnHeapVector(new double[0], false).size());

        assertEquals("1 size shallow copy", 1,
                new DenseLocalOnHeapVector(new double[1], true).size());

        assertEquals("1 size", 1,
                new DenseLocalOnHeapVector(new double[1], false).size());

        assertEquals("0 size default copy", 0,
                new DenseLocalOnHeapVector(new double[0]).size());

        assertEquals("1 size default copy", 1,
                new DenseLocalOnHeapVector(new double[1]).size());
    }

    /** */ @Test
    public void sizeTest() {
        for (int size : new int[] {1, 2, 4, 8, 16, 32, 64, 128}) {
            assertEquals("expected size " + size, size,
                    new DenseLocalOnHeapVector(new double[size], true).size());

            assertEquals("expected size " + size, size,
                    new DenseLocalOnHeapVector(new double[size], false).size());
        }
        // TODO complete test
    }

    /** */ @Test
    public void isDenseTest() { // TODO write test

    }

    /** */ @Test
    public void isSequentialAccessTest() { // TODO write test

    }

    /** */ @Test
    public void cloneTest() { // TODO write test

    }

    /** */ @Test
    public void allTest() { // TODO write test

    }

    /** */ @Test
    public void nonZeroesTest() { // TODO write test

    }

    /** */ @Test
    public void getElementTest() { // TODO write test

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
    public void isAddConstantTimeTest() { // TODO write test

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

}
