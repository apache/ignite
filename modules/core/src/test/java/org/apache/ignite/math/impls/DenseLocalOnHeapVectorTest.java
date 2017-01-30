package org.apache.ignite.math.impls;

import org.junit.Test;

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
