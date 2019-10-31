/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.test.TestBase;
import org.h2.util.MathUtils;

/**
 * Tests math utility methods.
 */
public class TestMathUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testRandom();
        testNextPowerOf2Int();
    }

    private void testRandom() {
        int bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << MathUtils.randomInt(8);
        }
        assertEquals(255, bits);
        bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << MathUtils.secureRandomInt(8);
        }
        assertEquals(255, bits);
        bits = 0;
        for (int i = 0; i < 1000; i++) {
            bits |= 1 << (MathUtils.secureRandomLong() & 7);
        }
        assertEquals(255, bits);
        // just verify the method doesn't throw an exception
        byte[] data = MathUtils.generateAlternativeSeed();
        assertTrue(data.length > 10);
    }

    private void testNextPowerOf2Int() {
        // the largest power of two that fits into an integer
        final int largestPower2 = 0x40000000;
        int[] testValues = { 0, 1, 2, 3, 4, 12, 17, 500, 1023,
                largestPower2 - 500, largestPower2 };
        int[] resultValues = { 1, 1, 2, 4, 4, 16, 32, 512, 1024,
                largestPower2, largestPower2 };

        for (int i = 0; i < testValues.length; i++) {
            assertEquals(resultValues[i], MathUtils.nextPowerOf2(testValues[i]));
        }
    }

}
