/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.IntIntHashMap;

/**
 * Tests the IntHashMap class.
 */
public class TestIntIntHashMap extends TestBase {

    private final Random rand = new Random();

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
        IntIntHashMap map = new IntIntHashMap();
        map.put(1, 1);
        map.put(1, 2);
        assertEquals(1, map.size());
        map.put(0, 1);
        map.put(0, 2);
        assertEquals(2, map.size());
        rand.setSeed(10);
        test(true);
        test(false);
    }

    private void test(boolean random) {
        int len = 2000;
        int[] x = new int[len];
        for (int i = 0; i < len; i++) {
            int key = random ? rand.nextInt() : i;
            x[i] = key;
        }
        IntIntHashMap map = new IntIntHashMap();
        for (int i = 0; i < len; i++) {
            map.put(x[i], i);
        }
        for (int i = 0; i < len; i++) {
            if (map.get(x[i]) != i) {
                throw new AssertionError("get " + x[i] + " = " + map.get(i) +
                        " should be " + i);
            }
        }
        for (int i = 1; i < len; i += 2) {
            map.remove(x[i]);
        }
        for (int i = 1; i < len; i += 2) {
            if (map.get(x[i]) != -1) {
                throw new AssertionError("get " + x[i] + " = " + map.get(i) +
                        " should be <=0");
            }
        }
        for (int i = 1; i < len; i += 2) {
            map.put(x[i], i);
        }
        for (int i = 0; i < len; i++) {
            if (map.get(x[i]) != i) {
                throw new AssertionError("get " + x[i] + " = " + map.get(i) +
                        " should be " + i);
            }
        }
    }
}
