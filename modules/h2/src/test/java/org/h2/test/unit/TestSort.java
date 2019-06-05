/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.dev.sort.InPlaceStableMergeSort;
import org.h2.dev.sort.InPlaceStableQuicksort;
import org.h2.test.TestBase;

/**
 * Tests the stable in-place sorting implementations.
 */
public class TestSort extends TestBase {

    /**
     * The number of times the compare method was called.
     */
    AtomicInteger compareCount = new AtomicInteger();

    /**
     * The comparison object used in this test.
     */
    Comparator<Long> comp = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            compareCount.incrementAndGet();
            return Long.compare(o1 >> 32, o2 >> 32);
        }
    };

    private final Long[] array = new Long[100000];
    private Class<?> clazz;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        test(InPlaceStableMergeSort.class);
        test(InPlaceStableQuicksort.class);
        test(Arrays.class);
    }

    private void test(Class<?> c) throws Exception {
        this.clazz = c;
        ordered(array);
        shuffle(array);
        stabilize(array);
        test("random");
        ordered(array);
        stabilize(array);
        test("ordered");
        ordered(array);
        reverse(array);
        stabilize(array);
        test("reverse");
        ordered(array);
        stretch(array);
        shuffle(array);
        stabilize(array);
        test("few random");
        ordered(array);
        stretch(array);
        stabilize(array);
        test("few ordered");
        ordered(array);
        reverse(array);
        stretch(array);
        stabilize(array);
        test("few reverse");
        // System.out.println();
    }

    /**
     * Sort the array and verify the result.
     *
     * @param type the type of data
     */
    private void  test(@SuppressWarnings("unused") String type) throws Exception {
        compareCount.set(0);

        // long t = System.nanoTime();

        clazz.getMethod("sort", Object[].class, Comparator.class).invoke(null,
                array, comp);

        // System.out.printf(
        //    "%4d ms; %10d comparisons order: %s data: %s\n",
        //    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t),
        //    compareCount.get(), clazz, type);

        verify(array);

    }

    private static void verify(Long[] array) {
        long last = Long.MIN_VALUE;
        int len = array.length;
        for (int i = 0; i < len; i++) {
            long x = array[i];
            long x1 = x >> 32, x2 = x - (x1 << 32);
            long last1 = last >> 32, last2 = last - (last1 << 32);
            if (x1 < last1) {
                if (array.length < 1000) {
                    System.out.println(Arrays.toString(array));
                }
                throw new RuntimeException("" + x);
            } else if (x1 == last1 && x2 < last2) {
                if (array.length < 1000) {
                    System.out.println(Arrays.toString(array));
                }
                throw new RuntimeException("" + x);
            }
            last = x;
        }
    }

    private static void ordered(Long[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = (long) i;
        }
    }

    private static void stretch(Long[] array) {
        for (int i = array.length - 1; i >= 0; i--) {
            array[i] = array[i / 4];
        }
    }

    private static void reverse(Long[] array) {
        for (int i = 0; i < array.length / 2; i++) {
            long temp = array[i];
            array[i] = array[array.length - i - 1];
            array[array.length - i - 1] = temp;
        }
    }

    private static void shuffle(Long[] array) {
        Random r = new Random(1);
        for (int i = 0; i < array.length; i++) {
            long temp = array[i];
            int j = r.nextInt(array.length);
            array[j] = array[i];
            array[i] = temp;
        }
    }

    private static void stabilize(Long[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = (array[i] << 32) + i;
        }
    }

}
