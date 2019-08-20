/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.dev.util.ImmutableArray2;
import org.h2.test.TestBase;

/**
 * Test the concurrent linked list.
 */
public class TestImmutableArray extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestImmutableArray test = (TestImmutableArray) TestBase.createCaller().init();
        test.test();
        testPerformance();
    }

    @Override
    public void test() throws Exception {
        testRandomized();
    }

    private static void testPerformance() {
        testPerformance(true);
        testPerformance(false);
        testPerformance(true);
        testPerformance(false);
        testPerformance(true);
        testPerformance(false);
    }

    private static void testPerformance(final boolean immutable) {
//        immutable time 2068 dummy: 60000000
//        immutable time 1140 dummy: 60000000
//        ArrayList time 361 dummy: 60000000

        System.out.print(immutable ? "immutable" : "ArrayList");
        long start = System.nanoTime();
        int count = 20000000;
        Integer x = 1;
        int sum = 0;
        if (immutable) {
            ImmutableArray2<Integer> test = ImmutableArray2.empty();
            for (int i = 0; i < count; i++) {
                if (i % 10 != 0) {
                    test = test.insert(test.length(), x);
                } else {
                    test = test.insert(i % 30, x);
                }
                if (test.length() > 100) {
                    while (test.length() > 30) {
                        if (i % 10 != 0) {
                            test = test.remove(test.length() - 1);
                        } else {
                            test = test.remove(0);
                        }
                    }
                }
                sum += test.get(0);
                sum += test.get(test.length() - 1);
                sum += test.get(test.length() / 2);
                if (i % 10 == 0) {
                    test = test.set(0, x);
                }
            }
        } else {
            ArrayList<Integer> test = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (i % 10 != 0) {
                    test.add(test.size(), x);
                } else {
                    test.add(i % 30, x);
                }
                if (test.size() > 100) {
                    while (test.size() > 30) {
                        if (i % 2 == 0) {
                            test.remove(test.size() - 1);
                        } else {
                            test.remove(0);
                        }
                    }
                }
                sum += test.get(0);
                sum += test.get(test.size() - 1);
                sum += test.get(test.size() / 2);
                if (i % 10 == 0) {
                    test.set(0, x);
                }
            }
        }
        System.out.println(" time " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) +
                " dummy: " + sum);
    }

    private void testRandomized() {
        Random r = new Random(0);
        for (int i = 0; i < 100; i++) {
            ImmutableArray2<Integer> test = ImmutableArray2.empty();
            // ConcurrentRing<Integer> test = new ConcurrentRing<Integer>();
            ArrayList<Integer> x = new ArrayList<>();
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < 1000; j++) {
                buff.append("[" + j + "] ");
                int opType = r.nextInt(3);
                switch (opType) {
                case 0: {
                    int index = test.length() == 0 ? 0 : r.nextInt(test.length());
                    int value = r.nextInt(100);
                    buff.append("insert " + index + " " + value + "\n");
                    test = test.insert(index, value);
                    x.add(index, value);
                    break;
                }
                case 1: {
                    if (test.length() > 0) {
                        int index = r.nextInt(test.length());
                        int value = r.nextInt(100);
                        buff.append("set " + index + " " + value + "\n");
                        x.set(index, value);
                        test = test.set(index, value);
                    }
                    break;
                }
                case 2: {
                    if (test.length() > 0) {
                        int index = r.nextInt(test.length());
                        buff.append("remove " + index + "\n");
                        x.remove(index);
                        test = test.remove(index);
                    }
                    break;
                }
                }
                assertEquals(x.size(), test.length());
                assertEquals(toString(x.iterator()), toString(test.iterator()));
            }
        }
    }

    private static <T> String toString(Iterator<T> it) {
        StringBuilder buff = new StringBuilder();
        while (it.hasNext()) {
            buff.append(' ').append(it.next());
        }
        return buff.toString();
    }

}
