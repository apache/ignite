/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.mvstore.ConcurrentArrayList;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Test the concurrent linked list.
 */
public class TestConcurrentLinkedList extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestConcurrentLinkedList test = (TestConcurrentLinkedList) TestBase.createCaller().init();
        test.test();
        testPerformance();
    }

    @Override
    public void test() throws Exception {
        testRandomized();
        testConcurrent();
    }

    private static void testPerformance() {
        testPerformance(true);
        testPerformance(false);
        testPerformance(true);
        testPerformance(false);
        testPerformance(true);
        testPerformance(false);
    }

    private static void testPerformance(final boolean stock) {
        System.out.print(stock ? "stock " : "custom ");
        long start = System.nanoTime();
        // final ConcurrentLinkedList<Integer> test =
        //     new ConcurrentLinkedList<Integer>();
        final ConcurrentArrayList<Integer> test =
                new ConcurrentArrayList<>();
        final LinkedList<Integer> x = new LinkedList<>();
        final AtomicInteger counter = new AtomicInteger();
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    if (stock) {
                        synchronized (x) {
                            Integer y = x.peekFirst();
                            if (y == null) {
                                counter.incrementAndGet();
                            }
                        }
                    } else {
                        Integer y = test.peekFirst();
                        if (y == null) {
                            counter.incrementAndGet();
                        }
                    }
                }
            }
        };
        task.execute();
        test.add(-1);
        x.add(-1);
        for (int i = 0; i < 2000000; i++) {
            Integer value = Integer.valueOf(i & 63);
            if (stock) {
                synchronized (x) {
                    Integer f = x.peekLast();
                    if (f != value) {
                        x.add(i);
                    }
                }
                Math.sin(i);
                synchronized (x) {
                    if (x.peekFirst() != x.peekLast()) {
                        x.removeFirst();
                    }
                }
            } else {
                Integer f = test.peekLast();
                if (f != value) {
                    test.add(i);
                }
                Math.sin(i);
                f = test.peekFirst();
                if (f != test.peekLast()) {
                    test.removeFirst(f);
                }
            }
        }
        task.get();
        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    private void testConcurrent() {
        final ConcurrentArrayList<Integer> test = new ConcurrentArrayList<>();
        // final ConcurrentRing<Integer> test = new ConcurrentRing<Integer>();
        final AtomicInteger counter = new AtomicInteger();
        final AtomicInteger size = new AtomicInteger();
        Task task = new Task() {
            @Override
            public void call() {
                while (!stop) {
                    Thread.yield();
                    if (size.get() < 10) {
                        test.add(counter.getAndIncrement());
                        size.getAndIncrement();
                    }
                }
            }
        };
        task.execute();
        for (int i = 0; i < 1000000;) {
            Thread.yield();
            Integer x = test.peekFirst();
            if (x == null) {
                continue;
            }
            assertEquals(i, x.intValue());
            if (test.removeFirst(x)) {
                size.getAndDecrement();
                i++;
            }
        }
        task.get();
    }

    private void testRandomized() {
        Random r = new Random(0);
        for (int i = 0; i < 100; i++) {
            ConcurrentArrayList<Integer> test = new ConcurrentArrayList<>();
            // ConcurrentRing<Integer> test = new ConcurrentRing<Integer>();
            LinkedList<Integer> x = new LinkedList<>();
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < 10000; j++) {
                buff.append("[" + j + "] ");
                int opType = r.nextInt(3);
                switch (opType) {
                case 0: {
                    int value = r.nextInt(100);
                    buff.append("add " + value + "\n");
                    test.add(value);
                    x.add(value);
                    break;
                }
                case 1: {
                    Integer value = x.peek();
                    if (value != null && (x.size() > 5 || r.nextBoolean())) {
                        buff.append("removeFirst\n");
                        x.removeFirst();
                        test.removeFirst(value);
                    } else {
                        buff.append("removeFirst -1\n");
                        test.removeFirst(-1);
                    }
                    break;
                }
                case 2: {
                    Integer value = x.peekLast();
                    if (value != null && (x.size() > 5 || r.nextBoolean())) {
                        buff.append("removeLast\n");
                        x.removeLast();
                        test.removeLast(value);
                    } else {
                        buff.append("removeLast -1\n");
                        test.removeLast(-1);
                    }
                    break;
                }
                }
                assertEquals(toString(x.iterator()), toString(test.iterator()));
                if (x.isEmpty()) {
                    assertNull(test.peekFirst());
                    assertNull(test.peekLast());
                } else {
                    assertEquals(x.peekFirst().intValue(), test.peekFirst().intValue());
                    assertEquals(x.peekLast().intValue(), test.peekLast().intValue());
                }
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
