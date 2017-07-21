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

package org.apache.ignite.loadtests.lang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.internal.util.typedef.C2;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 * Benchmark for different accessors in {@link org.jsr166.ConcurrentLinkedHashMap}.
 */
public class GridConcurrentLinkedHashMapBenchmark {
    /** Number of keys to use in benchmark. */
    private static final int KEY_RANGE = 1000;

    /** Amount of writes from total number of iterations. */
    private static final double WRITE_RATE = 0.2;

    /**
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {
        System.out.printf("%8s, %8s, %12s, %12s, %12s, %8s, %8s\n",
            "Method", "Threads", "It./s.", "It./s.*th.", "Iters.", "Time", "Writes");

        for (int i = 1; i <= 32; i*=2)
            testGet(i, WRITE_RATE);

        for (int i = 1; i <= 32; i*=2)
            testGetSafe(i, WRITE_RATE);
    }

    /**
     * Tests {@link org.jsr166.ConcurrentLinkedHashMap#getSafe(Object)} method.
     *
     * @param threadCnt Number of threads to run.
     * @param writeProportion Amount of writes from total number of iterations.
     */
    public static void testGetSafe(int threadCnt, double writeProportion) {
        test(new C2<Integer, ConcurrentLinkedHashMap<Integer, Integer>, Integer>() {
            @Override public Integer apply(Integer key, ConcurrentLinkedHashMap<Integer, Integer> map) {
                return map.getSafe(key);
            }

            @Override public String toString() {
                return "getSafe";
            }
        }, threadCnt, writeProportion);
    }

    /**
     * Tests {@link ConcurrentLinkedHashMap#get(Object)} method.
     *
     * @param threadCnt Number of threads to run.
     * @param writeProportion Amount of writes from total number of iterations.
     */
    public static void testGet(int threadCnt, double writeProportion) {
        test(new C2<Integer, ConcurrentLinkedHashMap<Integer, Integer>, Integer>() {
            @Override public Integer apply(Integer key, ConcurrentLinkedHashMap<Integer, Integer> map) {
                return map.get(key);
            }

            @Override public String toString() {
                return "get";
            }
        }, threadCnt, writeProportion);
    }

    /**
     * Test a generic access method on map.
     *
     * @param readOp Access method to test.
     * @param threadCnt Number of threads to run.
     * @param writeProportion Amount of writes from total number of iterations.
     */
    @SuppressWarnings({"BusyWait"})
    private static void test(C2<Integer, ConcurrentLinkedHashMap<Integer, Integer>, Integer> readOp, int threadCnt,
        double writeProportion) {
        assert writeProportion < 1;

        ConcurrentLinkedHashMap<Integer, Integer> map = new ConcurrentLinkedHashMap<>();

        CyclicBarrier barrier = new CyclicBarrier(threadCnt + 1);

        Collection<TestThread> threads = new ArrayList<>(threadCnt);

        for (int i = 0; i < threadCnt; i++) {
            TestThread thread = new TestThread(readOp, map, writeProportion, barrier);

            threads.add(thread);

            thread.start();
        }

        long start;

        try {
            // Wait threads warm-up.
            while (barrier.getNumberWaiting() != threadCnt)
                Thread.sleep(1);

            // Starting test and letting it run for 1 minute.
            barrier.await();

            start = System.currentTimeMillis();

            Thread.sleep(60000);
        }
        catch (InterruptedException ignored) {
            return;
        }
        catch (BrokenBarrierException e) {
            e.printStackTrace();

            return;
        }

        for (TestThread th : threads)
            th.interrupt();

        try {
            for (TestThread th : threads)
                th.join();
        }
        catch (InterruptedException ignored) {
            return;
        }

        long time = System.currentTimeMillis() - start;

        long iters = 0;

        for (TestThread th : threads)
            iters += th.iterations();

        System.out.printf("%8s, %8d, %12d, %12d, %12d, %8.3f, %8.2f\n",
            readOp.toString(), threadCnt, 1000*iters/time, 1000*iters/(time*threadCnt), iters, time/(double)1000, writeProportion);
    }

    /**
     * Test thread. Performs read/write operations on the given map
     * with the given ration.
     */
    private static class TestThread extends Thread {
        /** */
        private final C2<Integer, ConcurrentLinkedHashMap<Integer, Integer>, Integer> readOp;

        /** */
        private final ConcurrentLinkedHashMap<Integer, Integer> map;

        /** */
        private final double writeProportion;

        /** */
        private final CyclicBarrier barrier;

        /** */
        private final Random rnd = new Random();

        /** Number of passed run iterations. */
        private long iterations;

        /**
         * @param readOp Read operation to test.
         * @param map Map to test.
         * @param writeProportion Amount of writes from total number of iterations.
         * @param barrier Barrier to await before starting 'clear' run.
         */
        TestThread(final C2<Integer, ConcurrentLinkedHashMap<Integer, Integer>, Integer> readOp,
            ConcurrentLinkedHashMap<Integer, Integer> map, double writeProportion, CyclicBarrier barrier) {
            this.readOp = readOp;
            this.map = map;
            this.writeProportion = writeProportion;
            this.barrier = barrier;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            for (int i = 0; i < 1000000; i++)
                doIteration();

            try {
                barrier.await();
            }
            catch (InterruptedException ignored) {
                return;
            }
            catch (BrokenBarrierException e) {
                e.printStackTrace();

                return;
            }

            while (!interrupted()) {
                doIteration();

                iterations++;
            }
        }

        /**
         * Performs test iteration.
         */
        private void doIteration() {
            Integer key = rnd.nextInt(KEY_RANGE);

            if (rnd.nextDouble() <= writeProportion)
                map.put(key, rnd.nextInt());
            else
                readOp.apply(key, map);
        }

        /**
         * @return Number of passes iterations.
         */
        public long iterations() {
            return iterations;
        }
    }
}
