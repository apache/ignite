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

package org.apache.ignite.loadtests.h2indexing;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;

/**
 * NavigableMaps PUT benchmark.
 */
public class GridTreeBenchmark {
    /** */
    private static final int PUTS = 8000000;

    /** */
    private static final int THREADS = 8;

    /** */
    private static final int ITERATIONS = PUTS / THREADS;

    /**
     * Main method.
     *
     * @param args Command line args (not used).
     * @throws BrokenBarrierException If failed.
     * @throws InterruptedException If failed.
     */
    public static void main(String... args) throws BrokenBarrierException, InterruptedException {
        doTestMaps();
    }

    /**
     * @throws BrokenBarrierException If failed.
     * @throws InterruptedException If failed.
     */
    private static void doTestAtomicInt() throws BrokenBarrierException, InterruptedException {
        final AtomicInteger[] cnts = new AtomicInteger[8];

        for (int i = 0; i < cnts.length; i++)
            cnts[i] = new AtomicInteger();

        final Thread[] ths = new Thread[THREADS];

        final CyclicBarrier barrier = new CyclicBarrier(THREADS + 1);

        final AtomicInteger cnt = new AtomicInteger();

        for (int i = 0; i < ths.length; i++) {
            ths[i] = new Thread(new Runnable() {
                @Override public void run() {
                    int idx = cnt.getAndIncrement();

                    AtomicInteger x = cnts[idx % cnts.length];

                    try {
                        barrier.await();
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }

                    for (int i = 0; i < ITERATIONS; i++)
                        x.incrementAndGet();
                }
            });

            ths[i].start();
        }

        barrier.await();

        long start = System.currentTimeMillis();

        for (Thread t : ths)
            t.join();

        long time = System.currentTimeMillis() - start;

        System.out.println(time);

    }

    /**
     * @throws BrokenBarrierException If failed.
     * @throws InterruptedException If failed.
     */
    private static void doTestMaps() throws BrokenBarrierException, InterruptedException {
        final UUID[] data = generate();

        @SuppressWarnings("unchecked")
        final Map<UUID, UUID>[] maps = new Map[4];

        for (int i = 0; i < maps.length; i++)
            maps[i] =
                new SnapTreeMap<>();


        final Thread[] ths = new Thread[THREADS];

        final CyclicBarrier barrier = new CyclicBarrier(THREADS + 1);

        final AtomicInteger cnt = new AtomicInteger();

        for (int i = 0; i < ths.length; i++) {
            ths[i] = new Thread(new Runnable() {
                @Override public void run() {
                    int idx = cnt.getAndIncrement();

                    int off = idx * ITERATIONS;

                    Map<UUID, UUID> map = maps[idx % maps.length];

                    try {
                        barrier.await();
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }

                    for (int i = 0; i < ITERATIONS; i++) {
                        UUID id = data[off + i];

                        id = map.put(id, id);

                        assert id == null;
                    }
                }
            });

            ths[i].start();
        }

        System.out.println("Sleep");
        Thread.sleep(10000);

        System.out.println("Go");
        barrier.await();

        long start = System.currentTimeMillis();

        for (Thread t : ths)
            t.join();

        long time = System.currentTimeMillis() - start;

        System.out.println(time);
    }

    /**
     * @throws BrokenBarrierException If failed.
     * @throws InterruptedException If failed.
     */
    private static void doBenchmark() throws BrokenBarrierException, InterruptedException {
        int attemts = 20;
        int warmups = 10;

        long snapTreeTime = 0;
        long skipListTime = 0;

        for (int i = 0; i < attemts; i++) {
            ConcurrentNavigableMap<UUID, UUID> skipList = new ConcurrentSkipListMap<>();
            ConcurrentNavigableMap<UUID, UUID> snapTree = new SnapTreeMap<>();

            UUID[] ids = generate();

            boolean warmup = i < warmups;

            snapTreeTime += doTest(snapTree, ids, warmup);
            skipListTime += doTest(skipList, ids, warmup);

            assert skipList.size() == snapTree.size();

            Iterator<UUID> snapIt = snapTree.keySet().iterator();
            Iterator<UUID> listIt = skipList.keySet().iterator();

            for (int x = 0, len = skipList.size(); x < len; x++)
                assert snapIt.next() == listIt.next();

            System.out.println(i + " ==================");
        }

        attemts -= warmups;

        System.out.println("Avg for GridSnapTreeMap: " + (snapTreeTime / attemts) + " ms");
        System.out.println("Avg for ConcurrentSkipListMap: " + (skipListTime / attemts) + " ms");
     }

    /**
     * @return UUIDs.
     */
    private static UUID[] generate() {
        UUID[] ids = new UUID[ITERATIONS * THREADS];

        for (int i = 0; i < ids.length; i++)
            ids[i] = UUID.randomUUID();

        return ids;
    }

    /**
     * @param tree Tree.
     * @param data Data.
     * @param warmup Warmup.
     * @return Time.
     * @throws BrokenBarrierException If failed.
     * @throws InterruptedException If failed.
     */
    private static long doTest(final ConcurrentNavigableMap<UUID, UUID> tree, final UUID[] data, boolean warmup)
        throws BrokenBarrierException, InterruptedException {
        Thread[] ths = new Thread[THREADS];

        final CyclicBarrier barrier = new CyclicBarrier(THREADS + 1);

        final AtomicInteger cnt = new AtomicInteger();

        for (int i = 0; i < ths.length; i++) {
            ths[i] = new Thread(new Runnable() {
                @Override public void run() {
                    int off = cnt.getAndIncrement() * ITERATIONS;

                    try {
                        barrier.await();
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }

                    for (int i = 0; i < ITERATIONS; i++) {
                        UUID id = data[off + i];

                        id = tree.put(id, id);

                        assert id == null;
                    }
                }
            });

            ths[i].start();
        }

        barrier.await();

        long start = System.currentTimeMillis();

        for (Thread t : ths)
            t.join();

        long time = System.currentTimeMillis() - start;

        if (!warmup) {
            System.out.println(tree.getClass().getSimpleName() + "  " + time + " ms");

            return time;
        }

        return 0;
    }
}