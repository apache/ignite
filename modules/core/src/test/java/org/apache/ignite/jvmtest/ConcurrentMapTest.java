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

package org.apache.ignite.jvmtest;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ThreadLocalRandom8;

/**
 *
 */
public class ConcurrentMapTest {
    /** */
    private static Random rnd = new Random();

    /**
     *
     */
    private ConcurrentMapTest() {
        // No-op.
    }

    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String args[]) throws Exception {
        Thread.sleep(5000);

        Collection<IgnitePair<Integer>> ress = new LinkedList<>();

        for (int lvl = 16; lvl <= 16384; lvl *= 2) {
            System.gc();

            X.println("Testing map with concurrency level: " + lvl);

            int cap = 256 / lvl < 16 ? 16 * lvl : 256;

            int writes = testMap(100000, new ConcurrentHashMap8<String, Integer>(cap, 0.75f, lvl));

            ress.add(F.pair(lvl, writes));
        }

        X.println("Test summary.");

        for (IgnitePair<Integer> p : ress)
            X.println("Performance [lvl=" + p.get1() + ", writes=" + p.get2() + ']');

        testPut();

        testOpsSpeed();

        testCreationTime();
    }

    /**
     * @param keyRange Key range.
     * @param map Map.
     * @return Writes count.
     * @throws Exception If failed.
     */
    public static int testMap(final int keyRange, final ConcurrentMap<String, Integer> map) throws Exception {
        final AtomicBoolean done = new AtomicBoolean();

        final AtomicInteger writes = new AtomicInteger();

        IgniteInternalFuture fut1 = GridTestUtils.runMultiThreadedAsync(
                new Runnable() {
                    @Override public void run() {
                        while (!done.get()) {
                            map.put(rnd.nextInt(keyRange) + "very.long.string.for.key", 1);

                            writes.incrementAndGet();
                        }
                    }
                },
                40,
                "thread"
        );

        long duration = 20 * 1000;

        for (long time = 0; time < duration;) {
            Thread.sleep(5000);

            time += 5000;

            X.println(">>> Stats [duration=" + time + ", writes=" + writes.get() + ']');
        }

        done.set(true);

        fut1.get();

        X.println(">>> Test finished [duration=" + duration + ", writes=" + writes.get() + ']');

        return writes.get();
    }

    /**
     * @throws Exception If failed.
     */
    public static void testPut() throws Exception {
        Map<Integer, Integer> map = new ConcurrentHashMap8<>();

        map.put(0, 0);
        map.put(0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public static void testOpsSpeed() throws Exception {
        for (int i = 0; i < 4; i++) {
            X.println("New map ops time: " + runOps(new ConcurrentHashMap8<Integer, Integer>(), 1000000, 100));

            X.println("Jdk6 map ops time: " + runOps(new ConcurrentHashMap<Integer, Integer>(), 1000000, 100));
        }
    }

    /**
     * @param iterCnt Iterations count.
     * @param threadCnt Threads count.
     * @return Time taken.
     */
    private static long runOps(final Map<Integer,Integer> map, final int iterCnt, int threadCnt) throws Exception {
        long start = System.currentTimeMillis();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                for (int i = 0; i < iterCnt; i++) {
                    // Put random.
                    map.put(rnd.nextInt(0, 10000), 0);

                    // Read random.
                    map.get(rnd.nextInt(0, 10000));

                    // Remove random.
                    map.remove(rnd.nextInt(0, 10000));
                }

                return null;
            }
        }, threadCnt, "thread");

        return System.currentTimeMillis() - start;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public static void testCreationTime() throws Exception {
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();

            for (int j = 0; j < 1000000; j++)
                new ConcurrentHashMap8<Integer, Integer>();

            X.println("New map creation time: " + (System.currentTimeMillis() - now));

            now = System.currentTimeMillis();

            for (int j = 0; j < 1000000; j++)
                new ConcurrentHashMap<Integer, Integer>();

            X.println("Jdk6 map creation time: " + (System.currentTimeMillis() - now));
        }
    }

}