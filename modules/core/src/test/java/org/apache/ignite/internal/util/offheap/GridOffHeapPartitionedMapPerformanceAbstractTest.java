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

package org.apache.ignite.internal.util.offheap;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;

/**
 * Performance test for partitioned offheap hash map.
 */
@SuppressWarnings({"unchecked", "NonThreadSafeLazyInitialization"})
public abstract class GridOffHeapPartitionedMapPerformanceAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int LOAD_CNT = 256;

    /** Sample keys. */
    private static T3<Integer, Integer, byte[]> keys[];

    /** Wrapped keys. */
    private static GridByteArrayWrapper[] wrappers;

    /** Unsafe map. */
    private GridOffHeapPartitionedMap map;

    /** */
    protected float load = 0.75f;

    /** */
    protected int concurrency = 16;

    /** */
    protected short lruStripes = 16;

    /** */
    protected long mem = 2L * 1024L * 1024L * 1024L;

    /** */
    protected long dur = 120 * 1000;

    /**
     *
     */
    protected GridOffHeapPartitionedMapPerformanceAbstractTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        map = newMap();

        if (keys == null) {
            keys = new T3[LOAD_CNT];
            wrappers = new GridByteArrayWrapper[LOAD_CNT];

            AffinityFunction aff = new RendezvousAffinityFunction();

            Random rnd = new Random();

            for (int i = 0; i < LOAD_CNT; i++) {
                byte[] key = new byte[rnd.nextInt(511) + 1];

                rnd.nextBytes(key);

                GridByteArrayWrapper wrap = new GridByteArrayWrapper(key);

                keys[i] = new T3<>(aff.partition(wrap), wrap.hashCode(), key);
                wrappers[i] = wrap;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (map != null)
            map.destruct();
    }

    /**
     * @return New map.
     */
    protected abstract GridOffHeapPartitionedMap newMap();

    /**
     * @throws Exception If failed.
     */
    public void testPuts() throws Exception {
        info("Warming up...");

        checkPuts(1, 20000);

        info("Warm up finished.");

        checkPuts(Runtime.getRuntime().availableProcessors(), dur);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutsConcurrentMap() throws Exception {
        info("Warming up...");

        checkPutsConcurrentMap(1, 20000);

        info("Warm up finished.");

        checkPutsConcurrentMap(Runtime.getRuntime().availableProcessors(), dur);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemoves() throws Exception {
        info("Warming up...");

        checkPutRemoves(2, 20000);

        info("Warm up finished.");

        checkPutRemoves(Runtime.getRuntime().availableProcessors(), dur);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemovesConcurrentMap() throws Exception {
        info("Warming up...");

        checkPutRemovesConcurrentMap(2, 20000);

        info("Warm up finished.");

        checkPutRemovesConcurrentMap(Runtime.getRuntime().availableProcessors(), dur);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPuts(int threadCnt, long duration) throws Exception {
        final AtomicLong opCnt = new AtomicLong();
        final AtomicLong totalOpCnt = new AtomicLong();

        final AtomicBoolean done = new AtomicBoolean();

        long start = System.currentTimeMillis();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                byte[] val = new byte[1024];

                long locTotalOpCnt = 0;

                while (!done.get()) {
                    for (int i = 0; i < 500; i++) {
                        T3<Integer, Integer, byte[]> key = randomKey(rnd);

                        map.put(key.get1(), key.get2(), key.get3(), val);
                    }

                    locTotalOpCnt += 500;
                    opCnt.addAndGet(500);
                }

                totalOpCnt.addAndGet(locTotalOpCnt);

                return null;
            }
        }, threadCnt);

        final int step = 2000;

        while (System.currentTimeMillis() - start < duration) {
            U.sleep(step);

            long ops = opCnt.getAndSet(0);

            info("Putting " + (ops * 1000) / step + " ops/sec");
        }

        done.set(true);

        fut.get();

        long end = System.currentTimeMillis();

        info("Average put performance: " + (totalOpCnt.get() * 1000) / (end - start) + " ops/sec");
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutRemoves(int threadCnt, long duration) throws Exception {
        final AtomicLong opCnt = new AtomicLong();
        final AtomicLong totalOpCnt = new AtomicLong();

        final AtomicBoolean done = new AtomicBoolean();

        long start = System.currentTimeMillis();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                byte[] val = new byte[1024];

                long locTotalOpCnt = 0;

                while (!done.get()) {
                    for (int i = 0; i < 500; i++) {
                        T3<Integer, Integer, byte[]> key = randomKey(rnd);

                        int op = rnd.nextInt(2);

                        switch (op) {
                            case 0:
                                map.put(key.get1(), key.get2(), key.get3(), val);

                                break;

                            case 1:
                                map.remove(key.get1(), key.get2(), key.get3());

                                break;

                            default:
                                assert false;
                        }
                    }

                    locTotalOpCnt += 500;
                    opCnt.addAndGet(500);
                }

                totalOpCnt.addAndGet(locTotalOpCnt);

                return null;
            }
        }, threadCnt);

        final int step = 2000;

        while (System.currentTimeMillis() - start < duration) {
            U.sleep(step);

            long ops = opCnt.getAndSet(0);

            info("Putting " + (ops * 1000) / step + " ops/sec");
        }

        done.set(true);

        fut.get();

        long end = System.currentTimeMillis();

        info("Average random operation performance: " + (totalOpCnt.get() * 1000) / (end - start) + " ops/sec");
    }
    /**
     * @throws Exception If failed.
     */
    private void checkPutsConcurrentMap(int threadCnt, long duration) throws Exception {
        final Map<GridByteArrayWrapper, byte[]> map = new ConcurrentHashMap8<>();

        final AtomicLong opCnt = new AtomicLong();
        final AtomicLong totalOpCnt = new AtomicLong();

        final AtomicBoolean done = new AtomicBoolean();

        long start = System.currentTimeMillis();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                long locTotalOpCnt = 0;

                while (!done.get()) {
                    for (int i = 0; i < 500; i++) {
                        GridByteArrayWrapper key = randomKeyWrapper(rnd);

                        map.put(key, new byte[1024]);
                    }

                    locTotalOpCnt += 500;
                    opCnt.addAndGet(500);
                }

                totalOpCnt.addAndGet(locTotalOpCnt);

                return null;
            }
        }, threadCnt);

        final int step = 2000;

        while (System.currentTimeMillis() - start < duration) {
            U.sleep(step);

            long ops = opCnt.getAndSet(0);

            info("Putting " + (ops * 1000) / step + " ops/sec");
        }

        done.set(true);

        fut.get();

        long end = System.currentTimeMillis();

        info("Average put performance: " + (totalOpCnt.get() * 1000) / (end - start) + " ops/sec");
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutRemovesConcurrentMap(int threadCnt, long duration) throws Exception {
        final Map<GridByteArrayWrapper, byte[]> map = new ConcurrentHashMap8<>();

        final AtomicLong opCnt = new AtomicLong();
        final AtomicLong totalOpCnt = new AtomicLong();

        final AtomicBoolean done = new AtomicBoolean();

        long start = System.currentTimeMillis();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                byte[] val = new byte[1024];

                long locTotalOpCnt = 0;

                while (!done.get()) {
                    for (int i = 0; i < 500; i++) {
                        GridByteArrayWrapper key = randomKeyWrapper(rnd);

                        int op = rnd.nextInt(2);

                        switch (op) {
                            case 0:
                                map.put(key, val);

                                break;

                            case 1:
                                map.remove(key);

                                break;

                            default:
                                assert false;
                        }
                    }

                    locTotalOpCnt += 500;
                    opCnt.addAndGet(500);
                }

                totalOpCnt.addAndGet(locTotalOpCnt);

                return null;
            }
        }, threadCnt);

        final int step = 2000;

        while (System.currentTimeMillis() - start < duration) {
            U.sleep(step);

            long ops = opCnt.getAndSet(0);

            info("Putting " + (ops * 1000) / step + " ops/sec");
        }

        done.set(true);

        fut.get();

        long end = System.currentTimeMillis();

        info("Average random operation performance: " + (totalOpCnt.get() * 1000) / (end - start) + " ops/sec");
    }

    /**
     * Gets random key from pregenerated array.
     *
     * @param rnd Random to use.
     * @return Tuple with key.
     */
    private T3<Integer, Integer, byte[]> randomKey(Random rnd) {
        return keys[rnd.nextInt(keys.length)];
    }

    /**
     * Gets random key from pregenerated array.
     *
     * @param rnd Random to use.
     * @return Tuple with key.
     */
    private GridByteArrayWrapper randomKeyWrapper(Random rnd) {
        return wrappers[rnd.nextInt(keys.length)];
    }
}