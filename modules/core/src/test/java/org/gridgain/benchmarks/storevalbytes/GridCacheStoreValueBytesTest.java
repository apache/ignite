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

package org.gridgain.benchmarks.storevalbytes;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.loadtests.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.benchmarks.storevalbytes.GridCacheStoreValueBytesNode.*;

/**
 *
 */
public class GridCacheStoreValueBytesTest {
    /** */
    static final int KEYS_NUM = 10000;

    /** */
    static final Integer[] KEYS = new Integer[KEYS_NUM];

    /** */
    static final int DFL_MIN_VAL_SIZE = 512;

    /** */
    static final int DFL_MAX_VAL_SIZE = 1024;

    static {
        for (int i = 0; i < KEYS_NUM; i++)
            KEYS[i] = i;
    }

    /** */
    private static final int DFLT_THREADS_NUM = 2;

    /** */
    private static final boolean DFLT_RANDOM_GET = false;

    /** */
    private static final int DFLT_DURATION_MIN = 3;

    /** */
    private static final int UPDATE_INTERVAL_SEC = 10;

    /** */
    private static final int DFLT_WARMUP_TIME_SEC = 10;

    /** */
    private static final int DFLT_CONCURRENT_GET_NUM = 5000;

    /** */
    private static final int DFLT_GET_KEY_NUM = 20;

    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        boolean randomGet = DFLT_RANDOM_GET;

        int duration = DFLT_DURATION_MIN;

        boolean put = false;

        int warmup = DFLT_WARMUP_TIME_SEC;

        int concurrentGetNum = DFLT_CONCURRENT_GET_NUM;

        int threadsNum = DFLT_THREADS_NUM;

        int getKeyNum = DFLT_GET_KEY_NUM;

        int minSize = DFL_MIN_VAL_SIZE;

        int maxSize = DFL_MAX_VAL_SIZE;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "-c":
                    concurrentGetNum = Integer.parseInt(args[++i]);

                    break;

                case "-t":
                    threadsNum = Integer.parseInt(args[++i]);

                    break;

                case "-k":
                    getKeyNum = Integer.parseInt(args[++i]);

                    break;

                case "-randomGet":
                    randomGet = Boolean.parseBoolean(args[++i]);

                    break;

                case "-d":
                    duration = Integer.parseInt(args[++i]);

                    break;

                case "-w":
                    warmup = Integer.parseInt(args[++i]);

                    break;

                case "-put":
                    put = Boolean.parseBoolean(args[++i]);

                    break;

                case "-min":
                    minSize = Integer.parseInt(args[++i]);

                    break;

                case "-max":
                    maxSize = Integer.parseInt(args[++i]);

                    break;
            }
        }

        X.println("Duration: " + duration + " minutes");
        X.println("Warmup time: " + warmup + " seconds");
        X.println("Threads number: " + threadsNum);
        X.println("Concurrent get number: " + concurrentGetNum);
        X.println("Get keys number: " + getKeyNum);
        X.println("Random get: " + randomGet);

        Ignite ignite = Ignition.start(GridCacheStoreValueBytesNode.parseConfiguration(args, true));

        if (put) {
            X.println("Putting data in cache...");
            X.println("Min value size: " + minSize);
            X.println("Max value size: " + maxSize);

            Random random = new Random(1);

            int sizeRange = maxSize - minSize;

            GridCache<Integer, String> cache = ignite.cache(null);

            if (sizeRange == 0) {
                for (Integer key : KEYS)
                    cache.put(key, createValue(minSize));
            }
            else {
                for (Integer key : KEYS)
                    cache.put(key, createValue(minSize + random.nextInt(sizeRange)));
            }
        }

        try {
            runTest(ignite, concurrentGetNum, threadsNum, getKeyNum, duration * 60000, warmup * 1000, randomGet);
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * @param exec Pool.
     * @param ignite Grid.
     * @param concurrentGetNum Concurrent GET operations.
     * @param threadsNum Thread count.
     * @param getKeyNum Keys count.
     * @param finish Finish flag.
     * @param cntr Counter.
     * @param randomGet {@code True} to get random keys.
     * @return Futures.
     */
    static Collection<Future<?>> startThreads(ExecutorService exec, final Ignite ignite, int concurrentGetNum,
        int threadsNum, final int getKeyNum, final AtomicBoolean finish, final AtomicLong cntr,
        final boolean randomGet) {

        final Semaphore sem = new Semaphore(concurrentGetNum);

        final IgniteInClosure<IgniteFuture> lsnr = new CI1<IgniteFuture>() {
            @Override public void apply(IgniteFuture t) {
                sem.release();
            }
        };

        finish.set(false);

        cntr.set(0);

        Collection<Future<?>> futs = new ArrayList<>(threadsNum);

        for (int i = 0; i < threadsNum; i++) {
            futs.add(exec.submit(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    GridCache<Integer, String> cache = ignite.cache(null);

                    Random random = new Random();

                    while (!finish.get()) {
                        Collection<Integer> keys = new ArrayList<>(getKeyNum);

                        for (int i = 0; i < KEYS_NUM; i++) {
                            Integer key = KEYS[randomGet ? random.nextInt(KEYS_NUM) : i];

                            keys.add(key);

                            if (keys.size() == getKeyNum) {
                                sem.acquire();

                                IgniteFuture<Map<Integer, String>> f = cache.getAllAsync(keys);

                                f.listenAsync(lsnr);

                                cntr.incrementAndGet();

                                keys.clear();
                            }
                        }
                    }

                    return null;
                }
            }));
        }

        return futs;
    }

    /**
     * @param ignite Grid.
     * @param concurrentGetNum Number of concurrent getAllAsync operations.
     * @param threadsNum Thread count.
     * @param getKeyNum Keys count.
     * @param duration Test duration.
     * @param warmup Warmup duration.
     * @param randomGet If {@code true} then selects keys randomly, otherwise selects keys sequentially.
     * @throws Exception If failed.
     */
    static void runTest(final Ignite ignite, int concurrentGetNum, int threadsNum, int getKeyNum, final long duration,
        long warmup, final boolean randomGet) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(threadsNum);

        try {
            final AtomicBoolean finish = new AtomicBoolean();

            final AtomicLong cntr = new AtomicLong();

            X.println("Warming up...");

            Collection<Future<?>> futs = startThreads(exec, ignite, concurrentGetNum, threadsNum, getKeyNum, finish,
                cntr, randomGet);

            U.sleep(warmup);

            finish.set(true);

            boolean failed = false;

            for (Future<?> fut : futs) {
                try {
                    fut.get();
                }
                catch (ExecutionException e) {
                    X.error("Error during warmup: " + e);

                    e.getCause().printStackTrace();

                    failed = true;
                }
            }

            if (failed)
                return;

            X.println("Running test...");

            futs = startThreads(exec, ignite, concurrentGetNum, threadsNum, getKeyNum, finish, cntr, randomGet);

            long end = System.currentTimeMillis() + duration;

            GridCumulativeAverage avgGetPerSec = new GridCumulativeAverage();

            while (System.currentTimeMillis() < end) {
                long c1 = cntr.get();

                U.sleep(UPDATE_INTERVAL_SEC * 1000);

                long c2 = cntr.get();

                long getPerSec = (c2 - c1) / UPDATE_INTERVAL_SEC;

                X.println(">>> Gets/s: " + getPerSec);

                avgGetPerSec.update(getPerSec);
            }

            finish.set(true);

            for (Future<?> fut : futs) {
                try {
                    fut.get();
                }
                catch (ExecutionException e) {
                    X.error("Error during execution: " + e);

                    e.getCause().printStackTrace();
                }
            }

            X.println(">>> Average gets/s: " + avgGetPerSec);
        }
        finally {
            exec.shutdown();
        }
    }
}
