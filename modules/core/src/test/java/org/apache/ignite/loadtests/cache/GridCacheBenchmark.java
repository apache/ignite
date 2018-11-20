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

package org.apache.ignite.loadtests.cache;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridFileLock;
import org.apache.ignite.testframework.GridLoadTestUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Benchmark for cache {@code putx()} and {@code get()} operations.
 */
public class GridCacheBenchmark {
    /** Warm up time. */
    public static final long WARM_UP_TIME = Long.getLong("TEST_WARMUP_TIME", 20000);

    /** Number of puts. */
    private static final long PUT_CNT = Integer.getInteger("TEST_PUT_COUNT", 3000000);

    /** Thread count. */
    private static final int THREADS = Integer.getInteger("TEST_THREAD_COUNT", 16);

    /** Test write or read operations. */
    private static boolean testWrite = Boolean.getBoolean("TEST_WRITE");

    /** Cache name. */
    private static final String CACHE = "partitioned";

    /** Counter. */
    private static final AtomicLong cntr = new AtomicLong();

    /** */
    private static final int LOG_MOD = 500000;

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public static void main(String[] args) throws Exception {
        GridFileLock fileLock = GridLoadTestUtils.fileLock();

        fileLock.lock();

        try {
            final String outputFileName = args.length > 0 ? args[0] : null;

            // try (Grid g = G.start("modules/core/src/test/config/load/cache-client-benchmark.xml")) {
            try (Ignite g = G.start("modules/core/src/test/config/load/cache-benchmark.xml")) {
                X.println("warmupTime=" + WARM_UP_TIME);
                X.println("putCnt=" + PUT_CNT);
                X.println("threadCnt=" + THREADS);
                X.println("testWrite=" + testWrite);

                final IgniteCache<Long, Long> cache = g.cache(CACHE);

                assert cache != null;

                cntr.set(0);

                final AtomicLong opCnt = new AtomicLong();

                X.println("Warming up (putx)...");

                GridLoadTestUtils.runMultithreadedInLoop(new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        long keyVal = cntr.incrementAndGet();

                        cache.put(keyVal % 100000, keyVal);

                        long ops = opCnt.incrementAndGet();

                        if (ops % LOG_MOD == 0)
                            X.println(">>> Performed " + ops + " operations.");

                        return null;
                    }
                }, THREADS, WARM_UP_TIME);

                cntr.set(0);

                opCnt.set(0);

                X.println("Warming up (get)...");

                GridLoadTestUtils.runMultithreadedInLoop(new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        long keyVal = cntr.incrementAndGet();

                        Long old = cache.get(keyVal % 100000);

                        long ops = opCnt.incrementAndGet();

                        if (ops % LOG_MOD == 0)
                            X.println(">>> Performed " + ops + " operations, old=" + old + ", keyval=" + keyVal);

                        return null;
                    }
                }, THREADS, WARM_UP_TIME);

                cache.clear();

                System.gc();

                cntr.set(0);

                opCnt.set(0);

                X.println("Starting Ignite cache putx() benchmark...");

                long durPutx = GridLoadTestUtils.measureTime(new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        while (true) {
                            long keyVal = cntr.incrementAndGet();

                            if (keyVal >= PUT_CNT)
                                break;

                            cache.put(keyVal % 100000, keyVal);

                            long ops = opCnt.incrementAndGet();

                            if (ops % LOG_MOD == 0)
                                X.println(">>> Performed " + ops + " operations.");
                        }

                        return null;
                    }
                }, THREADS);

                X.println(">>>");
                X.println(">> Ignite cache putx() benchmark results [duration=" + durPutx + " ms, tx/sec=" +
                    (opCnt.get() * 1000 / durPutx) + ", total=" + opCnt.get() + ']');
                X.println(">>>");

                System.gc();

                cntr.set(0);

                opCnt.set(0);

                X.println("Starting Ignite cache get() benchmark...");

                long durGet = GridLoadTestUtils.measureTime(new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        while (true) {
                            long keyVal = cntr.incrementAndGet();

                            if (keyVal >= PUT_CNT)
                                break;

                            Long old = cache.get(keyVal % 100000);

                            long ops = opCnt.incrementAndGet();

                            if (ops % LOG_MOD == 0)
                                X.println(">>> Performed " + ops + " operations, old=" + old + ", keyval=" + keyVal);
                        }

                        return null;
                    }
                }, THREADS);

                X.println(">>>");
                X.println(">> Ignite cache get() benchmark results [duration=" + durGet + " ms, tx/sec=" +
                    (opCnt.get() * 1000 / durGet) + ", total=" + opCnt.get() + ']');
                X.println(">>>");

                if (outputFileName != null)
                    GridLoadTestUtils.appendLineToFile(
                        outputFileName,
                        "%s,%d,%d",
                        GridLoadTestUtils.DATE_TIME_FORMAT.format(new Date()),
                        durPutx,
                        durGet);
            }
        }
        finally {
            fileLock.close();
        }
    }
}