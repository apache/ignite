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

package org.gridgain.loadtests.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.IgniteTxIsolation.REPEATABLE_READ;

/**
 * Performance comparison between putAll and group lock.
 *
 */
public class GridCacheGroupLockComparisonTest {
    /** Batch size. */
    private static final int BATCH_SIZE = Integer.getInteger("TEST_BATCH_SIZE", 25000);

    /** Thread count. */
    private static final int THREADS = Integer.getInteger("TEST_THREAD_COUNT", 16);

    /** Cache name. */
    private static final String CACHE = "partitioned";

    /** Total number of objects in cache. */
    private static final long OBJECT_CNT = Integer.getInteger("TEST_OBJECT_COUNT", 2000000);

    /** Counter. */
    private static final AtomicLong cntr = new AtomicLong();

    /** */
    private static final int LOG_MOD = 50000;

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite g = G.start("modules/tests/config/load/cache-benchmark.xml")) {
            System.out.println("threadCnt=" + THREADS);
            System.out.println("objectCnt=" + OBJECT_CNT);
            System.out.println("batchSize=" + BATCH_SIZE);

            // Populate and warm-up.
            gridGainGroupLock(g, OBJECT_CNT, THREADS);

            gridGainGroupLock(g, OBJECT_CNT, THREADS);
        }
    }

    /**
     * @param ignite Grid.
     * @param max Maximum cache size.
     * @param threads Threads.
     * @throws Exception If failed.
     */
    private static void gridGainPutAll(Ignite ignite, final long max, int threads) throws Exception {
        X.println(">>>");
        X.println(">>> Testing putAll");
        X.println(">>>");

        final GridCache<GridCacheAffinityKey<Long>, Long> cache = ignite.cache(CACHE);

        assert cache != null;

        final AtomicLong opCnt = new AtomicLong();

        cntr.set(0);

        final long start = System.currentTimeMillis();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                while (true) {
                    Map<GridCacheAffinityKey<Long>, Long> vals =
                        new HashMap<>(BATCH_SIZE);

                    long start = cntr.getAndAdd(BATCH_SIZE);

                    if (start >= max)
                        break;

                    for (long i = start; i < start + BATCH_SIZE; i++)
                        vals.put(new GridCacheAffinityKey<>(i % 100000, start), i);

                    cache.putAll(vals);

                    long ops = opCnt.addAndGet(BATCH_SIZE);

                    if (ops % LOG_MOD == 0)
                        X.println(">>> Performed " + ops + " operations.");
                }

                return null;
            }
        }, threads, "load-worker");

        long dur = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">> putAll timed results [dur=" + dur + " ms, tx/sec=" + (opCnt.get() * 1000 / dur) +
            ", total=" + opCnt.get() + ", duration=" + (dur + 500) / 1000 + "s]");
        X.println(">>>");
    }

    /**
     * @param ignite Grid.
     * @param max Maximum cache size.
     * @param threads Threads.
     * @throws Exception If failed.
     */
    private static void gridGainGroupLock(Ignite ignite, final long max, int threads) throws Exception {
        X.println(">>>");
        X.println(">>> Testing group lock");
        X.println(">>>");

        final GridCache<GridCacheAffinityKey<Long>, Long> cache = ignite.cache(CACHE);

        assert cache != null;

        final AtomicLong opCnt = new AtomicLong();

        cntr.set(0);

        final AtomicInteger range = new AtomicInteger();

        final long start = System.currentTimeMillis();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                int affIdx = range.getAndIncrement();

                String affKey = Thread.currentThread().getName();

                long rangeCnt = OBJECT_CNT / THREADS;

                long base = affIdx * rangeCnt;

                X.println("Going to put vals in range [" + base + ", " + (base + rangeCnt - 1) + ']');

                long key = 0;

                while (true) {
                    long total = cntr.getAndAdd(BATCH_SIZE);

                    if (total >= max)
                        break;

                    // Threads should not lock the same key.

                    try (IgniteTx tx = cache.txStartAffinity(affKey, PESSIMISTIC, REPEATABLE_READ, 0, BATCH_SIZE)) {
                        for (long i = 0; i < BATCH_SIZE; i++) {
                            cache.put(new GridCacheAffinityKey<>((key % rangeCnt) + base, affKey), i);

                            key++;
                        }

                        tx.commit();
                    }

                    long ops = opCnt.addAndGet(BATCH_SIZE);

                    if (ops % LOG_MOD == 0)
                        X.println(">>> Performed " + ops + " operations.");
                }

                return null;
            }
        }, threads, "load-worker");

        long dur = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Cache size: " + cache.size());
        X.println(">>> Group lock timed results [dur=" + dur + " ms, tx/sec=" + (opCnt.get() * 1000 / dur) +
            ", total=" + opCnt.get() + ", duration=" + (dur + 500) / 1000 + "s]");
        X.println(">>>");
    }
}
