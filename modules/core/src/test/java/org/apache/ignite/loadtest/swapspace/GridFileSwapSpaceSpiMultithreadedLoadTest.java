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

package org.apache.ignite.loadtest.swapspace;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.loadtests.util.GridCumulativeAverage;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.swapspace.SwapContext;
import org.apache.ignite.spi.swapspace.SwapKey;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

/**
 * Runs concurrent operations on File Swap Space SPI
 * in multiple threads.
 */
public class GridFileSwapSpaceSpiMultithreadedLoadTest extends GridCommonAbstractTest {
    /** Number of threads. */
    private static final int N_THREADS = 8;

    /** Space name. */
    private static final String SPACE_NAME = "grid-mt-bm-space";

    /** Batch size. */
    private static final int BATCH_SIZE = 200;

    /** Max entries to store. */
    private static final long MAX_ENTRIES = 9000000;

    /** Test duration. */
    private static final long DURATION = 2 * 60 * 1000;

    /** Swap context. */
    private final SwapContext swapCtx = new SwapContext();

    /** SPI to test. */
    private SwapSpaceSpi spi;

    /**
     * Starts the daemon thread.
     *
     * @param runnable Thread runnable.
     */
    private static void startDaemon(Runnable runnable) {
        Thread t = new Thread(runnable);

        t.setDaemon(true);

        t.start();
    }

    /**
     * @return An SPI instance to test.
     */
    protected SwapSpaceSpi spi() {
        FileSwapSpaceSpi spi = new FileSwapSpaceSpi();

//        spi.setConcurrencyLevel(N_THREADS);
//        spi.setWriterThreadsCount(N_THREADS);

        return spi;
    }

    /**
     * @return Swap context for swap operations.
     */
    @SuppressWarnings("ConstantConditions")
    private SwapContext context() {
        return swapCtx;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        spi = spi();

        getTestResources().inject(spi);

        spi.spiStart("");

        spi.clear(SPACE_NAME);
    }

    /** @throws Exception If failed. */
    @Override protected void afterTest() throws Exception {
        spi.spiStop();
    }

    /**
     * Tests concurrent batch evict-promote.
     *
     * @throws Exception If error occurs.
     */
    public void testBatchEvictUnswap() throws Exception {
        final AtomicInteger storedEntriesCnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        startDaemon(new Runnable() {
            @SuppressWarnings("BusyWait")
            @Override public void run() {
                int curCnt = storedEntriesCnt.get();

                GridCumulativeAverage avg = new GridCumulativeAverage();

                try {
                    while (!done.get()) {
                        Thread.sleep(1000);

                        int newCnt = storedEntriesCnt.get();

                        int entPerSec = newCnt - curCnt;

                        X.println(">>> Storing " + entPerSec + " entries/second");

                        avg.update(entPerSec);

                        curCnt = newCnt;
                    }
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
                finally {
                    X.println(">>> Average store speed: " + avg + " entries/second");
                }
            }
        });

        IgniteInternalFuture<?> evictFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                    Map<SwapKey, byte[]> entries = new HashMap<>(BATCH_SIZE);

                    while (!done.get()) {
                        long l = rnd.nextLong(0, MAX_ENTRIES);

                        entries.put(new SwapKey(l), Long.toString(l).getBytes());

                        if (entries.size() == BATCH_SIZE) {
                            spi.storeAll(SPACE_NAME, entries, context());

                            storedEntriesCnt.addAndGet(BATCH_SIZE);

                            entries.clear();
                        }
                    }
                }
                catch (IgniteSpiException e) {
                    e.printStackTrace();

                    throw new IgniteException(e);
                }
            }
        }, N_THREADS, "store");

        final AtomicInteger readRmvKeys = new AtomicInteger();

        startDaemon(new Runnable() {
            @SuppressWarnings("BusyWait")
            @Override public void run() {
                int curCnt = readRmvKeys.get();

                GridCumulativeAverage avg = new GridCumulativeAverage();

                try {
                    while (!done.get()) {
                        Thread.sleep(1000);

                        int newCnt = readRmvKeys.get();

                        int entPerSec = newCnt - curCnt;

                        X.println(">>> Read-and-removed " + entPerSec + " entries/second");

                        avg.update(entPerSec);

                        curCnt = newCnt;
                    }
                }
                catch (InterruptedException ignored) {
                    //No-op.
                }
                finally {
                    X.println(">>> Average read-and-remove speed: " + avg + " entries/second");
                }
            }
        });

        IgniteInternalFuture<?> unswapFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                    Collection<SwapKey> keys = new ArrayList<>(BATCH_SIZE);

                    while (!done.get()) {
                        keys.add(new SwapKey(rnd.nextLong(0, MAX_ENTRIES)));

                        if (keys.size() == BATCH_SIZE) {
                            spi.readAll(SPACE_NAME, keys, context());

                            spi.removeAll(SPACE_NAME, keys, null, context());

                            readRmvKeys.addAndGet(BATCH_SIZE);

                            keys.clear();
                        }
                    }
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }
            }
        }, N_THREADS, "read-remove");

        Thread.sleep(DURATION);

        done.set(true);

        evictFut.get();

        unswapFut.get();
    }
}