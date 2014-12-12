/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.loadtest.swapspace;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.swapspace.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.loadtests.util.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.atomic.*;

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
    private static final long DURATION = 10 * 60 * 1000;

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
    private SwapSpaceSpi spi() {
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

        IgniteFuture<?> evictFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
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

        IgniteFuture<?> unswapFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
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
                catch (IgniteCheckedException e) {
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
