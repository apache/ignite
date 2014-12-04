/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Tests that removes are not lost when topology changes.
 */
public abstract class GridCacheAbstractRemoveFailureTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 3;

    /** Keys count. */
    private static final int KEYS_CNT = 10000;

    /** Test duration. */
    private static final long DUR = 90 * 1000L;

    /** Cache data assert frequency. */
    private static final long ASSERT_FREQ = 10_000;

    /** Kill delay. */
    private static final T2<Integer, Integer> KILL_DELAY = new T2<>(2000, 5000);

    /** Start delay. */
    private static final T2<Integer, Integer> START_DELAY = new T2<>(2000, 5000);

    /** Node kill lock (used to prevent killing while cache data is compared). */
    private final Lock killLock = new ReentrantLock();

    /** */
    private CountDownLatch assertLatch;

    /** */
    private CountDownLatch updateLatch;

    /** Caches comparison request flag. */
    private volatile boolean cmp;

    /** */
    private String sizePropVal;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // Need to increase value set in GridAbstractTest
        sizePropVal = System.getProperty(GG_ATOMIC_CACHE_DELETE_HISTORY_SIZE);

        System.setProperty(GG_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "100000");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.setProperty(GG_ATOMIC_CACHE_DELETE_HISTORY_SIZE, sizePropVal != null ? sizePropVal : "");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DUR + 60_000;
    }

    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setDgcFrequency(0);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAndRemove() throws Exception {
        final GridCache<Integer, Integer> sndCache0 = grid(0).cache(null);

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicLong cntr = new AtomicLong();

        final AtomicLong errCntr = new AtomicLong();

        // Expected values in cache.
        final Map<Integer, GridTuple<Integer>> expVals = new ConcurrentHashMap8<>();

        IgniteFuture<?> updateFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    for (int i = 0; i < 100; i++) {
                        int key = rnd.nextInt(KEYS_CNT);

                        boolean put = rnd.nextInt(0, 100) > 10;

                        while (true) {
                            try {
                                if (put) {
                                    sndCache0.put(key, i);

                                    expVals.put(key, F.t(i));
                                }
                                else {
                                    sndCache0.remove(key);

                                    expVals.put(key, F.<Integer>t(null));
                                }

                                break;
                            }
                            catch (GridException e) {
                                if (put)
                                    log.error("Put failed [key=" + key + ", val=" + i + ']', e);
                                else
                                    log.error("Remove failed [key=" + key + ']', e);

                                errCntr.incrementAndGet();
                            }
                        }
                    }

                    cntr.addAndGet(100);

                    if (cmp) {
                        assertLatch.countDown();

                        updateLatch.await();
                    }
                }

                return null;
            }
        });

        IgniteFuture<?> killFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    U.sleep(random(KILL_DELAY.get1(), KILL_DELAY.get2()));

                    killLock.lock();

                    try {
                        killAndRestart(stop);
                    }
                    finally {
                        killLock.unlock();
                    }
                }

                return null;
            }
        });

        try {
            long stopTime = DUR + U.currentTimeMillis() ;

            long nextAssert = U.currentTimeMillis() + ASSERT_FREQ;

            while (U.currentTimeMillis() < stopTime) {
                long start = System.nanoTime();

                long ops = cntr.longValue();

                U.sleep(1000);

                long diff = cntr.longValue() - ops;

                double time = (System.nanoTime() - start) / 1_000_000_000d;

                long opsPerSecond = (long)(diff / time);

                log.info("Operations/second: " + opsPerSecond);

                if (U.currentTimeMillis() >= nextAssert) {
                    updateLatch = new CountDownLatch(1);

                    assertLatch = new CountDownLatch(1);

                    cmp = true;

                    killLock.lock();

                    try {
                        if (!assertLatch.await(60_000, TimeUnit.MILLISECONDS))
                            throw new GridException("Failed to suspend thread executing updates.");

                        log.info("Checking cache content.");

                        assertCacheContent(expVals);

                        nextAssert = System.currentTimeMillis() + ASSERT_FREQ;
                    }
                    finally {
                        killLock.unlock();

                        updateLatch.countDown();

                        U.sleep(500);
                    }
                }
            }
        }
        finally {
            stop.set(true);
        }

        killFut.get();

        updateFut.get();

        log.info("Test finished. Update errors: " + errCntr.get());

    }

    /**
     * @param stop Stop flag.
     * @throws Exception If failed.
     */
    void killAndRestart(AtomicBoolean stop) throws Exception {
        if (stop.get())
            return;

        int idx = random(1, gridCount() + 1);

        log.info("Killing node " + idx);

        stopGrid(idx);

        U.sleep(random(START_DELAY.get1(), START_DELAY.get2()));

        if (stop.get())
            return;

        log.info("Restarting node " + idx);

        startGrid(idx);

        U.sleep(1000);
    }

    /**
     * @param expVals Expected values in cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope", "ConstantIfStatement"})
    private void assertCacheContent(Map<Integer, GridTuple<Integer>> expVals) throws Exception {
        assert !expVals.isEmpty();

        Collection<Integer> failedKeys = new HashSet<>();

        for (int i = 0; i < GRID_CNT; i++) {
            Ignite ignite = grid(i);

            GridCache<Integer, Integer> cache = ignite.cache(null);

            for (Map.Entry<Integer, GridTuple<Integer>> expVal : expVals.entrySet()) {
                Integer val = cache.get(expVal.getKey());

                if (!F.eq(expVal.getValue().get(), val)) {
                    failedKeys.add(expVal.getKey());

                    boolean primary = cache.affinity().isPrimary(ignite.cluster().localNode(), expVal.getKey());
                    boolean backup = cache.affinity().isBackup(ignite.cluster().localNode(), expVal.getKey());

                    log.error("Unexpected cache data [exp=" + expVal + ", actual=" + val + ", nodePrimary=" + primary +
                        ", nodeBackup=" + backup + ", nodeId=" + ignite.cluster().localNode().id() + ']');
                }
            }
        }

        assertTrue("Unexpected data for keys: " + failedKeys, failedKeys.isEmpty());
    }

    /**
     * @param min Min possible value.
     * @param max Max possible value (exclusive).
     * @return Random value.
     */
    private static int random(int min, int max) {
        if (max == min)
            return max;

        return ThreadLocalRandom.current().nextInt(min, max);
    }
}
