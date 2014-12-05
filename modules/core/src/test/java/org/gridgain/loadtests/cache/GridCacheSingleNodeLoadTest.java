/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.thread.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.apache.ignite.spi.collision.fifoqueue.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 */
public class GridCacheSingleNodeLoadTest {
    /** Thread count. */
    private static final int THREADS = 200;

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        start();

        try {
            runTest(200, THREADS);

            runTest(1000, THREADS);
        }
        finally {
            stop();
        }
    }

    /**
     * @param putCnt Number of puts per thread.
     * @param userThreads Number of user threads.
     * @throws Exception If failed.
     */
    private static void runTest(final int putCnt, int userThreads) throws Exception {
        final AtomicInteger keyGen = new AtomicInteger();

        final AtomicLong totalTime = new AtomicLong();

        final AtomicInteger txCntr = new AtomicInteger();

        X.println("Starting multithread test with thread count: " + userThreads);

        long start = System.currentTimeMillis();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                GridCache<Integer, Student> cache = G.grid().cache(null);

                assert cache != null;

                long startTime = System.currentTimeMillis();

                for (int i = 0; i < putCnt; i++) {
                    cache.putx(keyGen.incrementAndGet(), new Student());

                    int cnt = txCntr.incrementAndGet();

                    if (cnt % 5000 == 0)
                        X.println("Processed transactions: " + cnt);
                }

                totalTime.addAndGet(System.currentTimeMillis() - startTime);

                return null;
            }
        }, userThreads, "load-worker");

        long time = System.currentTimeMillis() - start;

        X.println("Average tx/sec: " + (txCntr.get() * 1000 / time));
        X.println("Average commit time (ms): " + (totalTime.get() / txCntr.get()));
    }

    /**
     * @throws Exception If failed.
     */
    private static void start() throws Exception {
        IgniteConfiguration c =  new IgniteConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        FifoQueueCollisionSpi cols = new FifoQueueCollisionSpi();

        cols.setParallelJobsNumber(Integer.MAX_VALUE);

        c.setCollisionSpi(cols);

        c.setExecutorService(new IgniteThreadPoolExecutor(THREADS / 2, THREADS / 2, 0L, new LinkedBlockingQueue<Runnable>()));
        c.setSystemExecutorService(new IgniteThreadPoolExecutor(THREADS * 2, THREADS * 2, 0L,
            new LinkedBlockingQueue<Runnable>()));

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setNearEvictionPolicy(new GridCacheLruEvictionPolicy(10000));
        cc.setEvictionPolicy(new GridCacheLruEvictionPolicy(300000));
        cc.setSwapEnabled(false);
        cc.setDistributionMode(PARTITIONED_ONLY);

        c.setCacheConfiguration(cc);

        G.start(c);
    }

    /**
     * Stop grid.
     */
    private static void stop() {
        G.stop(true);
    }

    /**
     * Entity class for test.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class Student {
        /** */
        private final UUID id;

        /**
         * Constructor.
         */
        Student() {
            id = UUID.randomUUID();
        }

        /**
         * @return Id.
         */
        public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Student.class, this);
        }
    }

    /**
     * Ensure singleton.
     */
    private GridCacheSingleNodeLoadTest() {
        // No-op.
    }
}
