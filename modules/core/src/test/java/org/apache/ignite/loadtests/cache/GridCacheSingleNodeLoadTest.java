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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.thread.*;
import org.apache.ignite.spi.collision.fifoqueue.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;

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
                GridCache<Integer, Student> cache = G.ignite().cache(null);

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

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        FifoQueueCollisionSpi cols = new FifoQueueCollisionSpi();

        cols.setParallelJobsNumber(Integer.MAX_VALUE);

        c.setCollisionSpi(cols);

        c.setExecutorService(new IgniteThreadPoolExecutor(THREADS / 2, THREADS / 2, 0L, new LinkedBlockingQueue<Runnable>()));
        c.setSystemExecutorService(new IgniteThreadPoolExecutor(THREADS * 2, THREADS * 2, 0L,
            new LinkedBlockingQueue<Runnable>()));

        CacheConfiguration cc = new CacheConfiguration();

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
