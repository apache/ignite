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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.noop.NoopSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 * Test for cache swap.
 */
public class GridCacheOffHeapTest extends GridCommonAbstractTest {
    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private CacheMode mode;

    /** */
    private int onheap;

    /** Start size. */
    private int startSize = 4 * 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSwapSpaceSpi(new NoopSwapSpaceSpi());

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_ASYNC);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setCacheMode(mode);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setStartSize(startSize);

        if (onheap > 0) {
            FifoEvictionPolicy plc = new FifoEvictionPolicy();
            plc.setMaxSize(onheap);

            cacheCfg.setEvictionPolicy(plc);

            cacheCfg.setOffHeapMaxMemory(80 * 1024L * 1024L * 1024L); // 10GB
        }

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnHeapReplicatedPerformance() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-817");
        
        mode = REPLICATED;
        onheap = 0;
        startSize = 18 * 1024 * 1024;

        performanceTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnHeapPartitionedPerformance() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-817");
        
        mode = PARTITIONED;
        onheap = 0;
        startSize = 18 * 1024 * 1024;

        performanceTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapReplicatedPerformance() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-817");
        
        mode = REPLICATED;
        onheap = 1024 * 1024;
        startSize = onheap;

        performanceTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapPartitionedPerformance() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-817");
        
        mode = PARTITIONED;
        onheap = 4 * 1024 * 1024;

        performanceTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnHeapReplicatedPerformanceMultithreaded() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-817");
        
        mode = REPLICATED;
        onheap = 0;
        startSize = 18 * 1024 * 1024;

        performanceMultithreadedTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnHeapPartitionedPerformanceMultithreaded() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-817");
        
        mode = PARTITIONED;
        onheap = 0;
        startSize = 18 * 1024 * 1024;

        performanceMultithreadedTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapReplicatedPerformanceMultithreaded() throws Exception {
        mode = REPLICATED;
        onheap = 1024 * 1024;
        startSize = onheap;

        performanceMultithreadedTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapPartitionedPerformanceMultithreaded() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-817");
        
        mode = PARTITIONED;
        onheap = 4 * 1024 * 1024;

        performanceMultithreadedTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void performanceTest() throws Exception {
        Ignite g = startGrid();

        try {
            GridCacheAdapter<Integer, Integer> cache = ((IgniteKernal)g).internalCache(null);

//            int max = 17 * 1024 * 1024;
            int max = Integer.MAX_VALUE;

            long start = System.currentTimeMillis();

            for (int i = 0; i < max; i++) {
                cache.getAndPut(i, i);

                if (i % 100000 == 0) {
                    long cur = System.currentTimeMillis();

                    info("Stats [i=" + i + ", time=" + (cur - start) + ", throughput=" + (i * 1000d / (cur - start)) +
                        "ops/sec, onheapCnt=" + cache.size() + ", offheapCnt=" + cache.offHeapEntriesCount() + "]");
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void performanceMultithreadedTest() throws Exception {
        Ignite g = startGrid();

        try {
            final GridCacheAdapter<Integer, Integer> c = ((IgniteKernal)g).internalCache(null);

            final long start = System.currentTimeMillis();

            final AtomicInteger keyGen = new AtomicInteger();

            final int reserveSize = 1024 * 1024;

            multithreaded(new Callable<Object>() {
                @SuppressWarnings("InfiniteLoopStatement")
                @Override public Object call() throws Exception {
                    while (true) {
                        int val = keyGen.addAndGet(reserveSize); // Reserve keys.

                        for (int i = val - reserveSize; i < val; i++) {
                            c.getAndPut(i, i);

                            if (i % 500000 == 0) {
                                long dur = System.currentTimeMillis() - start;
                                long keySize= c.size() + c.offHeapEntriesCount();

                                info("Stats [size=" + keySize + ", time=" + dur + ", throughput=" +
                                    (keySize * 1000f / dur) + " ops/sec, onheapCnt=" + c.size() +
                                    ", offheapCnt=" + c.offHeapEntriesCount() + "]");
                            }
                        }
                    }
                }
            }, Runtime.getRuntime().availableProcessors());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Main method.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
//    public static void main(String[] args) throws Exception {
//        new GridCacheOffHeapTest().testOffHeapReplicatedPerformanceMultithreaded();
//    }
}