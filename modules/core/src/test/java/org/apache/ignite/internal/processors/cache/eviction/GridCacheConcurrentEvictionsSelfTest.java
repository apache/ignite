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

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class GridCacheConcurrentEvictionsSelfTest extends GridCommonAbstractTest {
    /** Replicated cache. */
    private CacheMode mode = REPLICATED;

    /** */
    private EvictionPolicy<?, ?> plc;

    /** */
    private int warmUpPutsCnt;

    /** */
    private int iterCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionConfiguration().setDefaultTxIsolation(REPEATABLE_READ);

        CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setNearConfiguration(null);

        cc.setEvictionPolicy(plc);
        cc.setOnheapCacheEnabled(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        plc = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        super.beforeTestsStarted();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutsFifoLocal() throws Exception {
        mode = LOCAL;

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(1000);

        this.plc = plc;
        warmUpPutsCnt = SF.applyLB(100_000, 10_000);
        iterCnt = SF.applyLB(100_000, 10_000);

        checkConcurrentPuts();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutsLruLocal() throws Exception {
        mode = LOCAL;

        LruEvictionPolicy plc = new LruEvictionPolicy();
        plc.setMaxSize(1000);

        this.plc = plc;
        warmUpPutsCnt = SF.applyLB(100_000, 10_000);
        iterCnt = SF.applyLB(100_000, 10_000);

        checkConcurrentPuts();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutsSortedLocal() throws Exception {
        mode = LOCAL;

        SortedEvictionPolicy plc = new SortedEvictionPolicy();
        plc.setMaxSize(1000);

        this.plc = plc;
        warmUpPutsCnt = SF.applyLB(100_000, 10_000);
        iterCnt = SF.applyLB(100_000, 10_000);

        checkConcurrentPuts();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkConcurrentPuts() throws Exception {
        try {
            Ignite ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

            // Warm up.
            for (int i = 0; i < warmUpPutsCnt; i++) {
                cache.put(i, i);

                if (i != 0 && i % 1000 == 0)
                    info("Warm up puts count: " + i);
            }

            info("Cache size: " + cache.size());

            cache.removeAll();

            final AtomicInteger idx = new AtomicInteger();

            int threadCnt = SF.applyLB(30, 8);

            long start = System.currentTimeMillis();

            IgniteInternalFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() {
                        for (int i = 0; i < iterCnt; i++) {
                            int j = idx.incrementAndGet();

                            cache.put(j, j);

                            if (i != 0 && i % 1000 == 0)
                                // info("Puts count: " + i);
                                info("Stats [putsCnt=" + i + ", size=" + cache.size(CachePeekMode.ONHEAP) + ']');
                        }

                        return null;
                    }
                },
                threadCnt
            );

            fut.get();

            info("Test results [threadCnt=" + threadCnt + ", iterCnt=" + iterCnt + ", cacheSize=" + cache.size(CachePeekMode.ONHEAP) +
                ", duration=" + (System.currentTimeMillis() - start) + ']');

            assertTrue(cache.size(CachePeekMode.ONHEAP) <= 1000);
        }
        finally {
            stopAllGrids();
        }
    }
}
