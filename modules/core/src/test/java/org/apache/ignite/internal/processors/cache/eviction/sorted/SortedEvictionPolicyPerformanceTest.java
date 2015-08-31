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

package org.apache.ignite.internal.processors.cache.eviction.sorted;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.LongAdder8;
import org.jsr166.ThreadLocalRandom8;

/**
 * {@link SortedEvictionPolicy} performance test.
 */
public class SortedEvictionPolicyPerformanceTest extends GridCommonAbstractTest {
    /** Threads. */
    private static final int THREADS = 8;

    /** Keys. */
    private static final int KEYS = 100_000;

    /** Max size. */
    private static final int MAX_SIZE = 1000;

    /** Put probability. */
    private static final int P_PUT = 50;

    /** Get probability. */
    private static final int P_GET = 30;

    /** Rnd. */
    private static final ThreadLocalRandom8 RND = ThreadLocalRandom8.current();

    /** Ignite. */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setNearConfiguration(null);

        SortedEvictionPolicy plc = new SortedEvictionPolicy();
        plc.setMaxSize(MAX_SIZE);

        ccfg.setEvictionPolicy(plc);
        ccfg.setEvictSynchronized(false);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Tests throughput.
     */
    public void testThroughput() throws Exception {
        final LongAdder8 cnt = new LongAdder8();
        final AtomicBoolean finished = new AtomicBoolean();

        final int pPut = P_PUT;
        final int pGet = P_PUT + P_GET;

        final IgniteCache<Integer, Integer> cache = ignite.cache(null);

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (;;) {
                    U.sleep(1000);

                    info("Ops/sec: " + cnt.sumThenReset());
                }
            }
        }, 1);

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get()) {
                        int p = RND.nextInt(100);

                        int key = RND.nextInt(KEYS);

                        if (p >= 0 && p < pPut)
                            cache.put(key, 0);
                        else if (p >= pPut && p < pGet)
                            cache.get(key);
                        else
                            cache.remove(key);

                        cnt.increment();
                    }

                    return null;
                }
            }, THREADS);
    }
}