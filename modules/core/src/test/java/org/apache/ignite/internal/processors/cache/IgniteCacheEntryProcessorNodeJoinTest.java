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

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests cache in-place modification logic with iterative value increment.
 */
public class IgniteCacheEntryProcessorNodeJoinTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of nodes to test on. */
    private static final int GRID_CNT = 2;

    /** Number of increment iterations. */
    private static final int INCREMENTS = 100;

    /** */
    private static final int KEYS = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cache = new CacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setBackups(1);
        cache.setRebalanceMode(SYNC);

        return cache;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleEntryProcessorNodeJoin() throws Exception {
        checkEntryProcessorNodeJoin(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllEntryProcessorNodeJoin() throws Exception {
        checkEntryProcessorNodeJoin(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntryProcessorNodeLeave() throws Exception {
        startGrid(GRID_CNT);

        // TODO: IGNITE-1525 (test fails with one-phase commit).
        boolean createCache = atomicityMode() == TRANSACTIONAL;

        String cacheName = null;

        if (createCache) {
            CacheConfiguration ccfg = cacheConfiguration();

            ccfg.setName("cache-2");
            ccfg.setBackups(2);

            ignite(0).createCache(ccfg);

            cacheName = ccfg.getName();
        }

        try {
            int NODES = GRID_CNT + 1;

            final int RESTART_IDX = GRID_CNT + 1;

            for (int iter = 0; iter < 10; iter++) {
                log.info("Iteration: " + iter);

                startGrid(RESTART_IDX);

                awaitPartitionMapExchange();

                final CountDownLatch latch = new CountDownLatch(1);

                IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        latch.await();

                        stopGrid(RESTART_IDX);

                        return null;
                    }
                }, "stop-thread");

                int increments = checkIncrement(cacheName, iter % 2 == 2, fut, latch);

                assert increments >= INCREMENTS;

                fut.get();

                for (int i = 0; i < KEYS; i++) {
                    for (int g = 0; g < NODES; g++) {
                        Set<String> vals = ignite(g).<String, Set<String>>cache(cacheName).get("set-" + i);

                        assertNotNull(vals);
                        assertEquals(increments, vals.size());
                    }
                }

                ignite(0).cache(cacheName).removeAll();
            }
        }
        finally {
            if (createCache)
                ignite(0).destroyCache(cacheName);
        }
    }

    /**
     * @param invokeAll If {@code true} tests invokeAll operation.
     * @throws Exception If failed.
     */
    private void checkEntryProcessorNodeJoin(boolean invokeAll) throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final int started = 6;

        try {
            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    try {
                        for (int i = 0; i < started; i++) {
                            U.sleep(1_000);

                            startGrid(GRID_CNT + i);
                        }
                    }
                    catch (Exception e) {
                        error.compareAndSet(null, e);
                    }
                }
            }, 1, "starter");

            try {
                checkIncrement(null, invokeAll, null, null);
            }
            finally {
                stop.set(true);

                fut.get(getTestTimeout());
            }

            for (int i = 0; i < KEYS; i++) {
                for (int g = 0; g < GRID_CNT + started; g++) {
                    Set<String> vals = ignite(g).<String, Set<String>>cache(null).get("set-" + i);

                    assertNotNull(vals);
                    assertEquals(INCREMENTS, vals.size());
                }
            }
        }
        finally {
            for (int i = 0; i < started; i++)
                stopGrid(GRID_CNT + i);
        }
    }

    /**
     * @param cacheName Cache name.
     * @param invokeAll If {@code true} tests invokeAll operation.
     * @param fut If not null then executes updates while future is not done.
     * @param latch Latch to count down when first update is done.
     * @throws Exception If failed.
     * @return Number of increments.
     */
    private int checkIncrement(
        String cacheName,
        boolean invokeAll,
        @Nullable IgniteInternalFuture<?> fut,
        @Nullable CountDownLatch latch) throws Exception {
        int increments = 0;

        for (int k = 0; k < INCREMENTS || (fut != null && !fut.isDone()); k++) {
            increments++;

            if (invokeAll) {
                IgniteCache<String, Set<String>> cache = ignite(0).cache(cacheName);

                Map<String, Processor> procs = new LinkedHashMap<>();

                for (int i = 0; i < KEYS; i++) {
                    String key = "set-" + i;

                    String val = "value-" + k;

                    procs.put(key, new Processor(val));
                }

                Map<String, EntryProcessorResult<Integer>> resMap = cache.invokeAll(procs);

                for (String key : procs.keySet()) {
                    EntryProcessorResult<Integer> res = resMap.get(key);

                    assertNotNull(res);
                    assertEquals(k + 1, (Object) res.get());
                }
            }
            else {
                IgniteCache<String, Set<String>> cache = ignite(0).cache(cacheName);

                for (int i = 0; i < KEYS; i++) {
                    String key = "set-" + i;

                    String val = "value-" + k;

                    Integer valsCnt = cache.invoke(key, new Processor(val));

                    Integer exp = k + 1;

                    if (!exp.equals(valsCnt))
                        log.info("Unexpected return value [valsCnt=" + valsCnt +
                            ", exp=" + exp +
                            ", cacheVal=" + cache.get(key) + ']');

                    assertEquals(exp, valsCnt);
                }
            }

            if (latch != null && k == 0)
                latch.countDown();
        }

        return increments;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplaceNodeJoin() throws Exception {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final int started = 6;

        try {
            int keys = 100;

            final AtomicBoolean done = new AtomicBoolean(false);

            for (int i = 0; i < keys; i++)
                ignite(0).cache(null).put(i, 0);

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    try {
                        for (int i = 0; i < started; i++) {
                            U.sleep(1_000);

                            IgniteEx grid = startGrid(GRID_CNT + i);

                            info("Test started grid [idx=" + (GRID_CNT + i) + ", nodeId=" + grid.localNode().id() + ']');
                        }
                    }
                    catch (Exception e) {
                        error.compareAndSet(null, e);
                    }
                    finally {
                        done.set(true);
                    }
                }
            }, 1, "starter");

            int updVal = 0;

            try {
                while (!done.get()) {
                    info("Will put: " + (updVal + 1));

                    for (int i = 0; i < keys; i++)
                        assertTrue("Failed [key=" + i + ", oldVal=" + updVal+ ']',
                            ignite(0).cache(null).replace(i, updVal, updVal + 1));

                    updVal++;
                }
            }
            finally {
                fut.get(getTestTimeout());
            }

            for (int i = 0; i < keys; i++) {
                for (int g = 0; g < GRID_CNT + started; g++) {
                    Integer val = ignite(g).<Integer, Integer>cache(null).get(i);

                    GridCacheEntryEx entry = ((IgniteKernal)grid(g)).internalCache(null).peekEx(i);

                    if (updVal != val)
                        info("Invalid value for grid [g=" + g + ", entry=" + entry + ']');

                    assertEquals((Integer)updVal, val);
                }
            }
        }
        finally {
            for (int i = 0; i < started; i++)
                stopGrid(GRID_CNT + i);
        }
    }

    /** */
    private static class Processor implements EntryProcessor<String, Set<String>, Integer>, Serializable {
        /** */
        private String val;

        /**
         * @param val Value.
         */
        private Processor(String val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<String, Set<String>> e, Object... args) {
            Set<String> vals = e.getValue();

            if (vals == null)
                vals = new HashSet<>();

            vals.add(val);

            e.setValue(vals);

            return vals.size();
        }
    }
}