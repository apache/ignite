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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.processor.EntryProcessor;
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
    private static final int NUM_SETS = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache = new CacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setBackups(1);
        cache.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        cfg.setDiscoverySpi(disco);

        return cfg;
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
                checkIncrement(invokeAll);
            }
            finally {
                stop.set(true);

                fut.get(getTestTimeout());
            }

            for (int i = 0; i < NUM_SETS; i++) {
                for (int g = 0; g < GRID_CNT + started; g++) {
                    Set<String> vals = ignite(g).<String, Set<String>>cache(null).get("set-" + i);

                    assertNotNull(vals);
                    assertEquals(100, vals.size());
                }
            }
        }
        finally {
            for (int i = 0; i < started; i++)
                stopGrid(GRID_CNT + i);
        }
    }

    /**
     * @param invokeAll If {@code true} tests invokeAll operation.
     * @throws Exception If failed.
     */
    private void checkIncrement(boolean invokeAll) throws Exception {
        for (int k = 0; k < 100; k++) {
            if (invokeAll) {
                IgniteCache<String, Set<String>> cache = ignite(0).cache(null);

                Map<String, Processor> procs = new LinkedHashMap<>();

                for (int i = 0; i < NUM_SETS; i++) {
                    String key = "set-" + i;

                    String val = "value-" + k;

                    cache.invoke(key, new Processor(val));
                }

                cache.invokeAll(procs);
            }
            else {
                for (int i = 0; i < NUM_SETS; i++) {
                    String key = "set-" + i;

                    String val = "value-" + k;

                    IgniteCache<String, Set<String>> cache = ignite(0).cache(null);

                    cache.invoke(key, new Processor(val));
                }
            }
        }
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
    private static class Processor implements EntryProcessor<String, Set<String>, Void>, Serializable {
        /** */
        private String val;

        /**
         * @param val Value.
         */
        private Processor(String val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<String, Set<String>> e, Object... args) {
            Set<String> vals = e.getValue();

            if (vals == null)
                vals = new HashSet<>();

            vals.add(val);

            e.setValue(vals);

            return null;
        }
    }
}