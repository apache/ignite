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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

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
