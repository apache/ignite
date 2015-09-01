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

package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import java.util.UUID;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKey;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test with variable number of nodes.
 */
public class GridCachePartitionedNodeRestartTxSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int INIT_GRID_NUM = 3;

    /** */
    private static final int MAX_GRID_NUM = 20;

    /**
     * Constructs a test.
     */
    public GridCachePartitionedNodeRestartTxSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(PARTITIONED);
        atomicCfg.setBackups(1);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testSimple() throws Exception {
        String key = UUID.randomUUID().toString();

        try {
            // Prepare nodes and cache data.
            prepareSimple(key);

            // Test simple key/value.
            checkSimple(key);
        }
        finally {
            for (int i = 0; i < MAX_GRID_NUM; i++)
                stopGrid(i);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCustom() throws Exception {
        String key = UUID.randomUUID().toString();

        try {
            // Prepare nodes and cache data.
            prepareCustom(key);

            // Test {@link GridCacheInternalKey}/{@link GridCacheAtomicLongValue}.
            checkCustom(key);
        }
        finally {
            for (int i = 0; i < MAX_GRID_NUM; i++)
                stopGrid(i);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        String key = UUID.randomUUID().toString();

        try {
            // Prepare nodes and cache data.
            prepareAtomic(key);

            // Test AtomicLong
            checkAtomic(key);
        }
        finally {
            for (int i = 0; i < MAX_GRID_NUM; i++)
                stopGrid(i);
        }
    }

    /**
     *  Test simple key/value.
     * @param key Simple key.
     * @throws Exception If failed.
     */
    private void checkSimple(String key) throws Exception {
        for (int i = INIT_GRID_NUM; i < MAX_GRID_NUM; i++) {
            startGrid(i);

            assert PARTITIONED == grid(i).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode();

            try (Transaction tx = grid(i).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Integer val = (Integer) grid(i).cache(null).get(key);

                assertEquals("Simple check failed for node: " + i, (Integer) i, val);

                grid(i).cache(null).put(key, i + 1);

                tx.commit();
            }

            stopGrid(i);
        }
    }

    /**
     * Test {@link GridCacheInternalKey}/{@link GridCacheAtomicLongValue}.
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkCustom(String name) throws Exception {
        for (int i = INIT_GRID_NUM; i < 20; i++) {
            startGrid(i);

            assert PARTITIONED == grid(i).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode();

            try (Transaction tx = grid(i).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                GridCacheAtomicLongValue atomicVal = ((GridCacheAtomicLongValue) grid(i).cache(null).get(key));

                assertNotNull(atomicVal);

                assertEquals("Custom check failed for node: " + i, (long) i, atomicVal.get());

                atomicVal.set(i + 1);

                grid(i).cache(null).put(key, atomicVal);

                tx.commit();
            }

            stopGrid(i);
        }
    }

    /**
     * Test AtomicLong.
     * @param name Name of atomic.
     * @throws Exception If failed.
     */
    private void checkAtomic(String name) throws Exception {
        for (int i = INIT_GRID_NUM; i < 20; i++) {
            startGrid(i);

            assert PARTITIONED == grid(i).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode();

            IgniteAtomicLong atomic = grid(i).atomicLong(name, 0, true);

            long val = atomic.get();

            assertEquals("Atomic check failed for node: " + i, (long)i, val);

            atomic.incrementAndGet();

            stopGrid(i);
        }
    }

    /**
     * Prepare test environment.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void prepareSimple(String key) throws Exception {
        // Start nodes.
        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert startGrid(i) != null;

        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert PARTITIONED == grid(i).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode();

        // Init cache data.

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // Put simple value.
            grid(0).cache(null).put(key, INIT_GRID_NUM);

            tx.commit();
        }
    }

    /**
     * Prepare test environment.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void prepareCustom(String key) throws Exception {
        // Start nodes.
        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert startGrid(i) != null;

        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert PARTITIONED == grid(i).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode();

        // Init cache data.

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // Put custom data
            grid(0).cache(null).put(new GridCacheInternalKeyImpl(key), new GridCacheAtomicLongValue(INIT_GRID_NUM));

            tx.commit();
        }

        stopGrid(0);
    }

    /**
     * Prepare test environment.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void prepareAtomic(String key) throws Exception {
        // Start nodes.
        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert startGrid(i) != null;

        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert PARTITIONED == grid(i).cache(null).getConfiguration(CacheConfiguration.class).getCacheMode();

        // Init cache data.
        grid(0).atomicLong(key, 0, true).getAndSet(INIT_GRID_NUM);

        assertEquals(INIT_GRID_NUM, grid(0).atomicLong(key, 0, true).get());

        stopGrid(0);
    }
}