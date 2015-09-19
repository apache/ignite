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

package org.apache.ignite.internal.processors.cache.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests write-behind store with near and dht commit option.
 */
public class GridCacheWriteBehindStorePartitionedMultiNodeSelfTest extends GridCommonAbstractTest {
    /** Grids to start. */
    private static final int GRID_CNT = 5;

    /** Ip finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Flush frequency. */
    public static final int WRITE_BEHIND_FLUSH_FREQ = 1000;

    /** Stores per grid. */
    private GridCacheTestStore[] stores = new GridCacheTestStore[GRID_CNT];

    /** Start grid counter. */
    private int idx;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setWriteBehindEnabled(true);
        cc.setWriteBehindFlushFrequency(WRITE_BEHIND_FLUSH_FREQ);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(new NearCacheConfiguration());

        CacheStore store = stores[idx] = new GridCacheTestStore();

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        idx++;

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stores = null;

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    private void prepare() throws Exception {
        idx = 0;

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleWritesOnDhtNode() throws Exception {
        checkSingleWrites();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBatchWritesOnDhtNode() throws Exception {
        checkBatchWrites();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxWritesOnDhtNode() throws Exception {
        checkTxWrites();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSingleWrites() throws Exception {
        prepare();

        IgniteCache<Integer, String> cache = grid(0).cache(null);

        for (int i = 0; i < 100; i++)
            cache.put(i, String.valueOf(i));

        checkWrites();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkBatchWrites() throws Exception {
        prepare();

        Map<Integer, String> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, String.valueOf(i));

        grid(0).cache(null).putAll(map);

        checkWrites();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTxWrites() throws Exception {
        prepare();

        IgniteCache<Object, Object> cache = grid(0).cache(null);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 100; i++)
                cache.put(i, String.valueOf(i));

            tx.commit();
        }

        checkWrites();
    }

    /**
     * @throws IgniteInterruptedCheckedException If sleep was interrupted.
     */
    private void checkWrites() throws IgniteInterruptedCheckedException {
        U.sleep(WRITE_BEHIND_FLUSH_FREQ * 2);

        Collection<Integer> allKeys = new ArrayList<>(100);

        for (int i = 0; i < GRID_CNT; i++) {
            Map<Integer,String> map = stores[i].getMap();

            assertFalse("Missing writes for node: " + i, map.isEmpty());

            allKeys.addAll(map.keySet());

            // Check there is no intersection.
            for (int j = 0; j < GRID_CNT; j++) {
                if (i == j)
                    continue;

                Collection<Integer> intersection = new HashSet<>(stores[j].getMap().keySet());

                intersection.retainAll(map.keySet());

                assertTrue(intersection.isEmpty());
            }
        }

        assertEquals(100, allKeys.size());

        for (int i = 0; i < 100; i++)
            assertTrue(allKeys.contains(i));
    }
}