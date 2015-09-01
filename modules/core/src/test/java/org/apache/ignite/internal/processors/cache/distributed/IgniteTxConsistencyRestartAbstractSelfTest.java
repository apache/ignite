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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class IgniteTxConsistencyRestartAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** Key range. */
    private static final int RANGE = 100_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(cacheMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setNearConfiguration(nearConfiguration());
        ccfg.setRebalanceMode(SYNC);

        if (cacheMode() == CacheMode.PARTITIONED)
            ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Partition distribution mode for PARTITIONED cache.
     */
    protected abstract NearCacheConfiguration nearConfiguration();

    /**
     * @throws Exception If failed.
     */
    public void testTxConsistency() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        IgniteDataStreamer<Object, Object> ldr = grid(0).dataStreamer(null);

        for (int i = 0; i < RANGE; i++) {
            ldr.addData(i, 0);

            if (i > 0 && i % 1000 == 0)
                info("Put keys: " + i);
        }

        ldr.close();

        final AtomicBoolean done = new AtomicBoolean(false);

        Thread restartThread = new Thread() {
            @Override public void run() {
                Random rnd = new Random();

                while (!done.get()) {
                    try {
                        int idx = rnd.nextInt(GRID_CNT);

                        stopGrid(idx);

                        startGrid(idx);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        restartThread.start();

        Random rnd = new Random();

        // Make some iterations with 1-3 keys transactions.
        for (int i = 0; i < 50_000; i++) {
            int idx = i % GRID_CNT;

            if (i > 0 && i % 1000 == 0)
                info("Running iteration: " + i);

            try {
                IgniteKernal grid = (IgniteKernal)grid(idx);

                IgniteCache<Integer, Integer> cache = grid.cache(null);

                List<Integer> keys = new ArrayList<>();

                int keyCnt = rnd.nextInt(3);

                for (int k = 0; k < keyCnt; k++)
                    keys.add(rnd.nextInt(RANGE));

                Collections.sort(keys);

                try (Transaction tx = grid.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Map<Integer, Integer> map = cache.getAll(new LinkedHashSet<Integer>(keys));

                    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                        assertNotNull("Null value received from cache [key=" + entry.getKey() + "]", entry.getValue());

                        cache.put(entry.getKey(), entry.getValue() + 1);
                    }

                    tx.commit();
                }
            }
            catch (Exception e) {
                info("Failed to update keys: " + e.getMessage());
            }
        }

        done.set(true);

        restartThread.join();

        for (int k = 0; k < RANGE; k++) {
            Integer val = null;

            for (int i = 0; i < GRID_CNT; i++) {
                IgniteEx grid = grid(i);

                IgniteCache<Integer, Integer> cache = grid.cache(null);

                if (grid.affinity(null).isPrimaryOrBackup(grid.localNode(), k)) {
                    if (val == null) {
                        val = cache.localPeek(k, CachePeekMode.ONHEAP);

                        assertNotNull("Failed to peek value for key: " + k, val);
                    }
                    else
                        assertEquals("Failed to find value in cache [primary=" +
                            grid.affinity(null).isPrimary(grid.localNode(), k) + ']',
                            val, cache.localPeek(k, CachePeekMode.ONHEAP));
                }
            }
        }
    }
}