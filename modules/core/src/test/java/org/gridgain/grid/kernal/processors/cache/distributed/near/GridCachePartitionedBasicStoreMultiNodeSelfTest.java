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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import javax.cache.configuration.*;
import java.util.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCachePartitionedBasicStoreMultiNodeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of grids to start. */
    private static final int GRID_CNT = 3;

    /** Cache store. */
    private static List<GridCacheTestStore> stores;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (GridCacheTestStore store : stores)
            store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            cache(i).removeAll();

        for (GridCacheTestStore store : stores)
            store.reset();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stores = Collections.synchronizedList(new ArrayList<GridCacheTestStore>());

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        stores = null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setBackups(1);

        GridCacheTestStore store = new GridCacheTestStore();

        stores.add(store);

        cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Distribution mode.
     */
    protected GridCacheDistributionMode mode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutFromPrimary() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isPrimary(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.put(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutFromBackup() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.put(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutFromNear() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (!cache.affinity().isPrimaryOrBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.put(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentFromPrimary() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isPrimary(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.putIfAbsent(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentFromBackup() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.putIfAbsent(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentFromNear() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (!cache.affinity().isPrimaryOrBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.putIfAbsent(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        Map<Integer, String> map = new HashMap<>(10);

        for (int i = 0; i < 10; i++)
            map.put(i, "val");

        cache.putAll(map);

        checkStoreUsage(-1, 0, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleOperations() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        try (IgniteTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ)) {
            cache.put(1, "val");
            cache.put(2, "val");
            cache.put(3, "val");

            cache.get(4);

            cache.putAll(F.asMap(5, "val", 6, "val"));

            tx.commit();
        }

        checkStoreUsage(4, 0, 1, 1);
    }

    /**
     * @param expLoad Expected load calls.
     * @param expPut Expected put calls.
     * @param expPutAll Expected putAll calls.
     * @param expTxs Expected number of transactions.
     */
    private void checkStoreUsage(int expLoad, int expPut, int expPutAll, int expTxs) {
        int load = 0;
        int put = 0;
        int putAll = 0;
        int txs = 0;

        for (GridCacheTestStore store : stores) {
            load += store.getLoadCount();

            put += store.getPutCount();

            putAll += store.getPutAllCount();

            txs += store.transactions().size();
        }

        if (expLoad != -1)
            assertEquals(expLoad, load);

        assertEquals(expPut, put);
        assertEquals(expPutAll, putAll);
        assertEquals(expTxs, txs);
    }
}
