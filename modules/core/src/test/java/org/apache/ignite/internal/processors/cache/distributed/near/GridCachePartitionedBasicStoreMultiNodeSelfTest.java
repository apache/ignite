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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class GridCachePartitionedBasicStoreMultiNodeSelfTest extends GridCommonAbstractTest {
    /** Number of grids to start. */
    private static final int GRID_CNT = 3;

    /** Cache store. */
    private static List<GridCacheTestStore> stores;

    /** */
    @Before
    public void beforeGridCachePartitionedBasicStoreMultiNodeSelfTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        if (nearCacheConfiguration() != null)
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (GridCacheTestStore store : stores)
            store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            jcache(i).removeAll();

        for (GridCacheTestStore store : stores)
            store.reset();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stores = Collections.synchronizedList(new ArrayList<GridCacheTestStore>());

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stores = null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        if (nearCacheConfiguration() != null)
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setBackups(1);

        cc.setCacheStoreFactory(new StoreFactory());
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        cc.setNearConfiguration(nearCacheConfiguration());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Distribution mode.
     */
    protected NearCacheConfiguration nearCacheConfiguration() {
        return new NearCacheConfiguration();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutFromPrimary() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        int key = primaryKey(cache);

        assertNull(cache.getAndPut(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutFromBackup() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        int key = backupKey(cache);

        assertNull(cache.getAndPut(key, "val"));

        checkStoreUsage(1, 1, 0, nearCacheConfiguration() == null ? 2 : 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutFromNear() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        int key = nearKey(cache);

        assertNull(cache.getAndPut(key, "val"));

        checkStoreUsage(1, 1, 0, nearCacheConfiguration() == null ? 2 : 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsentFromPrimary() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        int key = primaryKey(cache);

        assertTrue(cache.putIfAbsent(key, "val"));

        checkStoreUsage(1, 1, 0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsentFromBackup() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        int key = backupKey(cache);

        assertTrue(cache.putIfAbsent(key, "val"));

        checkStoreUsage(1, 1, 0, nearCacheConfiguration() == null ? 2 : 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsentFromNear() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        int key = nearKey(cache);

        assertTrue(cache.putIfAbsent(key, "val"));

        checkStoreUsage(1, 1, 0, nearCacheConfiguration() == null ? 2 : 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAll() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        Map<Integer, String> map = new HashMap<>();

        for (int i = 0; i < 10; i++)
            map.put(i, "val");

        cache.putAll(map);

        checkStoreUsage(-1, 0, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleOperations() throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
            cache.put(1, "val");
            cache.put(2, "val");
            cache.put(3, "val");

            cache.get(4);

            cache.putAll(F.asMap(5, "val", 6, "val"));

            tx.commit();
        }

        checkStoreUsage(1, 0, 1, 1);
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

    /**
     *
     */
    static class StoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            GridCacheTestStore store = new GridCacheTestStore();

            stores.add(store);

            return store;
        }
    }
}
