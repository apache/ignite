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

package org.apache.ignite.cache.store.jdbc;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jsr166.LongAdder8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 *
 */
public class GridCacheJdbcBlobStoreMultithreadedSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of grids to start. */
    private static final int GRID_CNT = 5;

    /** Number of transactions. */
    private static final int TX_CNT = 1000;

    /** Client flag. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_CNT - 2);

        client = true;

        Ignite grid = startGrid(GRID_CNT - 2);

        grid.createNearCache(null, new NearCacheConfiguration());

        grid = startGrid(GRID_CNT - 1);

        grid.cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        if (!client) {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(PARTITIONED);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setSwapEnabled(false);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setBackups(1);

            cc.setCacheStoreFactory(new TestStoreFactory());
            cc.setReadThrough(true);
            cc.setWriteThrough(true);
            cc.setLoadPreviousValue(true);

            c.setCacheConfiguration(cc);
        }
        else
            c.setClientMode(true);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPut() throws Exception {
        IgniteInternalFuture<?> fut1 = runMultiThreadedAsync(new Callable<Object>() {
            private final Random rnd = new Random();

            @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    IgniteCache<Object, Object> cache = jcache(rnd.nextInt(GRID_CNT));

                    cache.put(rnd.nextInt(1000), "value");
                }

                return null;
            }
        }, 4, "put");

        IgniteInternalFuture<?> fut2 = runMultiThreadedAsync(new Callable<Object>() {
            private final Random rnd = new Random();

            @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    IgniteCache<Object, Object> cache = jcache(rnd.nextInt(GRID_CNT));

                    cache.putIfAbsent(rnd.nextInt(1000), "value");
                }

                return null;
            }
        }, 4, "putIfAbsent");

        fut1.get();
        fut2.get();

        checkOpenedClosedCount();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPutAll() throws Exception {
        runMultiThreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    Map<Integer, String> map = new TreeMap<>();

                    for (int j = 0; j < 10; j++)
                        map.put(rnd.nextInt(1000), "value");

                    IgniteCache<Object, Object> cache = jcache(rnd.nextInt(GRID_CNT));

                    cache.putAll(map);
                }

                return null;
            }
        }, 8, "putAll");

        checkOpenedClosedCount();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedExplicitTx() throws Exception {
        runMultiThreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    IgniteEx ignite = grid(rnd.nextInt(GRID_CNT));

                    IgniteCache<Object, Object> cache = ignite.cache(null);

                    try (Transaction tx = ignite.transactions().txStart()) {
                        cache.put(1, "value");
                        cache.put(2, "value");
                        cache.put(3, "value");

                        cache.get(1);
                        cache.get(4);

                        Map<Integer, String> map = new TreeMap<>();

                        map.put(5, "value");
                        map.put(6, "value");

                        cache.putAll(map);

                        tx.commit();
                    }
                }

                return null;
            }
        }, 8, "tx");

        checkOpenedClosedCount();
    }

    /**
     * Test store factory.
     */
    private static class TestStoreFactory implements Factory<CacheStore> {
        @Override public CacheStore create() {
            try {
                CacheStore<Integer, String> store = new CacheJdbcBlobStore<>();

                Field f = store.getClass().getDeclaredField("testMode");

                f.setAccessible(true);

                f.set(store, true);

                return store;
            }
            catch (NoSuchFieldException | IllegalAccessException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     *
     */
    private void checkOpenedClosedCount() {
        assertEquals(GRID_CNT, Ignition.allGrids().size());

        for (Ignite ignite : Ignition.allGrids()) {
            GridCacheContext cctx = ((IgniteKernal)ignite).internalCache().context();

            CacheStore store = cctx.store().configuredStore();

            long opened = ((LongAdder8)U.field(store, "opened")).sum();
            long closed = ((LongAdder8)U.field(store, "closed")).sum();

            assert opened > 0;
            assert closed > 0;

            assertEquals(opened, closed);
        }
    }
}