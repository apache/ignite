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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jdk8.backport.*;

import javax.cache.configuration.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;
import static org.apache.ignite.testframework.GridTestUtils.*;

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

    /** Distribution mode. */
    private GridCacheDistributionMode mode;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mode = NEAR_PARTITIONED;

        startGridsMultiThreaded(GRID_CNT - 2);

        mode = NEAR_ONLY;

        startGrid(GRID_CNT - 2);

        mode = CLIENT_ONLY;

        startGrid(GRID_CNT - 1);
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

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setBackups(1);
        cc.setDistributionMode(mode);

        cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store()));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedPut() throws Exception {
        IgniteFuture<?> fut1 = runMultiThreadedAsync(new Callable<Object>() {
            private final Random rnd = new Random();

            @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    GridCache<Integer, String> cache = cache(rnd.nextInt(GRID_CNT));

                    cache.put(rnd.nextInt(1000), "value");
                }

                return null;
            }
        }, 4, "put");

        IgniteFuture<?> fut2 = runMultiThreadedAsync(new Callable<Object>() {
            private final Random rnd = new Random();

            @Override public Object call() throws Exception {
                for (int i = 0; i < TX_CNT; i++) {
                    GridCache<Integer, String> cache = cache(rnd.nextInt(GRID_CNT));

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

                    GridCache<Integer, String> cache = cache(rnd.nextInt(GRID_CNT));

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
                    GridCache<Integer, String> cache = cache(rnd.nextInt(GRID_CNT));

                    try (IgniteTx tx = cache.txStart()) {
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
     * @return New store.
     * @throws Exception In case of error.
     */
    private CacheStore<Integer, String> store() throws Exception {
        CacheStore<Integer, String> store = new CacheJdbcBlobStore<>();

        Field f = store.getClass().getDeclaredField("testMode");

        f.setAccessible(true);

        f.set(store, true);

        return store;
    }

    /**
     *
     */
    private void checkOpenedClosedCount() {
        assertEquals(GRID_CNT, Ignition.allGrids().size());

        for (Ignite ignite : Ignition.allGrids()) {
            GridCacheContext cctx = ((GridKernal)ignite).internalCache().context();

            CacheStore store = cctx.store().configuredStore();

            long opened = ((LongAdder)U.field(store, "opened")).sum();
            long closed = ((LongAdder)U.field(store, "closed")).sum();

            assert opened > 0;
            assert closed > 0;

            assertEquals(opened, closed);
        }
    }
}
