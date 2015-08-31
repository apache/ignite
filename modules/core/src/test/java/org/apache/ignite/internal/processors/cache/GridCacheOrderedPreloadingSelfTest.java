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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/**
 * Checks ordered preloading.
 */
public class GridCacheOrderedPreloadingSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of grids in test. */
    private static final int GRID_CNT = 4;

    /** First cache name. */
    public static final String FIRST_CACHE_NAME = "first";

    /** Second cache name. */
    public static final String SECOND_CACHE_NAME = "second";

    /** First cache mode. */
    private CacheMode firstCacheMode;

    /** Second cache mode. */
    private CacheMode secondCacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(
            cacheConfig(firstCacheMode, 1, FIRST_CACHE_NAME),
            cacheConfig(secondCacheMode, 2, SECOND_CACHE_NAME));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @param cacheMode Cache mode.
     * @param preloadOrder Preload order.
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfig(CacheMode cacheMode, int preloadOrder, String name) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);
        cfg.setCacheMode(cacheMode);
        cfg.setRebalanceOrder(preloadOrder);
        cfg.setRebalanceMode(ASYNC);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderPartitionedPartitioned() throws Exception {
        checkPreloadOrder(PARTITIONED, PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderReplicatedReplicated() throws Exception {
        checkPreloadOrder(REPLICATED, REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderPartitionedReplicated() throws Exception {
        checkPreloadOrder(PARTITIONED, REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloadOrderReplicatedPartitioned() throws Exception {
        checkPreloadOrder(REPLICATED, PARTITIONED);
    }

    /**
     * @param first First cache mode.
     * @param second Second cache mode.
     * @throws Exception If failed.
     */
    private void checkPreloadOrder(CacheMode first, CacheMode second) throws Exception {
        firstCacheMode = first;
        secondCacheMode = second;

        Ignite g = startGrid(0);

        try {
            IgniteCache<Object, Object> cache = g.cache("first");

            // Put some data into cache.
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            for (int i = 1; i < GRID_CNT; i++)
                startGrid(i);

            // For first node in topology replicated preloader gets completed right away.
            for (int i = 1; i < GRID_CNT; i++) {
                IgniteKernal kernal = (IgniteKernal)grid(i);

                GridFutureAdapter<?> fut1 = (GridFutureAdapter<?>)kernal.internalCache(FIRST_CACHE_NAME).preloader()
                    .syncFuture();
                GridFutureAdapter<?> fut2 = (GridFutureAdapter<?>)kernal.internalCache(SECOND_CACHE_NAME).preloader()
                    .syncFuture();

                fut1.get();
                fut2.get();

                assertTrue("[i=" + i + ", fut1=" + fut1 + ", fut2=" + fut2 + ']', fut1.endTime() <= fut2.endTime());
            }
        }
        finally {
            stopAllGrids();
        }
    }
}