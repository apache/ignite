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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test rebalance with different thread pool size for nodes.
 */
public class RebalanceWithDifferentThreadPoolSizeTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Rebalance pool size. */
    private int rebalancePoolSize;

    /** Entries count. */
    private static final int ENTRIES_COUNT = 3000;

    /** Cache name. */
    private static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        cfg.setRebalanceThreadPoolSize(rebalancePoolSize);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
            .setRebalanceMode(CacheRebalanceMode.SYNC) // Wait reblance finish before node start
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void shouldFinishRebalanceWhenConnectedNewNodeWithHigherThreadPoolSize() throws Exception {
        rebalancePoolSize = 1;

        IgniteEx ex = startGrid(0);

        ex.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ex.cache(CACHE_NAME);
        for (int i = 0; i < ENTRIES_COUNT; i++)
            cache.put(i, i);

        rebalancePoolSize = 2;

        startGrid(1);

        rebalancePoolSize = 5;

        startGrid(2);

        assertEquals(3, ex.context().discovery().aliveServerNodes().size());
    }
}
