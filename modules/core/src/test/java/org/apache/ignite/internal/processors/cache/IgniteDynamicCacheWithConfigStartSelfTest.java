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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteDynamicCacheWithConfigStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "partitioned";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        if (client)
            cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(String.class, String.class);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnClient() throws Exception {
        int srvCnt = 3;

        startGrids(srvCnt);

        try {
            client = true;

            IgniteEx client = startGrid(srvCnt);

            for (int i = 0; i < 100; i++)
                client.cache(CACHE_NAME).put(i, i);

            for (int i = 0; i < 100; i++)
                assertEquals(i, grid(0).cache(CACHE_NAME).get(i));

            client.cache(CACHE_NAME).removeAll();

            for (int i = 0; i < 100; i++)
                assertNull(grid(0).cache(CACHE_NAME).get(i));
        }
        finally {
            stopAllGrids();
        }
    }
}