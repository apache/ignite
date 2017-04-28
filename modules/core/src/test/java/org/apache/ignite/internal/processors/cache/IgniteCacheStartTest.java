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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheStartTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartAndNodeJoin() throws Exception {
        Ignite node0 = startGrid(0);

        checkCache(0, "c1", false);

        node0.createCache(cacheConfiguration("c1"));

        checkCache(0, "c1", true);

        startGrid(1);

        checkCache(0, "c1", true);
        checkCache(1, "c1", true);

        client = true;

        startGrid(2);

        checkCache(0, "c1", true);
        checkCache(1, "c1", true);
        checkCache(2, "c1", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartFromJoiningNode1() throws Exception {
        checkStartFromJoiningNode(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartFromJoiningNode2() throws Exception {
        checkStartFromJoiningNode(true);
    }

    /**
     * @param joinClient {@code True} if client node joins.
     * @throws Exception If failed.
     */
    private void checkStartFromJoiningNode(boolean joinClient) throws Exception {
        startGrid(0);
        startGrid(1);

        client = true;

        startGrid(2);

        ccfg = cacheConfiguration("c1");
        client = joinClient;

        startGrid(3);

        checkCache(0, "c1", true);
        checkCache(1, "c1", true);
        checkCache(2, "c1", false);
        checkCache(3, "c1", true);

        client = false;
        ccfg = null;

        startGrid(4);

        checkCache(0, "c1", true);
        checkCache(1, "c1", true);
        checkCache(2, "c1", false);
        checkCache(3, "c1", true);
        checkCache(4, "c1", true);

        client = true;

        startGrid(5);

        checkCache(0, "c1", true);
        checkCache(1, "c1", true);
        checkCache(2, "c1", false);
        checkCache(3, "c1", true);
        checkCache(4, "c1", true);
        checkCache(5, "c1", false);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(cacheName);

        return ccfg;
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param expCache {@code True} if cache should be created.
     */
    private void checkCache(int idx, String cacheName, boolean expCache) {
        IgniteKernal node = (IgniteKernal)ignite(idx);

        if (expCache)
            assertNotNull(node.context().cache().cache(cacheName));
        else
            assertNull(node.context().cache().cache(cacheName));

        assertNotNull(node.context().cache().cache(CU.UTILITY_CACHE_NAME));
    }
}
