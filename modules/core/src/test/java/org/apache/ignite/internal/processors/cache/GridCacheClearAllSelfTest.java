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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test {@link IgniteCache#clear()} operation in multinode environment with nodes
 * having caches with different names.
 */
public class GridCacheClearAllSelfTest extends GridCommonAbstractTest {
    /** Grid nodes count. */
    private static final int GRID_CNT = 3;

    /** Amount of keys stored in the default cache. */
    private static final int KEY_CNT = 20;

    /** Amount of keys stored in cache other than default. */
    private static final int KEY_CNT_OTHER = 10;

    /** Default cache name. */
    private static final String CACHE_NAME = "cache_name";

    /** Cache name which differs from the default one. */
    private static final String CACHE_NAME_OTHER = "cache_name_other";

    /** Test attribute name. */
    private static final String TEST_ATTRIBUTE = "TestAttribute";

    /** VM IP finder for TCP discovery SPI. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name which will be passed to grid configuration. */
    private CacheMode cacheMode = PARTITIONED;

    /** Cache mode which will be passed to grid configuration. */
    private String cacheName = CACHE_NAME;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setNodeFilter(new AttributeFilter(cacheName));

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE, cacheName));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Start GRID_CNT nodes. All nodes except the last one will have one cache with particular name, while the last
     * one will have one cache of the same type, but with different name.
     *
     * @throws Exception In case of exception.
     */
    private void startNodes() throws Exception {
        cacheName = CACHE_NAME;

        for (int i = 0; i < GRID_CNT - 1; i++)
            startGrid(i);

        cacheName = CACHE_NAME_OTHER;

        startGrid(GRID_CNT - 1);

        awaitPartitionMapExchange();
    }

    /**
     * Test for partitioned cache.
     *
     * @throws Exception In case of exception.
     */
    public void testGlobalClearAllPartitioned() throws Exception {
        cacheMode = PARTITIONED;

        startNodes();

        performTest();
    }

    /**
     * Test for replicated cache.
     *
     * @throws Exception In case of exception.
     */
    public void testGlobalClearAllReplicated() throws Exception {
        cacheMode = REPLICATED;

        startNodes();

        performTest();
    }

    /**
     * Ensure that clear() clears correct cache and is only executed on nodes with the cache excluding
     * master-node where it is executed locally.
     *
     * @throws Exception If failed.
     */
    public void performTest() throws Exception {
        // Put values into normal replicated cache.
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).cache(CACHE_NAME).put(i, "val" + i);

        // Put values into a cache with another name.
        for (int i = 0; i < KEY_CNT_OTHER; i++)
            grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).put(i, "val" + i);

        // Check cache sizes.
        for (int i = 0; i < GRID_CNT - 1; i++) {
            IgniteCache<Object, Object> cache = grid(i).cache(CACHE_NAME);

            assertEquals("Key set [i=" + i + ']', KEY_CNT, cache.localSize(CachePeekMode.ALL));
        }

        assert grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).localSize() == KEY_CNT_OTHER;

        // Perform clear.
        grid(0).cache(CACHE_NAME).clear();

        // Expect caches with the given name to be clear on all nodes.
        for (int i = 0; i < GRID_CNT - 1; i++)
            assert grid(i).cache(CACHE_NAME).localSize() == 0;

        // ... but cache with another name should remain untouched.
        assert grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).localSize() == KEY_CNT_OTHER;
    }

    /** {@inheritDoc} */
    private static class AttributeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private String attrValue;

        /**
         * @param attrValue Attribute value.
         */
        private AttributeFilter(String attrValue) {
            this.attrValue = attrValue;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return F.eq(attrValue, clusterNode.attribute(TEST_ATTRIBUTE));
        }
    }
}