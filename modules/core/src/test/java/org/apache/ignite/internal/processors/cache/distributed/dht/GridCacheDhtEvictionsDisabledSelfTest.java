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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test cache closure execution.
 */
public class GridCacheDhtEvictionsDisabledSelfTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheDhtEvictionsDisabledSelfTest() {
        super(false); // Don't start grid node.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName("test");
        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(null);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If failed. */
    public void testOneNode() throws Exception {
        checkNodes(startGridsMultiThreaded(1));

        assertEquals(26, colocated(0, "test").size());
        assertEquals(26, jcache(0, "test").localSize());
    }

    /** @throws Exception If failed. */
    public void testTwoNodes() throws Exception {
        checkNodes(startGridsMultiThreaded(2));

        assertTrue(colocated(0, "test").size() > 0);
        assertTrue(jcache(0, "test").localSize() > 0);
    }

    /** @throws Exception If failed. */
    public void testThreeNodes() throws Exception {
        checkNodes(startGridsMultiThreaded(3));

        assertTrue(colocated(0, "test").size() > 0);
        assertTrue(jcache(0, "test").localSize() > 0);
    }

    /**
     * @param g Grid.
     * @throws Exception If failed.
     */
    private void checkNodes(Ignite g) throws Exception {
        IgniteCache<String, String> cache = g.cache("test");

        for (char c = 'a'; c <= 'z'; c++) {
            String key = Character.toString(c);

            cache.put(key, "val-" + key);

            String v1 = cache.get(key);
            String v2 = cache.get(key); // Get second time.

            info("v1: " + v1);
            info("v2: " + v2);

            assertNotNull(v1);
            assertNotNull(v2);

            assertEquals(v1, v2);
        }
    }
}