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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

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
        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setDefaultTimeToLive(0);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
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
        assertEquals(26, cache(0, "test").size());
    }

    /** @throws Exception If failed. */
    public void testTwoNodes() throws Exception {
        checkNodes(startGridsMultiThreaded(2));

        assertTrue(colocated(0, "test").size() > 0);
        assertTrue(cache(0, "test").size() > 0);
    }

    /** @throws Exception If failed. */
    public void testThreeNodes() throws Exception {
        checkNodes(startGridsMultiThreaded(3));

        assertTrue(colocated(0, "test").size() > 0);
        assertTrue(cache(0, "test").size() > 0);
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

            if (affinity(cache).mapKeyToNode(key).isLocal())
                assertSame(v1, v2);
            else
                assertEquals(v1, v2);
        }
    }
}
