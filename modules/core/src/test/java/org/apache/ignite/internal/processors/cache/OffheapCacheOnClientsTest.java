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

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.offheap.GridOffHeapProcessor;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class OffheapCacheOnClientsTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "CACHE_NAME";

    /** */
    private boolean client;

    /** */
    private boolean forceSrvMode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        client = true;

        startGrid(1);

        forceSrvMode = true;

        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (client) {
            cfg.setClientMode(true);

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(forceSrvMode);
        }

        return cfg;
    }
    /**
     * @throws Exception If failed.
     */
    public void testOffheapCacheOnClient() throws Exception {
        try {
            Ignite client = grid(1);

            testStartCacheOnClient(client, OFFHEAP_TIERED);
            testStartCacheOnClient(client, OFFHEAP_VALUES);
            testStartCacheOnClient(client, ONHEAP_TIERED);

            client = grid(2);

            testStartCacheOnClient(client, OFFHEAP_TIERED);
            testStartCacheOnClient(client, OFFHEAP_VALUES);
            testStartCacheOnClient(client, ONHEAP_TIERED);
        }
        finally {
            grid(0).destroyCache(CACHE_NAME);
        }
    }

    /**
     * @param client Node.
     * @param memMode Memory mode.
     * @throws Exception If failed.
     */
    private void testStartCacheOnClient(Ignite client, CacheMemoryMode memMode) throws Exception {
        assertTrue(client.configuration().isClientMode());

        try {
            client.createCache(new CacheConfiguration(CACHE_NAME)
                .setCacheMode(REPLICATED)
                .setOffHeapMaxMemory(1024 * 1024)
                .setMemoryMode(memMode));

            IgniteCache<Integer, Integer> cache = client.cache(CACHE_NAME);

            assertNotNull(cache);

            cache.put(1, 1);
            assertEquals((Integer)1, cache.get(1));

            GridOffHeapProcessor offheap = ((IgniteKernal)client).cachex(CACHE_NAME).context().offheap();

            assertNotNull(offheap);

            ConcurrentMap offheapMaps = GridTestUtils.getFieldValue(offheap, "offheap");
            assertNotNull(offheapMaps);

            assertEquals(0,offheapMaps.size());
        }
        finally {
            client.destroyCache(CACHE_NAME);
        }
    }
}
