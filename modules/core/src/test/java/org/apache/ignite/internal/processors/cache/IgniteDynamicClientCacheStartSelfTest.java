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

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridNoStorageCacheMap;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests that cache specified in configuration start on client nodes.
 */
public class IgniteDynamicClientCacheStartSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private CacheConfiguration ccfg;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConfiguredCacheOnClientNode() throws Exception {
        ccfg = new CacheConfiguration();

        final String cacheName = null;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        client = true;

        Ignite ignite1 = startGrid(1);

        checkCache(ignite1, cacheName, false, false);

        ccfg = new CacheConfiguration();

        ccfg.setNearConfiguration(new NearCacheConfiguration());

        Ignite ignite2 = startGrid(2);

        checkCache(ignite2, cacheName, false, true);

        ccfg = null;

        Ignite ignite3 = startGrid(3);

        checkNoCache(ignite3, cacheName);

        assertNotNull(ignite3.cache(cacheName));

        checkCache(ignite3, cacheName, false, false);

        Ignite ignite4 = startGrid(4);

        checkNoCache(ignite4, cacheName);

        assertNotNull(ignite4.createNearCache(cacheName, new NearCacheConfiguration<>()));

        checkCache(ignite4, cacheName, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCacheStartError() throws Exception {
        ccfg = new CacheConfiguration();

        final String cacheName = null;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        client = true;

        final Ignite ignite1 = startGrid(1);

        checkCache(ignite1, cacheName, false, false);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.getOrCreateNearCache(cacheName, new NearCacheConfiguration<>());

                return null;
            }
        }, CacheException.class, null);

        checkCache(ignite1, cacheName, false, false);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.createNearCache(cacheName, new NearCacheConfiguration<>());

                return null;
            }
        }, CacheException.class, null);

        checkCache(ignite1, cacheName, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedCacheClient() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setCacheMode(REPLICATED);

        final String cacheName = null;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        client = true;

        final Ignite ignite1 = startGrid(1);

        checkCache(ignite1, cacheName, false, false);

        ccfg.setNearConfiguration(new NearCacheConfiguration());

        Ignite ignite2 = startGrid(2);

        checkCache(ignite2, cacheName, false, true);

        ccfg = null;

        Ignite ignite3 = startGrid(3);

        checkNoCache(ignite3, cacheName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedWithNearCacheClient() throws Exception {
        ccfg = new CacheConfiguration();

        ccfg.setNearConfiguration(new NearCacheConfiguration());

        ccfg.setCacheMode(REPLICATED);

        final String cacheName = null;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        client = true;

        final Ignite ignite1 = startGrid(1);

        checkCache(ignite1, cacheName, false, true);

        ccfg.setNearConfiguration(null);

        Ignite ignite2 = startGrid(2);

        checkCache(ignite2, cacheName, false, false);

        ccfg = null;

        Ignite ignite3 = startGrid(3);

        checkNoCache(ignite3, cacheName);
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name
     * @param srv {@code True} if server cache is expected.
     * @param near {@code True} if near cache is expected.
     */
    private void checkCache(Ignite ignite, String cacheName, boolean srv, boolean near) {
        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

        assertNotNull("No cache on node " + ignite.name(), cache);

        assertEquals(near, cache.context().isNear());

        if (near)
            cache = ((GridNearCacheAdapter)cache).dht();

        if (srv)
            assertSame(GridCacheConcurrentMap.class, cache.map().getClass());
        else
            assertSame(GridNoStorageCacheMap.class, cache.map().getClass());

        ClusterNode node = ((IgniteKernal)ignite).localNode();

        for (Ignite ignite0 : Ignition.allGrids()) {
            GridDiscoveryManager disco = ((IgniteKernal)ignite0).context().discovery();

            assertTrue(disco.cacheNode(node, cacheName));
            assertEquals(srv, disco.cacheAffinityNode(node, cacheName));
            assertEquals(near, disco.cacheNearNode(node, cacheName));

            if (srv)
                assertTrue(ignite0.affinity(null).primaryPartitions(node).length > 0);
            else
                assertEquals(0, ignite0.affinity(null).primaryPartitions(node).length);
        }

        assertNotNull(ignite.cache(cacheName));
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     */
    private void checkNoCache(Ignite ignite, String cacheName) {
        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

        assertNull("Unexpected cache on node " + ignite.name(), cache);

        ClusterNode node = ((IgniteKernal)ignite).localNode();

        for (Ignite ignite0 : Ignition.allGrids()) {
            GridDiscoveryManager disco = ((IgniteKernal)ignite0).context().discovery();

            assertFalse(disco.cacheNode(node, cacheName));
            assertFalse(disco.cacheAffinityNode(node, cacheName));
            assertFalse(disco.cacheNearNode(node, cacheName));
        }
    }
}