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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CachePeekMode.NEAR;

/**
 * This class tests that {@link IgniteCache#get(Object)} updates a near cache entry after a node
 * which owns the primary partition for the given key left the cluster.
 */
public class GridCacheNearClientHitTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private final static TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private final static String CACHE_NAME = "test-near-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Grid configuration used for starting of grid.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getClientConfiguration(final String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception If failed.
     */
    private CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setBackups(1);

        cfg.setName(CACHE_NAME);

        return cfg;
    }

    /**
     * @return Near cache configuration.
     * @throws Exception If failed.
     */
    private NearCacheConfiguration nearCacheConfiguration() throws Exception {
        NearCacheConfiguration cfg = new NearCacheConfiguration<>();

        cfg.setNearEvictionPolicy(new LruEvictionPolicy<>(25000));

        return cfg;
    }

    public void testLocalPeekAfterPrimaryNodeLeft() throws Exception {
        try {
            Ignite crd = startGrid("coordinator", getConfiguration("coordinator"));

            Ignite client = startGrid("client", getClientConfiguration("client"));

            Ignite serverNode = startGrid("server", getConfiguration("server"));

            IgniteCache cache = serverNode.getOrCreateCache(cacheConfiguration());

            IgniteCache nearCache = client.createNearCache(CACHE_NAME, nearCacheConfiguration());

            UUID serverNodeId = serverNode.cluster().localNode().id();

            int remoteKey = 0;
            for (; ; remoteKey++) {
                if (crd.affinity(CACHE_NAME).mapKeyToNode(remoteKey).id().equals(serverNodeId))
                    break;
            }

            cache.put(remoteKey, remoteKey);

            Object value = nearCache.localPeek(remoteKey, NEAR);

            assertNull("The value should not be loaded from a remote node.", value);

            nearCache.get(remoteKey);

            value = nearCache.localPeek(remoteKey, NEAR);

            assertNotNull("The returned value should not be null.", value);

            serverNode.close();

            awaitPartitionMapExchange();

            value = nearCache.localPeek(remoteKey, NEAR);

            assertNull("The value should not be loaded from a remote node.", value);

            value = nearCache.get(remoteKey);

            assertNotNull("The value should be loaded from a remote node.", value);

            value = nearCache.localPeek(remoteKey, NEAR);

            assertNotNull("The returned value should not be null.", value);
        }
        finally {
            stopAllGrids();
        }
    }
}
