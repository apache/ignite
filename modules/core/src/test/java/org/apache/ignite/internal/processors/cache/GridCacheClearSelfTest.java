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

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for cache clear.
 */
public class GridCacheClearSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        Ignition.setClientMode(true);

        try {
            startGrid("client1");
            startGrid("client2");
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearPartitioned() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.ONHEAP_TIERED, false, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearPartitionedOffHeap() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.OFFHEAP_TIERED, false, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearPartitionedNear() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.ONHEAP_TIERED, true, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearPartitionedOffHeapNear() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.OFFHEAP_TIERED, true, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearReplicated() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.ONHEAP_TIERED, false, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearReplicatedOffHeap() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.OFFHEAP_TIERED, false, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearReplicatedNear() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.ONHEAP_TIERED, true, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearReplicatedOffHeapNear() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.OFFHEAP_TIERED, true, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyPartitioned() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.ONHEAP_TIERED, false, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyPartitionedOffHeap() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.OFFHEAP_TIERED, false, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyPartitionedNear() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.ONHEAP_TIERED, true, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyPartitionedOffHeapNear() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.OFFHEAP_TIERED, true, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyReplicated() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.ONHEAP_TIERED, false, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyReplicatedOffHeap() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.OFFHEAP_TIERED, false, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyReplicatedNear() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.ONHEAP_TIERED, true, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeyReplicatedOffHeapNear() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.OFFHEAP_TIERED, true, Collections.singleton(3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysPartitioned() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.ONHEAP_TIERED, false, F.asSet(2, 6, 9));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysPartitionedOffHeap() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.OFFHEAP_TIERED, false, F.asSet(2, 6, 9));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysPartitionedNear() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.ONHEAP_TIERED, true, F.asSet(2, 6, 9));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysPartitionedOffHeapNear() throws Exception {
        testClear(CacheMode.PARTITIONED, CacheMemoryMode.OFFHEAP_TIERED, true, F.asSet(2, 6, 9));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysReplicated() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.ONHEAP_TIERED, false, F.asSet(2, 6, 9));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysReplicatedOffHeap() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.OFFHEAP_TIERED, false, F.asSet(2, 6, 9));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysReplicatedNear() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.ONHEAP_TIERED, true, F.asSet(2, 6, 9));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearKeysReplicatedOffHeapNear() throws Exception {
        testClear(CacheMode.REPLICATED, CacheMemoryMode.OFFHEAP_TIERED, true, F.asSet(2, 6, 9));
    }

    /**
     * @param cacheMode Cache mode.
     * @param memMode Memory mode.
     * @param near Near cache flag.
     * @param keys Keys to clear.
     */
    private void testClear(CacheMode cacheMode, CacheMemoryMode memMode, boolean near, @Nullable Set<Integer> keys) {
        Ignite client1 = client1();
        Ignite client2 = client2();

        try {
            CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>("cache");

            cfg.setCacheMode(cacheMode);
            cfg.setMemoryMode(memMode);

            IgniteCache<Integer, Integer> cache1 = near ?
                client1.createCache(cfg, new NearCacheConfiguration<Integer, Integer>()) :
                client1.createCache(cfg);

            IgniteCache<Integer, Integer> cache2 = near ?
                client2.createNearCache("cache", new NearCacheConfiguration<Integer, Integer>()) :
                client2.<Integer, Integer>cache("cache");

            for (int i = 0; i < 10; i++)
                cache1.put(i, i);

            for (int i = 0; i < 10; i++)
                cache2.get(i);

            assertEquals(10, cache1.size(CachePeekMode.PRIMARY));
            assertEquals(10, cache2.size(CachePeekMode.PRIMARY));
            assertEquals(near ? 10 : 0, cache1.localSize(CachePeekMode.NEAR));
            assertEquals(near ? 10 : 0, cache2.localSize(CachePeekMode.NEAR));

            if (F.isEmpty(keys))
                cache1.clear();
            else if (keys.size() == 1)
                cache1.clear(F.first(keys));
            else
                cache1.clearAll(keys);

            int expSize = F.isEmpty(keys) ? 0 : 10 - keys.size();

            assertEquals(expSize, cache1.size(CachePeekMode.PRIMARY));
            assertEquals(expSize, cache2.size(CachePeekMode.PRIMARY));
            assertEquals(near ? expSize : 0, cache1.localSize(CachePeekMode.NEAR));
            assertEquals(near ? expSize : 0, cache2.localSize(CachePeekMode.NEAR));
        }
        finally {
            client1.destroyCache("cache");
        }
    }

    /**
     * @return Client 1.
     */
    private Ignite client1() {
        return grid("client1");
    }

    /**
     * @return Client 2.
     */
    private Ignite client2() {
        return grid("client2");
    }
}
