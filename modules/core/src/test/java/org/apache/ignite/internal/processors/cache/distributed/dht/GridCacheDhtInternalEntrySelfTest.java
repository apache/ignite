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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAlwaysEvictionPolicy;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Tests for internal DHT entry.
 */
public class GridCacheDhtInternalEntrySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Atomic long name. */
    private static final String ATOMIC_LONG_NAME = "test.atomic.long";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 2));
        cacheCfg.setBackups(0);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();
        nearCfg.setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy());
        cacheCfg.setNearConfiguration(nearCfg);

        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If failed. */
    public void testInternalKeyReaders() throws Exception {
        IgniteBiTuple<ClusterNode, ClusterNode> nodes = getNodes(ATOMIC_LONG_NAME);

        ClusterNode primary = nodes.get1();
        ClusterNode other = nodes.get2();

        // Create on non-primary node.
        grid(other).cache(null).put(new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME), 1);

        check(primary, other, true);

        // Update on primary.
        grid(primary).cache(null).put(new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME), 2);

        // Check on non-primary.
        assertEquals(2, grid(other).cache(null).get(new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME)));

        check(primary, other, true);

        // Remove.
        assert grid(other).cache(null).remove(new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME));

        check(primary, other, false);
    }

    /**
     * @param primary Primary node.
     * @param other Non-primary node.
     * @param exists Whether entry is expected to exist.
     * @throws Exception In case of error.
     */
    private void check(ClusterNode primary, ClusterNode other, boolean exists) throws Exception {
        if (exists) {
            // Check primary node has entry in DHT cache.
            assert peekNear(primary) == null;
            assert peekDht(primary) != null;

            // Check non-primary node has entry in near cache.
            assert peekNear(other) != null;
            assert peekDht(other) == null;

            // Check primary node has reader for non-primary node.
            assert peekDhtEntry(primary).readers().contains(other.id());
        }
        else {
            assert peekGlobal(primary) == null;
            assert peekGlobal(other) == null;
        }
    }

    /**
     * @param node Node.
     * @return Atomic long value.
     */
    private Object peekGlobal(ClusterNode node) {
        return grid(node).cache(null).localPeek(new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME), CachePeekMode.ONHEAP);
    }

    /**
     * @param node Node.
     * @return Atomic long value.
     */
    private Object peekNear(ClusterNode node) {
        return grid(node).cache(null).localPeek(new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME), CachePeekMode.NEAR);
    }

    /**
     * @param node Node.
     * @return Atomic long value.
     */
    private Object peekDht(ClusterNode node) {
        return grid(node).cache(null).localPeek(new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME), CachePeekMode.BACKUP,
            CachePeekMode.PRIMARY);
    }

    /**
     * @param node Node.
     * @return DHT entry.
     */
    private GridDhtCacheEntry peekDhtEntry(ClusterNode node) {
        return (GridDhtCacheEntry)dht(grid(node).cache(null)).peekEx(
            new GridCacheInternalKeyImpl(ATOMIC_LONG_NAME));
    }

    /**
     * @param key Key.
     * @return Pair {primary node, some other node}.
     */
    private IgniteBiTuple<ClusterNode, ClusterNode> getNodes(String key) {
        Affinity<Object> aff = grid(0).affinity(null);

        ClusterNode primary = aff.mapKeyToNode(key);

        assert primary != null;

        Collection<ClusterNode> nodes = new ArrayList<>(grid(0).cluster().nodes());

        nodes.remove(primary);

        ClusterNode other = F.first(nodes);

        assert other != null;

        assert !F.eqNodes(primary, other);

        return F.t(primary, other);
    }
}