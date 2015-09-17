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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests affinity mapping when {@link AffinityKeyMapper} is used.
 */
public class GridAffinityMappedTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridAffinityMappedTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        if (gridName.endsWith("1"))
            cfg.setCacheConfiguration(); // Empty cache configuration.
        else {
            assert gridName.endsWith("2") || gridName.endsWith("3");

            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setAffinity(new MockCacheAffinityFunction());
            cacheCfg.setAffinityMapper(new MockCacheAffinityKeyMapper());

            cfg.setCacheConfiguration(cacheCfg);
            cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, gridName.endsWith("2") ? 0 : 1));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        startGrid(2);
        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
        stopGrid(3);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testMappedAffinity() throws IgniteCheckedException {
        Ignite g1 = grid(1);
        Ignite g2 = grid(2);
        Ignite g3 = grid(3);

        assert g1.configuration().getCacheConfiguration().length == 0;
        assert g2.configuration().getCacheConfiguration()[0].getCacheMode() == PARTITIONED;
        assert g3.configuration().getCacheConfiguration()[0].getCacheMode() == PARTITIONED;

        ClusterNode first = g2.cluster().localNode();
        ClusterNode second = g3.cluster().localNode();

        //When MockCacheAfinity and MockCacheAffinityKeyMapper are set to cache configuration we expect the following.
        //Key 0 is mapped to partition 0, first node.
        //Key 1 is mapped to partition 1, second node.
        //key 2 is mapped to partition 0, first node because mapper substitutes key 2 with affinity key 0.
        Map<ClusterNode, Collection<Integer>> map = g1.cluster().mapKeysToNodes(null, F.asList(0));

        assertNotNull(map);
        assertEquals("Invalid map size: " + map.size(), 1, map.size());
        assertEquals(F.first(map.keySet()), first);

        UUID id1 = g1.cluster().mapKeyToNode(null, 1).id();

        assertNotNull(id1);
        assertEquals(second.id(),  id1);

        UUID id2 = g1.cluster().mapKeyToNode(null, 2).id();

        assertNotNull(id2);
        assertEquals(first.id(),  id2);
    }

    /**
     * Mock affinity implementation that ensures constant key-to-node mapping based on {@link GridCacheModuloAffinityFunction}
     * The partition selection is as follows: 0 maps to partition 0 and any other value maps to partition 1
     */
    private static class MockCacheAffinityFunction extends GridCacheModuloAffinityFunction {
        /**
         * Initializes module affinity with 2 parts and 0 backups
         */
        private MockCacheAffinityFunction() {
            super(2, 0);
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return Integer.valueOf(0) == key ? 0 : 1;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            //no-op
        }
    }

    /**
     * Mock affinity mapper implementation that substitutes values other than 0 and 1 with 0.
     */
    private static class MockCacheAffinityKeyMapper implements AffinityKeyMapper {
        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return key instanceof Integer ? 1 == (Integer)key ? key : 0 : key;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // This mapper is stateless and needs no initialization logic.
        }
    }
}