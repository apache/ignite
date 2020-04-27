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
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsDiscoDataHandlingInNewClusterTest extends GridCommonAbstractTest {
    /** */
    private static final String NODE_CONS_ID_0 = "node0";

    /** */
    private static final String NODE_CONS_ID_1 = "node1";

    /** */
    private static final String STATIC_CACHE_NAME_0 = "staticCache0";

    /** */
    private static final String DYNAMIC_CACHE_NAME_0 = "dynaCache0";

    /** */
    private static final String DYNAMIC_CACHE_NAME_1 = "dynaCache1";

    /** */
    private static final String DYNAMIC_CACHE_NAME_2 = "dynaCache2";

    /** */
    private static final String MIXED_CACHES_GROUP_NAME_0 = "mixedCachesGroup0";

    /** */
    private static final String DYNAMIC_CACHES_GROUP_NAME_1 = "dynaCachesGroup1";

    /** */
    private static final AffinityFunction AFFINITY = new RendezvousAffinityFunction(false, 16);

    /** Node filter to pin dynamic caches to a specific node. */
    private static final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
        @Override public boolean apply(ClusterNode node) {
            return node.consistentId().toString().contains(NODE_CONS_ID_1);
        }
    };

    /** */
    private static final AtomicBoolean SHOULD_FAIL = new AtomicBoolean(false);

    /** Discovery SPI aimed to fail node with it when another server node joins the topology. */
    private TcpDiscoverySpi failingOnNodeJoinSpi = new TcpDiscoverySpi() {
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (SHOULD_FAIL.get()) {
                if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                    super.startMessageProcess(msg);

                    throw new RuntimeException("Simulation of failure of node " + NODE_CONS_ID_0);
                }
            }

            super.startMessageProcess(msg);
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(STATIC_CACHE_NAME_0)
                .setGroupName(MIXED_CACHES_GROUP_NAME_0)
                .setAffinity(AFFINITY)
                .setNodeFilter(nodeFilter)
        );

        if (igniteInstanceName.equals(NODE_CONS_ID_0)) {
            failingOnNodeJoinSpi.setIpFinder(sharedStaticIpFinder);
            failingOnNodeJoinSpi.setJoinTimeout(60_000);

            cfg.setDiscoverySpi(failingOnNodeJoinSpi);
        }

        return cfg;
    }

    /**
     * @throws Exception
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    public void testNewDynamicCacheDoesntStartOnOldNode() throws Exception {
        IgniteEx ig0 = startGrid(NODE_CONS_ID_0);

        startGrid(NODE_CONS_ID_1);

        ig0.cluster().active(true);

        startDynamicCache(ig0, DYNAMIC_CACHE_NAME_0, MIXED_CACHES_GROUP_NAME_0);

        stopGrid(NODE_CONS_ID_1);

        startDynamicCache(ig0, DYNAMIC_CACHE_NAME_1, MIXED_CACHES_GROUP_NAME_0);

        startDynamicCache(ig0, DYNAMIC_CACHE_NAME_2, DYNAMIC_CACHES_GROUP_NAME_1);

        SHOULD_FAIL.set(true);

        IgniteEx ig1 = startGrid(NODE_CONS_ID_1);

        verifyCachesAndGroups(ig1);
    }

    /** */
    private void startDynamicCache(Ignite ig, String cacheName, String groupName) {
        ig.getOrCreateCache(new CacheConfiguration<>(cacheName)
            .setGroupName(groupName)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setNodeFilter(nodeFilter)
        );
    }

    /** */
    private void verifyCachesAndGroups(IgniteEx ig) {
        Map<String, DynamicCacheDescriptor> caches = ig.context().cache().cacheDescriptors();

        assertEquals(3, caches.size());
        assertTrue(caches.keySet().contains(GridCacheUtils.UTILITY_CACHE_NAME));
        assertTrue(caches.keySet().contains(STATIC_CACHE_NAME_0));
        assertTrue(caches.keySet().contains(DYNAMIC_CACHE_NAME_0));

        Map<Integer, CacheGroupDescriptor> groups = ig.context().cache().cacheGroupDescriptors();

        assertEquals(2, groups.size());

        boolean defaultGroupFound = false;
        boolean mixedCachesGroupFound = false;

        for (CacheGroupDescriptor grpDesc : groups.values()) {
            if (grpDesc.cacheOrGroupName().equals(GridCacheUtils.UTILITY_CACHE_NAME))
                defaultGroupFound = true;
            else if (grpDesc.cacheOrGroupName().equals(MIXED_CACHES_GROUP_NAME_0))
                mixedCachesGroupFound = true;
        }

        assertTrue(String.format("Default group found: %b, mixed group found: %b",
            defaultGroupFound,
            mixedCachesGroupFound),
            defaultGroupFound && mixedCachesGroupFound);
    }
}
