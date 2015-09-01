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

package org.apache.ignite.cache.affinity;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class IgniteClientNodeAffinityTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODE_CNT = 4;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /** */
    private static final String CACHE4 = "cache4";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (gridName.equals(getTestGridName(NODE_CNT - 1)))
            cfg.setClientMode(true);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setBackups(1);
        ccfg1.setName(CACHE1);
        ccfg1.setAffinity(new RendezvousAffinityFunction());
        ccfg1.setNodeFilter(new TestNodesFilter());

        CacheConfiguration ccfg2 = new CacheConfiguration();

        ccfg2.setBackups(1);
        ccfg2.setName(CACHE2);
        ccfg2.setAffinity(new RendezvousAffinityFunction());

        CacheConfiguration ccfg3 = new CacheConfiguration();

        ccfg3.setBackups(1);
        ccfg3.setName(CACHE3);
        ccfg3.setAffinity(new FairAffinityFunction());
        ccfg3.setNodeFilter(new TestNodesFilter());

        CacheConfiguration ccfg4 = new CacheConfiguration();

        ccfg4.setCacheMode(REPLICATED);
        ccfg4.setName(CACHE4);
        ccfg4.setNodeFilter(new TestNodesFilter());

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3, ccfg4);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeNotInAffinity() throws Exception {
        checkCache(CACHE1, 2);

        checkCache(CACHE2, 2);

        checkCache(CACHE3, 2);

        checkCache(CACHE4, 3);

        Ignite client = ignite(NODE_CNT - 1);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setBackups(0);

        ccfg.setNodeFilter(new TestNodesFilter());

        IgniteCache<Integer, Integer> cache = client.createCache(ccfg);

        try {
            checkCache(null, 1);
        }
        finally {
            cache.destroy();
        }

        cache = client.createCache(ccfg, new NearCacheConfiguration());

        try {
            checkCache(null, 1);
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param expNodes Expected number of nodes per partition.
     */
    private void checkCache(String cacheName, int expNodes) {
        log.info("Test cache: " + cacheName);

        Ignite client = ignite(NODE_CNT - 1);

        assertTrue(client.configuration().isClientMode());

        ClusterNode clientNode = client.cluster().localNode();

        for (int i = 0; i < NODE_CNT; i++) {
            Ignite ignite = ignite(i);

            Affinity<Integer> aff = ignite.affinity(cacheName);

            for (int part = 0; part < aff.partitions(); part++) {
                Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(part);

                assertEquals(expNodes, nodes.size());

                assertFalse(nodes.contains(clientNode));
            }
        }
    }

    /**
     *
     */
    private static class TestNodesFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            Boolean attr = clusterNode.attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE);

            assertNotNull(attr);

            assertFalse(attr);

            return true;
        }
    }
}