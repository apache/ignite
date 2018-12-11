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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
@RunWith(JUnit4.class)
public class AffinityClientNodeSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODE_CNT = 4;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE4 = "cache4";

    /** */
    private static final String CACHE5 = "cache5";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration ccfg1 = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg1.setBackups(1);
        ccfg1.setName(CACHE1);
        ccfg1.setAffinity(new RendezvousAffinityFunction());
        ccfg1.setNodeFilter(new TestNodesFilter());

        CacheConfiguration ccfg2 = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg2.setBackups(1);
        ccfg2.setName(CACHE2);
        ccfg2.setAffinity(new RendezvousAffinityFunction());

        CacheConfiguration ccfg4 = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg4.setCacheMode(REPLICATED);
        ccfg4.setName(CACHE4);
        ccfg4.setNodeFilter(new TestNodesFilter());

        CacheConfiguration ccfg5 = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg5.setBackups(1);
        ccfg5.setName(CACHE5);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(NODE_CNT - 1))) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration(ccfg5);
        }
        else
            cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg4);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODE_CNT - 1);

        startGrid(NODE_CNT - 1); // Start client after servers.
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeNotInAffinity() throws Exception {
        checkCache(CACHE1, 2);

        checkCache(CACHE2, 2);

        checkCache(CACHE4, 3);

        checkCache(CACHE5, 2);

        Ignite client = ignite(NODE_CNT - 1);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setBackups(0);

        ccfg.setNodeFilter(new TestNodesFilter());

        IgniteCache<Integer, Integer> cache = client.createCache(ccfg);

        try {
            checkCache(DEFAULT_CACHE_NAME, 1);
        }
        finally {
            cache.destroy();
        }

        cache = client.createCache(ccfg, new NearCacheConfiguration());

        try {
            checkCache(DEFAULT_CACHE_NAME, 1);
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

        Affinity<Object> aff0 = ignite(0).affinity(cacheName);

        Ignite client = ignite(NODE_CNT - 1);

        assertTrue(client.configuration().isClientMode());

        ClusterNode clientNode = client.cluster().localNode();

        for (int i = 0; i < NODE_CNT; i++) {
            Ignite ignite = ignite(i);

            Affinity<Object> aff = ignite.affinity(cacheName);

            for (int part = 0; part < aff.partitions(); part++) {
                Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(part);

                assertEquals(expNodes, nodes.size());
                assertEquals(aff0.mapPartitionToPrimaryAndBackups(part), nodes);

                assertFalse(nodes.contains(clientNode));

                assertEquals(aff0.partition(part), aff.partition(part));

                TestKey key = new TestKey(part, part + 1);

                assertEquals(aff0.partition(key), aff.partition(key));
                assertEquals(aff0.mapKeyToPrimaryAndBackups(key), aff.mapKeyToPrimaryAndBackups(key));
            }
        }
    }

    /**
     *
     */
    static class TestKey {
        /** */
        private int id;

        /** */
        @AffinityKeyMapped
        private int affId;

        /**
         * @param id ID.
         * @param affId Affinity ID.
         */
        public TestKey(int id, int affId) {
            this.id = id;
            this.affId = affId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey testKey = (TestKey)o;

            return id == testKey.id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
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
