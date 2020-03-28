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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class IgniteDiscoDataHandlingInNewClusterTest extends GridCommonAbstractTest {
    /** */
    private static final String NODE_1_CONS_ID = "node01";

    /** */
    private static final String NODE_2_CONS_ID = "node02";

    /** */
    private static final String NODE_3_CONS_ID = "node03";

    /** */
    private static final String NODE_4_DIFF_STATIC_CONFIG = "node04_diff_config";

    /** */
    private static final String CLIENT_WITH_DIFF_CONFIG = "clientWithDiffConfig";

    /** */
    private static final String STATIC_CACHE_NAME_1 = "staticCache1";

    /** */
    private static final String STATIC_CACHE_NAME_2 = "staticCache2";

    /** */
    private static final String STATIC_CACHE_NAME_3 = "staticCache3";

    /** */
    private static final String DYNAMIC_CACHE_NAME_1 = "dynamicCache1";

    /** */
    private static final String DYNAMIC_CACHE_NAME_2 = "dynamicCache2";

    /** */
    private static final String DYNAMIC_CACHE_NAME_3 = "dynamicCache3";

    /** Group where static and dynamic caches reside. */
    private static final String GROUP_WITH_STATIC_CACHES = "group1";

    /** Group where only dynamic caches reside. */
    private static final String DYNAMIC_CACHES_GROUP_WITH_FILTER = "group2";

    /** Group where only dynamic caches reside. */
    private static final String DYNAMIC_CACHES_GROUP_WITHOUT_FILTER = "group3";

    /** Node filter to pin dynamic caches to a specific node. */
    private static final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
        @Override public boolean apply(ClusterNode node) {
            return node.consistentId().toString().contains(NODE_1_CONS_ID);
        }
    };

    /** Discovery SPI aimed to fail node with it when another server node joins the topology. */
    private TcpDiscoverySpi failingOnNodeJoinSpi = new TcpDiscoverySpi() {
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                super.startMessageProcess(msg);

                throw new RuntimeException("Simulation of failure of node " + NODE_1_CONS_ID);
            }

            super.startMessageProcess(msg);
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.contains(NODE_1_CONS_ID)) {
            failingOnNodeJoinSpi.setIpFinder(sharedStaticIpFinder);
            failingOnNodeJoinSpi.setJoinTimeout(60_000);

            cfg.setDiscoverySpi(failingOnNodeJoinSpi);
        }

        cfg.setConsistentId(igniteInstanceName);

        if (!igniteInstanceName.contains("client")) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setInitialSize(10500000)
                            .setMaxSize(6659883008L)
                            .setPersistenceEnabled(false)
                    )
            );
        }

        if (igniteInstanceName.equals(NODE_4_DIFF_STATIC_CONFIG) || igniteInstanceName.equals(CLIENT_WITH_DIFF_CONFIG))
            cfg.setCacheConfiguration(
                prepareStaticCacheCfg(STATIC_CACHE_NAME_1),
                prepareStaticCacheCfg(STATIC_CACHE_NAME_2)
            );
        else
            cfg.setCacheConfiguration(
                prepareStaticCacheCfg(STATIC_CACHE_NAME_1),
                prepareStaticCacheCfg(STATIC_CACHE_NAME_3)
            );

        return cfg;
    }

    /** */
    private CacheConfiguration prepareStaticCacheCfg(String cacheName) {
        return new CacheConfiguration(cacheName)
            .setGroupName(GROUP_WITH_STATIC_CACHES)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setNodeFilter(nodeFilter);
    }

    /**
     * Verifies that new node received discovery data from stopped grid filters it out
     * from GridCacheProcessor and GridDiscoveryManager internal structures.
     *
     * All subsequent servers and clients join topology successfully.
     *
     * See related ticket <a href="https://issues.apache.org/jira/browse/IGNITE-10878">IGNITE-10878</a>.
     */
    @Test
    public void testNewClusterIgnoresDiscoDataFromStopped_SameStaticConfig() throws Exception {
        IgniteEx ig0 = startGrid(NODE_1_CONS_ID);

        prepareDynamicCaches(ig0);

        IgniteEx ig1 = startGrid(NODE_2_CONS_ID);

        verifyCachesAndGroups(ig1, Arrays.asList(new String[] {STATIC_CACHE_NAME_1, STATIC_CACHE_NAME_3}));

        IgniteEx ig2 = startGrid(NODE_3_CONS_ID);

        verifyCachesAndGroups(ig2, Arrays.asList(new String[] {STATIC_CACHE_NAME_1, STATIC_CACHE_NAME_3}));

        IgniteEx client = startClientGrid("client01");

        verifyCachesAndGroups(client, Arrays.asList(new String[] {STATIC_CACHE_NAME_1, STATIC_CACHE_NAME_3}));
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testNewClusterIgnoresDiscoDataFromStopped_DifferentStaticConfig() throws Exception {
        IgniteEx ig0 = startGrid(NODE_1_CONS_ID);

        prepareDynamicCaches(ig0);

        IgniteEx ig1 = startGrid(NODE_4_DIFF_STATIC_CONFIG);

        verifyCachesAndGroups(ig1, Arrays.asList(new String[] {STATIC_CACHE_NAME_1, STATIC_CACHE_NAME_2}));

        IgniteEx client = startClientGrid(CLIENT_WITH_DIFF_CONFIG);

        verifyCachesAndGroups(client, Arrays.asList(new String[] {STATIC_CACHE_NAME_1, STATIC_CACHE_NAME_2}));
    }

    /** */
    private void verifyCachesAndGroups(IgniteEx ignite, Collection<String> cacheNames) {
        Map<String, DynamicCacheDescriptor> caches = ignite.context().cache().cacheDescriptors();

        assertEquals(cacheNames.size() + 1, caches.size());
        assertTrue(caches.keySet().contains(GridCacheUtils.UTILITY_CACHE_NAME));
        assertTrue(caches.keySet().containsAll(cacheNames));

        Map<Integer, CacheGroupDescriptor> groups = ignite.context().cache().cacheGroupDescriptors();

        assertEquals(2, groups.size());

        boolean defaultGroupFound = false;
        boolean staticCachesGroupFound = false;

        for (CacheGroupDescriptor grpDesc : groups.values()) {
            if (grpDesc.cacheOrGroupName().equals(GridCacheUtils.UTILITY_CACHE_NAME))
                defaultGroupFound = true;
            else if (grpDesc.cacheOrGroupName().equals(GROUP_WITH_STATIC_CACHES))
                staticCachesGroupFound = true;
        }

        assertTrue(String.format("Default group found: %b, static group found: %b",
            defaultGroupFound,
            staticCachesGroupFound),
            defaultGroupFound && staticCachesGroupFound);
    }

    /** */
    private void prepareDynamicCaches(IgniteEx ig) {
        ig.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME_1)
            .setGroupName(GROUP_WITH_STATIC_CACHES)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setNodeFilter(nodeFilter)
        );

        ig.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME_2)
            .setGroupName(DYNAMIC_CACHES_GROUP_WITH_FILTER)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setNodeFilter((IgnitePredicate<ClusterNode>)node -> node.consistentId().toString().contains(NODE_1_CONS_ID))
        );

        ig.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME_3)
            .setGroupName(DYNAMIC_CACHES_GROUP_WITHOUT_FILTER)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
        );
    }

    /**
     * Turns off printing stack trace on detecting critical failure to speed up tests.
     */
    @BeforeClass
    public static void setUpClass() {
        System.setProperty(IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, "false");
    }

    /**
     * Restoring default value for printing stack trace setting.
     */
    @AfterClass
    public static void tearDownClass() {
        System.setProperty(IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, "true");
    }

    /** {@inheritDoc} */
    @After
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

}
