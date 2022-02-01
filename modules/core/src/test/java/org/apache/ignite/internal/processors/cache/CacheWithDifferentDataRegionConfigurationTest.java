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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Data regions validation test on joining node.
 */
public class CacheWithDifferentDataRegionConfigurationTest extends GridCommonAbstractTest {
    /** Node 1. */
    private static final int NODE_1 = 0;

    /** Node 2. */
    private static final int NODE_2 = 1;

    /** Node 3. */
    private static final int NODE_3 = 2;

    /** Region 1. */
    private static final String REGION_1 = "region_1";

    /** Region 2. */
    private static final String REGION_2 = "region_2";

    /** Region 3. */
    private static final String REGION_3 = "region_3";

    /** Region 4. */
    private static final String REGION_4 = "region_4";

    /** Cache 1. */
    private static final String CACHE_1 = "cache_1";

    /** Cache 2. */
    private static final String CACHE_2 = "cache_2";

    /** Persistence. */
    private static final boolean PERSISTENCE = true;

    /** Memory. */
    private static final boolean MEMORY = false;

    /**
     * @throws Exception If failed.
     */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    public void setUp() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void twoNodesHaveDifferentDefaultConfigurationUnacceptable() throws Exception {
        IgniteEx node1 = node(NODE_1)
            .withDefaultRegion("defaultName1", MEMORY)
            .andCache(CACHE_1)
            .start();

        node1.cluster().baselineAutoAdjustTimeout(1);

        assertThrowsContainsMessage(() -> node(NODE_2).withDefaultRegion("defaultName2", PERSISTENCE).andCache(CACHE_2).start(),
            IgniteSpiException.class,
            "Failed to join node (Incompatible data region configuration [region=DEFAULT"
        );
    }

    /**
     *
     */
    @Test
    public void twoNodesHaveCommonDefaultConfigurationAcceptable() throws Exception {
        IgniteEx node1 = node(NODE_1)
            .withDefaultRegion("defaultName1", PERSISTENCE)
            .andCache(CACHE_1)
            .start();

        IgniteEx node2 = node(NODE_2)
            .withDefaultRegion("defaultName2", PERSISTENCE)
            .andCache(CACHE_2)
            .start();

        node1.cluster().active(true);

        populateCache(node1, CACHE_1, 1000);
        populateCache(node2, CACHE_2, 350);

        assertThatCacheContains(node2, CACHE_1, 1000);
        assertThatCacheContains(node1, CACHE_2, 350);
    }

    /**
     *
     */
    @Test
    public void firstNodeHasDefaultAndSecondDefaultWithCustomNameAcceptable() throws Exception {
        IgniteEx node1 = node(NODE_1)
            .andCache(CACHE_1)
            .start();

        IgniteEx node2 = node(NODE_2)
            .withDefaultRegion("defaultName2", MEMORY)
            .andCache(CACHE_2)
            .start();

        node1.cluster().active(true);

        populateCache(node1, CACHE_1, 1000);
        populateCache(node2, CACHE_2, 350);

        assertThatCacheContains(node2, CACHE_1, 1000);
        assertThatCacheContains(node1, CACHE_2, 350);
    }

    /**
     *
     */
    @Test
    public void firstNodeHasDefaultAndSecondWithTwoRegionsDefaultAndPersistenceAcceptable() throws Exception {
        IgniteEx node1 = node(NODE_1)
            .andCache(CACHE_1)
            .start();

        node1.cluster().baselineAutoAdjustTimeout(1); //Hack: The way to add persistence cache into in-memory cluster

        IgniteEx node2 = node(NODE_2)
            .withRegion(REGION_1, MEMORY)
            .withRegion(REGION_2, PERSISTENCE)
            .andExclusiveCache(CACHE_2, REGION_2)
            .start();

        populateCache(node1, CACHE_1, 1000);
        populateCache(node2, CACHE_2, 350);

        assertThatCacheContains(node2, CACHE_1, 1000);
        assertThatCacheContains(node1, CACHE_2, 350);
    }

    /**
     *
     */
    @Test
    public void twoNodesHaveTwoNonOverlappingRegionsAcceptable() throws Exception {
        IgniteEx node1 = node(NODE_1)
            .withRegion(REGION_1, PERSISTENCE)
            .withRegion(REGION_2, MEMORY)
            .andExclusiveCache(CACHE_1, REGION_1)
            .start();

        IgniteEx node2 = node(NODE_2)
            .withRegion(REGION_3, MEMORY)
            .withRegion(REGION_4, PERSISTENCE)
            .andExclusiveCache(CACHE_2, REGION_4)
            .start();

        node1.cluster().active(true);

        populateCache(node1, CACHE_1, 1000);
        populateCache(node2, CACHE_2, 350);

        assertThatCacheContains(node2, CACHE_1, 1000);
        assertThatCacheContains(node1, CACHE_2, 350);
    }

    /**
     *
     */
    @Test
    public void twoNodesWithSameRegionsButDifferentPersistenceModeForThemUnacceptable() throws Exception {
        node(NODE_1)
            .withRegion(REGION_1, PERSISTENCE)
            .start();

        assertThrowsContainsMessage(() -> node(NODE_2).withRegion(REGION_1, MEMORY).start(),
            IgniteSpiException.class,
            "Failed to join node (Incompatible data region configuration [region=" + REGION_1
        );
    }

    /**
     *
     */
    @Test
    public void secondNodeMustRejectJoinOnThirdNode() throws Exception {
        IgniteEx node1 = node(NODE_1)
            .start();

        node1.cluster().baselineAutoAdjustTimeout(1); //Hack: The way to add persistence cache into in-memory cluster

        node(NODE_2)
            .withRegion(REGION_2, PERSISTENCE)
            .start();

        assertThrowsContainsMessage(() -> node(NODE_3).withRegion(REGION_2, MEMORY).start(),
            IgniteSpiException.class,
            "Failed to join node (Incompatible data region configuration [region=" + REGION_2
        );
    }

    /**
     * @param call Callable.
     * @param cls Class.
     * @param msg Message.
     */
    private void assertThrowsContainsMessage(Callable<?> call, Class<? extends Throwable> cls, String msg) {
        Throwable throwable = assertThrowsWithCause(call, cls);

        assertTrue("Message mismatch: " + msg, X.hasCause(throwable, msg, cls));
    }

    /**
     * @param node Node.
     * @param cacheName Cache name.
     * @param size Size.
     */
    private void assertThatCacheContains(IgniteEx node, String cacheName, int size) {
        IgniteCache<Integer, Integer> cache = node.getOrCreateCache(cacheName);

        for (int i = 0; i < size; i++)
            assertEquals((Integer)i, cache.get(i));
    }

    /**
     * @param node Node.
     * @param cacheName Cache name.
     * @param size Size.
     */
    public void populateCache(IgniteEx node, String cacheName, int size) {
        IgniteCache<Integer, Integer> cache = node.getOrCreateCache(cacheName);

        for (int i = 0; i < size; i++)
            cache.put(i, i);
    }

    /**
     * @param gridId Grid id.
     */
    private ConfigurationBuilder node(int gridId) {
        return new ConfigurationBuilder(gridId);
    }

    /**
     *
     */
    private static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /**
         *
         */
        private final String consistenceId;

        /**
         *
         */
        private NodeFilter(String consistenceId) {
            this.consistenceId = consistenceId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {

            return clusterNode.consistentId().equals(consistenceId);
        }
    }

    /**
     *
     */
    private class ConfigurationBuilder {
        /** Grid id. */
        private final String gridName;

        /** Regions. */
        private final List<DataRegionConfiguration> regions = new ArrayList<>();

        /** Caches. */
        private final List<CacheConfiguration> caches = new ArrayList<>();

        /** Default region configuration. */
        @Nullable private DataRegionConfiguration dfltRegionConfiguration;

        /**
         * @param gridId Grid id.
         */
        ConfigurationBuilder(int gridId) {
            this.gridName = getTestIgniteInstanceName(gridId);
        }

        /**
         * @param regionName Region name.
         * @param persistence Persistence.
         */
        ConfigurationBuilder withDefaultRegion(String regionName, boolean persistence) {
            dfltRegionConfiguration = new DataRegionConfiguration()
                .setName(regionName)
                .setInitialSize(100L * 1024 * 1024)
                .setMaxSize(500L * 1024 * 1024)
                .setPersistenceEnabled(persistence);

            return this;
        }

        /**
         * @param regionName Region name.
         * @param persistence Persistence.
         */
        ConfigurationBuilder withRegion(String regionName, boolean persistence) {
            regions.add(new DataRegionConfiguration()
                .setName(regionName)
                .setInitialSize(100L * 1024 * 1024)
                .setMaxSize(500L * 1024 * 1024)
                .setPersistenceEnabled(persistence)
            );

            return this;
        }

        /**
         * @param cacheName Cache name.
         */
        ConfigurationBuilder andCache(String cacheName) {
            return andCache(cacheName, null);
        }

        /**
         * @param cacheName Cache name.
         * @param regionName Region name.
         */
        ConfigurationBuilder andCache(String cacheName, String regionName) {
            caches.add(new CacheConfiguration().setDataRegionName(regionName).setName(cacheName));

            return this;
        }

        /**
         * This cache related with node via node filter.
         *
         * @param cacheName Cache name.
         * @param regionName Region name.
         */
        ConfigurationBuilder andExclusiveCache(String cacheName, String regionName) {
            caches.add(new CacheConfiguration()
                .setNodeFilter(new NodeFilter(gridName))
                .setDataRegionName(regionName)
                .setName(cacheName)
            );

            return this;
        }

        /** Start node from builder */
        public IgniteEx start() throws Exception {
            IgniteConfiguration cfg = getConfiguration(gridName);

            cfg.setConsistentId(gridName);

            DataStorageConfiguration storageCfg = new DataStorageConfiguration();
            storageCfg.setDataRegionConfigurations(regions.toArray(new DataRegionConfiguration[regions.size()]));
            cfg.setDataStorageConfiguration(storageCfg);

            if (dfltRegionConfiguration != null)
                storageCfg.setDefaultDataRegionConfiguration(dfltRegionConfiguration);

            cfg.setCacheConfiguration(caches.toArray(new CacheConfiguration[caches.size()]));

            return startGrid(cfg);
        }
    }
}
