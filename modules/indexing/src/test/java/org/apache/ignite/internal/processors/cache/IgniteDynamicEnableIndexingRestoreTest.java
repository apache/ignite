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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.DynamicEnableIndexingAbstractTest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *  Tests different scenarious to ensure that enabling indexing on persistence CACHE
 *  correctly persisted and validated on topology change.
 */
public class IgniteDynamicEnableIndexingRestoreTest extends DynamicEnableIndexingAbstractTest {
    /** */
    private static final String WRONG_SCHEMA_NAME = "DOMAIN_1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);
        cfg.setConsistentId(gridName);
        cfg.setSqlSchemas(POI_SCHEMA_NAME, WRONG_SCHEMA_NAME);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMergeCacheConfig_StartWithInitialCoordinator() throws Exception {
        testMergeCacheConfig(0, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMergeCacheConfig_StartWithInitialSecondNode() throws Exception {
        testMergeCacheConfig(1, 0);
    }

    /**
     * @param firstIdx Index of first starting node after cluster stopping.
     * @param secondIdx Index of second starting node after cluster stopping.
     */
    private void testMergeCacheConfig(int firstIdx, int secondIdx) throws Exception {
        prepareTestGrid();

        // Check when start from firstIdx node.
        IgniteEx ig = startGrid(firstIdx);

        startGrid(secondIdx);

        ig.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        performQueryingIntegrityCheck(ig);

        // Restart and start from the beginning.
        stopAllGrids();

        ig = startGrids(2);

        ig.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        performQueryingIntegrityCheck(ig);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testFailJoiningNodeBecauseNeedConfigUpdateOnActiveGrid() throws Exception {
        prepareTestGrid();

        IgniteEx ig = startGrid(1);
        ig.cluster().state(ClusterState.ACTIVE);

        try {
            startGrid(0);

            fail("Node should start with fail");
        }
        catch (Exception e) {
            assertThat(X.cause(e, IgniteSpiException.class).getMessage(),
                containsString("the config of the cache 'poi' has to be merged which is impossible on active grid"));
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testFailJoiningNodeDifferentSchemasOnDynamicIndexes() throws Exception {
        prepareTestGrid();

        IgniteEx ig = startGrid(1);
        ig.cluster().state(ClusterState.ACTIVE);

        // Enable indexing with different schema name.
        createTable(ig.cache(POI_CACHE_NAME), WRONG_SCHEMA_NAME, CacheConfiguration.DFLT_QUERY_PARALLELISM);

        try {
            startGrid(0);

            fail("Node should start with fail");
        }
        catch (Exception e) {
            assertThat(X.cause(e, IgniteSpiException.class).getMessage(),
                containsString("schema 'DOMAIN' from joining node differs to 'DOMAIN_1'"));
        }
    }

    /**
     * Check that client reconnects to restarted grid. Start grid from node with enabled indexing.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testReconnectClient_RestartFromNodeWithEnabledIndexing() throws Exception {
        testReconnectClient(true);
    }

    /**
     *  Check that client reconnects to restarted grid. Start grid from node that stopped before enabled indexing.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testReconnectClient_RestartFromNodeWithDisabledIndexing() throws Exception {
        testReconnectClient(false);
    }

    /**
     * @param startFromEnabledIndexing If @{code true}, start grid from node with enabled indexing.
     *
     * @throws Exception if failed.
     */
    private void testReconnectClient(boolean startFromEnabledIndexing) throws Exception {
        IgniteEx srv0 = startGrids(2);

        IgniteEx cli = startClientGrid(2);

        cli.cluster().state(ClusterState.ACTIVE);

        IgniteCache<?, ?> cache = srv0.createCache(testCacheConfiguration(POI_CACHE_NAME));

        loadData(srv0, 0, NUM_ENTRIES);

        stopGrid(1);

        createTable(cache, POI_SCHEMA_NAME, CacheConfiguration.DFLT_QUERY_PARALLELISM);

        performQueryingIntegrityCheck(srv0);

        IgniteClientReconnectAbstractTest.reconnectClientNode(log, cli, srv0, () -> {
            try {
                stopGrid(0);

                if (startFromEnabledIndexing)
                    startGrid(0);
                else
                    startGrid(1);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to restart cluster", e);
            }
        });

        assertEquals(2, cli.cluster().nodes().size());
        cli.cluster().state(ClusterState.ACTIVE);

        if (startFromEnabledIndexing) {
            awaitPartitionMapExchange();

            performQueryingIntegrityCheck(cli);
        }
        else
            assertEquals(NUM_ENTRIES, cli.getOrCreateCache(POI_CACHE_NAME).size(CachePeekMode.PRIMARY));
    }

    /**
     * Prepare test grid:
     * 1) Start two nodes, start cache and fill it with data.
     * 2) Stop second node.
     * 3) Enable indexing on cache on first node.
     * 4) Stop cluster.
     */
    private void prepareTestGrid() throws Exception {
        IgniteEx ig = startGrids(2);

        ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<?, ?> cache = ig.createCache(testCacheConfiguration(POI_CACHE_NAME));

        loadData(ig, 0, NUM_ENTRIES);

        stopGrid(1);

        createTable(cache, POI_SCHEMA_NAME, CacheConfiguration.DFLT_QUERY_PARALLELISM);

        performQueryingIntegrityCheck(ig);

        stopAllGrids();
    }

    /** */
    private CacheConfiguration<?, ?> testCacheConfiguration(String name) {
        //Set transactional because of https://issues.apache.org/jira/browse/IGNITE-5564.
        return new CacheConfiguration<>(name)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }
}
