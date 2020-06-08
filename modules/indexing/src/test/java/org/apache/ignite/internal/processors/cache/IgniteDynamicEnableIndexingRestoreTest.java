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

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *  Tests different scenarious to ensure that enabling indexing on persistence CACHE
 *  correctly persisted and validated on topology change.
 */
public class IgniteDynamicEnableIndexingRestoreTest extends GridCommonAbstractTest {
    /** */
    public static final String POI_CACHE_NAME = "poi";

    /** */
    public static final String POI_SCHEMA_NAME = "DOMAIN";

    /** */
    public static final String WRONG_SCHEMA_NAME = "DOMAIN_1";

    /** */
    public static final String POI_TABLE_NAME = "POI";

    /** */
    public static final String POI_CLASS_NAME = "PointOfInterest";

    /** */
    public static final String ID_FIELD_NAME = "id";

    /** */
    public static final String NAME_FIELD_NAME = "name";

    /** */
    public static final String PK_INDEX_NAME = "_key_pk";

    /** */
    public static final String LATITUDE_FIELD_NAME = "latitude";

    /** */
    public static final String LONGITUDE_FIELD_NAME = "longitude";

    /** */
    public static final int NUM_ENTRIES = 500;

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
            assertThat(X.cause(e, IgniteSpiException.class).getMessage(), containsString("Failed to join node to the active cluster"));
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
        createTable(ig.cache(POI_CACHE_NAME), WRONG_SCHEMA_NAME);

        try {
            startGrid(0);

            fail("Node should start with fail");
        }
        catch (Exception e) {
            assertThat(X.cause(e, IgniteSpiException.class).getMessage(), containsString("Failed to join node to the active cluster"));
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

        fillTestData(srv0);

        stopGrid(1);

        createTable(cache, POI_SCHEMA_NAME);

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

            fillTestData(ig);

            stopGrid(1);

            createTable(cache, POI_SCHEMA_NAME);

            performQueryingIntegrityCheck(ig);

            stopAllGrids();
    }

    /** */
    private void performQueryingIntegrityCheck(IgniteEx ig) throws Exception {
        IgniteCache<Object, Object> cache = ig.getOrCreateCache(POI_CACHE_NAME).withKeepBinary();

        List<List<?>> res = cache.query(new SqlFieldsQuery(String.format("SELECT * FROM %s", POI_TABLE_NAME))
            .setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals(NUM_ENTRIES, res.size());

        cache.query(new SqlFieldsQuery(
                    String.format("DELETE FROM %s WHERE %s = %s", POI_TABLE_NAME, ID_FIELD_NAME,  "100")
                ).setSchema(POI_SCHEMA_NAME)).getAll();

        assertNull(cache.get(100));

        cache.query(new SqlFieldsQuery(
            String.format(
                "INSERT INTO %s(%s) VALUES (%s)",
                POI_TABLE_NAME,
                String.join(",", ID_FIELD_NAME, NAME_FIELD_NAME),
                String.join(",", "100","'test'"))
        ).setSchema(POI_SCHEMA_NAME)).getAll();

        assertNotNull(cache.get(100));

        cache.query(new SqlFieldsQuery(String.format("UPDATE %s SET %s = '%s' WHERE %s = 100",
            POI_TABLE_NAME, NAME_FIELD_NAME, "POI_100", ID_FIELD_NAME)).setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals("POI_100", ((BinaryObject)cache.get(100)).field(NAME_FIELD_NAME));

        assertIndexUsed(cache, "SELECT * FROM " + POI_TABLE_NAME + " WHERE " + ID_FIELD_NAME + " = 10", PK_INDEX_NAME);
    }

    /**
     * Fill cache with test data.
     */
    private void fillTestData(Ignite ig) {
        try (IgniteDataStreamer<? super Object, ? super Object> s = ig.dataStreamer(POI_CACHE_NAME)) {
            Random rnd = ThreadLocalRandom.current();

            for (int i = 0; i < NUM_ENTRIES; i++) {
                BinaryObject bo = ig.binary().builder(POI_CLASS_NAME)
                    .setField(NAME_FIELD_NAME, "POI_" + i, String.class)
                    .setField(LATITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .setField(LONGITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .build();

                s.addData(i, bo);
            }
        }
    }

    /** */
    private void createTable(IgniteCache<?, ?> cache, String schemaName) {
        cache.query(new SqlFieldsQuery(
                String.format("CREATE TABLE %s.%s " +
                                "(%s INT, %s VARCHAR," +
                                " %s DOUBLE PRECISION," +
                                " %s DOUBLE PRECISION," +
                                " PRIMARY KEY (%s)" +
                                ") WITH " +
                                " \"CACHE_NAME=%s,VALUE_TYPE=%s\"",
                        schemaName, POI_TABLE_NAME, ID_FIELD_NAME, NAME_FIELD_NAME,
                        LATITUDE_FIELD_NAME, LONGITUDE_FIELD_NAME, ID_FIELD_NAME,
                        POI_CACHE_NAME, POI_CLASS_NAME)
        ));
    }

    /** */
    private void assertIndexUsed(IgniteCache<?, ?> cache, String sql, String idx)
        throws IgniteCheckedException {
        AtomicReference<String> currPlan = new AtomicReference<>();

        boolean res = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                String plan = explainPlan(cache, sql);

                currPlan.set(plan);

                return plan.contains(idx);
            }
        }, 10_000);

        assertTrue("Query \"" + sql + "\" executed without usage of " + idx + ", see plan:\n\"" +
            currPlan.get() + "\"", res);
    }

    /** */
    private String explainPlan(IgniteCache<?, ?> cache, String sql) {
        return cache.query(new SqlFieldsQuery("EXPLAIN " + sql).setSchema(POI_SCHEMA_NAME))
            .getAll().get(0).get(0).toString().toLowerCase();
    }

    /** */
    private CacheConfiguration<?, ?> testCacheConfiguration(String name) {
        return new CacheConfiguration<>(name)
                    //Set transactional because of https://issues.apache.org/jira/browse/IGNITE-5564.
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setCacheMode(CacheMode.REPLICATED)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }
}
