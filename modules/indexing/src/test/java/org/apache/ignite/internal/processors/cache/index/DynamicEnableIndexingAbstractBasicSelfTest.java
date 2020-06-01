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

package org.apache.ignite.internal.processors.cache.index;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 *
 */
@RunWith(Parameterized.class)
public class DynamicEnableIndexingAbstractBasicSelfTest extends GridCommonAbstractTest {
    /** Cache modes. */
    @Parameters(name = "isNear={0},opNode={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[] {true, IDX_CLI},
                new Object[] {true, IDX_SRV_CRD},
                new Object[] {true, IDX_SRV_NON_CRD},
                new Object[] {true, IDX_SRV_FILTERED},
                new Object[] {false, IDX_CLI},
                new Object[] {false, IDX_SRV_CRD},
                new Object[] {false, IDX_SRV_NON_CRD},
                new Object[] {false, IDX_SRV_FILTERED}
        );
    }

    /** Node index for regular server (coordinator). */
    private static final int IDX_SRV_CRD = 0;

    /** Node index for regular server (not coordinator). */
    private static final int IDX_SRV_NON_CRD = 1;

    /** Node index for regular client. */
    private static final int IDX_CLI = 2;

    /** Node index for server which doesn't pass node filter. */
    private static final int IDX_SRV_FILTERED = 3;

    /** Node index for client with near-only cache. */
    private static final int IDX_CLI_NEAR_ONLY = 4;

    /** Attribute to filter node out of cache data nodes. */
    private static final String ATTR_FILTERED = "FILTERED";

    /** */
    private static final String POI_CACHE_NAME = "poi";

    /** */
    static final String POI_SCHEMA_NAME = "DOMAIN";

    /** */
    private static final String POI_TABLE_NAME = "POI";

    /** */
    private static final String POI_CLASS_NAME = "PointOfInterest";

    /** */
    private static final String ID_FIELD_NAME = "id";

    /** */
    private static final String NAME_FIELD_NAME = "name";

    /** */
    private static final String KEY_PK_IDX_NAME = "_key_pk";

    /** */
    private static final String LATITUDE_FIELD_NAME = "latitude";

    /** */
    private static final String LONGITUDE_FIELD_NAME = "longitude";

    /** */
    private static final int NUM_ENTRIES = 1000;

    /** */
    @Parameter(0)
    public boolean hasNear;

    /** */
    @Parameter(1)
    public int nodeIdx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTest();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);

        node().cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node().context().cache().dynamicDestroyCache(POI_CACHE_NAME, false, true, false, null).get();

        super.afterTest();
    }

    /** */
    @Test
    public void testAtomicPartitioned() throws Exception {
        enableDynamicIndexingTest(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC);
    }

    /** */
    @Test
    public void testAtomicReplicated() throws Exception {
        enableDynamicIndexingTest(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC);
    }

    /** */
    @Test
    public void testTransactionalPartitioned() throws Exception {
        enableDynamicIndexingTest(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    @Test
    public void testTransactionalReplicated() throws Exception {
        enableDynamicIndexingTest(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void enableDynamicIndexingTest(CacheMode mode, CacheAtomicityMode atomicityMode) throws Exception {
        CacheConfiguration<?, ?> ccfg = testCacheConfiguration(POI_CACHE_NAME, mode, atomicityMode);

        if (hasNear)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        node().context().cache().dynamicStartCache(ccfg, POI_CACHE_NAME, null, true, true, true).get();

        grid(IDX_CLI_NEAR_ONLY).getOrCreateNearCache(POI_CACHE_NAME, new NearCacheConfiguration<>());

        loadData(0, NUM_ENTRIES / 2);

        node().cache(POI_CACHE_NAME).query(new SqlFieldsQuery(
                String.format("CREATE TABLE %s.%s " +
                        "(%s INT, %s VARCHAR," +
                        " %s DOUBLE PRECISION," +
                        " %s DOUBLE PRECISION," +
                        " PRIMARY KEY (%s)" +
                        ") WITH " +
                        " \"CACHE_NAME=%s,VALUE_TYPE=%s\"",
                        POI_SCHEMA_NAME, POI_TABLE_NAME, ID_FIELD_NAME, NAME_FIELD_NAME,
                        LATITUDE_FIELD_NAME, LONGITUDE_FIELD_NAME, ID_FIELD_NAME,
                        POI_CACHE_NAME, POI_CLASS_NAME)
                ));

        grid(IDX_SRV_CRD).cache(POI_CACHE_NAME).indexReadyFuture().get();

        loadData(NUM_ENTRIES / 2, NUM_ENTRIES);

        for (Ignite ig: G.allGrids())
            performQueryingIntegrityCheck((IgniteEx)ig);
    }

    /** */
    private void loadData(int start, int end) {
        try (IgniteDataStreamer<Object, Object> streamer = node().dataStreamer(POI_CACHE_NAME)) {
            Random rnd = ThreadLocalRandom.current();

            for (int i = start; i < end; i++) {
                BinaryObject bo = node().binary().builder(POI_CLASS_NAME)
                        .setField(NAME_FIELD_NAME, "POI_" + i, String.class)
                        .setField(LATITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                        .setField(LONGITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                        .build();

                streamer.addData(i, bo);
            }
        }
    }

    /** */
    private void performQueryingIntegrityCheck(IgniteEx ig) throws Exception {
        IgniteCache<Object, Object> cache = ig.cache(POI_CACHE_NAME).withKeepBinary();

        List<List<?>> res = cache.query(new SqlFieldsQuery(String.format("SELECT * FROM %s", POI_TABLE_NAME))
                .setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals(NUM_ENTRIES, res.size());

        cache.query(new SqlFieldsQuery(String.format("DELETE FROM %s WHERE _key = %s", POI_TABLE_NAME, "100"))
                .setSchema(POI_SCHEMA_NAME)).getAll();

        assertNull(cache.get(100));

        cache.query(new SqlFieldsQuery(
                String.format(
                        "INSERT INTO %s(%s) VALUES (%s)",
                        POI_TABLE_NAME,
                        String.join(",", ID_FIELD_NAME, NAME_FIELD_NAME),
                        String.join(",", "100","'test'"))
        ).setSchema(POI_SCHEMA_NAME)).getAll();

        assertNotNull(cache.get(100));

        cache.query(new SqlFieldsQuery(String.format("UPDATE %s SET %s = '%s' WHERE ID = 100",
                POI_TABLE_NAME, NAME_FIELD_NAME, "POI_100")).setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals("POI_100", ((BinaryObject)cache.get(100)).field(NAME_FIELD_NAME));

        assertIndexUsed(cache, "SELECT * FROM " + POI_TABLE_NAME + " WHERE ID = 100", KEY_PK_IDX_NAME);
    }

    /** */
    private void assertIndexUsed(IgniteCache<?, ?> cache, String sql, String idx) throws IgniteCheckedException {
        AtomicReference<String> currPlan = new AtomicReference<>();

        boolean res = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                String plan = explainPlan(cache, sql);

                currPlan.set(plan);

                return plan.contains(idx);
            }
        }, 1_000);

        assertTrue("Query \"" + sql + "\" executed without usage of " + idx + ", see plan:\n\"" +
                currPlan.get() + "\"", res);
    }

    /** */
    private String explainPlan(IgniteCache<?, ?> cache, String sql) {
        return cache.query(new SqlFieldsQuery("EXPLAIN " + sql).setSchema(POI_SCHEMA_NAME))
                .getAll().get(0).get(0).toString().toLowerCase();
    }

    /** */
    private IgniteEx node() {
        return grid(nodeIdx);
    }

    /** */
    private List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
                serverConfiguration(IDX_SRV_CRD),
                serverConfiguration(IDX_SRV_NON_CRD),
                clientConfiguration(IDX_CLI),
                serverConfiguration(IDX_SRV_FILTERED, true),
                clientConfiguration(IDX_CLI_NEAR_ONLY)
        );
    }

    /** */
    private IgniteConfiguration clientConfiguration(int idx) throws Exception{
        return commonConfiguration(idx).setClientMode(true);
    }

    /** */
    private IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return serverConfiguration(idx, false);
    }

    /** */
    private IgniteConfiguration serverConfiguration(int idx, boolean filter) throws Exception {
        IgniteConfiguration cfg = commonConfiguration(idx);

        if (filter)
            cfg.setUserAttributes(Collections.singletonMap(ATTR_FILTERED, true));

        return cfg;
    }

    /** */
    private IgniteConfiguration commonConfiguration(int idx) throws Exception {
        String gridName = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(gridName);

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(128 * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setConsistentId(gridName);

        cfg.setSqlSchemas(POI_SCHEMA_NAME);

        return optimize(cfg);
    }

    /** */
    private CacheConfiguration<?, ?> testCacheConfiguration(
            String name,
            CacheMode mode,
            CacheAtomicityMode atomicityMode
    ) {
        return new CacheConfiguration<>(name)
                .setNodeFilter(new NodeFilter())
                .setAtomicityMode(atomicityMode)
                .setCacheMode(mode);
    }

    /** */
    private Collection<QueryEntity> queryEntities() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(ID_FIELD_NAME, Integer.class.getName());
        fields.put(NAME_FIELD_NAME, String.class.getName());
        fields.put(LATITUDE_FIELD_NAME, Double.class.getName());
        fields.put(LONGITUDE_FIELD_NAME, Double.class.getName());

        return Collections.singletonList(
                new QueryEntity()
                        .setKeyType(Integer.class.getName())
                        .setKeyFieldName(ID_FIELD_NAME)
                        .setValueType(POI_CLASS_NAME)
                        .setTableName(POI_TABLE_NAME)
                        .setFields(fields)
        );
    }

    /** */
    protected static class NodeFilter implements IgnitePredicate<ClusterNode>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(ATTR_FILTERED) == null;
        }
    }
}
