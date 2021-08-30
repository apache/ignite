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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base class for enable indexing tests.
 */
public class DynamicEnableIndexingAbstractTest extends GridCommonAbstractTest {
    /** Node index for regular server (coordinator). */
    protected static final int IDX_SRV_CRD = 0;

    /** Node index for regular server (not coordinator). */
    protected static final int IDX_SRV_NON_CRD = 1;

    /** Node index for regular client. */
    protected static final int IDX_CLI = 2;

    /** Node index for server which doesn't pass node filter. */
    protected static final int IDX_SRV_FILTERED = 3;

    /** Node index for client with near-only cache. */
    protected static final int IDX_CLI_NEAR_ONLY = 4;

    /** Attribute to filter node out of cache data nodes. */
    protected static final String ATTR_FILTERED = "FILTERED";

    /** */
    protected static final String POI_CACHE_NAME = "poi";

    /** */
    protected static final int NUM_ENTRIES = 1000;

    /** */
    protected static final String POI_SCHEMA_NAME = "DOMAIN";

    /** */
    protected static final String POI_TABLE_NAME = "POI";

    /** */
    protected static final String POI_CLASS_NAME = "PointOfInterest";

    /** */
    protected static final String ID_FIELD_NAME = "id";

    /** */
    protected static final String NAME_FIELD_NAME = "name";

    /** */
    protected static final String KEY_PK_IDX_NAME = "_key_PK";

    /** */
    protected static final String LATITUDE_FIELD_NAME = "latitude";

    /** */
    protected static final String LONGITUDE_FIELD_NAME = "longitude";

    /** */
    protected static final String SELECT_ALL_QUERY = String.format("SELECT * FROM %s", POI_TABLE_NAME);

    /** */
    protected static final int QUERY_PARALLELISM = 4;

    /** */
    protected void createTable(IgniteCache<?, ?> cache, int qryParallelism) {
        createTable(cache, POI_SCHEMA_NAME, qryParallelism);
    }

    /** */
    protected void createTable(IgniteCache<?, ?> cache, String schemaName, int qryParallelism) {
        String sql = String.format(
            "CREATE TABLE %s.%s " +
            "(%s INT, %s VARCHAR," +
            " %s DOUBLE PRECISION," +
            " %s DOUBLE PRECISION," +
            " PRIMARY KEY (%s)" +
            ") WITH " +
            " \"CACHE_NAME=%s,VALUE_TYPE=%s,PARALLELISM=%d\"",
            schemaName, POI_TABLE_NAME, ID_FIELD_NAME, NAME_FIELD_NAME, LATITUDE_FIELD_NAME, LONGITUDE_FIELD_NAME,
            ID_FIELD_NAME, POI_CACHE_NAME, POI_CLASS_NAME, qryParallelism);

        cache.query(new SqlFieldsQuery(sql));
    }

    /** */
    protected List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
            serverConfiguration(IDX_SRV_CRD),
            serverConfiguration(IDX_SRV_NON_CRD),
            clientConfiguration(IDX_CLI),
            serverConfiguration(IDX_SRV_FILTERED, true),
            clientConfiguration(IDX_CLI_NEAR_ONLY)
        );
    }

    /** */
    protected IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /** */
    protected IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return serverConfiguration(idx, false);
    }

    /** */
    protected IgniteConfiguration serverConfiguration(int idx, boolean filter) throws Exception {
        IgniteConfiguration cfg = commonConfiguration(idx);

        if (filter)
            cfg.setUserAttributes(Collections.singletonMap(ATTR_FILTERED, true));

        return cfg;
    }

    /** */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
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
    protected CacheConfiguration<?, ?> testCacheConfiguration(
        String name,
        CacheMode mode,
        CacheAtomicityMode atomicityMode
    ) {
        return new CacheConfiguration<>(name)
            .setNodeFilter(new DynamicEnableIndexingBasicSelfTest.NodeFilter())
            .setAtomicityMode(atomicityMode)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(mode);
    }

    /** */
    protected void loadData(IgniteEx node, int start, int end) {
        try (IgniteDataStreamer<Object, Object> streamer = node.dataStreamer(POI_CACHE_NAME)) {
            Random rnd = ThreadLocalRandom.current();

            for (int i = start; i < end; i++) {
                BinaryObject bo = node.binary().builder(POI_CLASS_NAME)
                    .setField(NAME_FIELD_NAME, "POI_" + i, String.class)
                    .setField(LATITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .setField(LONGITUDE_FIELD_NAME, rnd.nextDouble(), Double.class)
                    .build();

                streamer.addData(i, bo);
            }
        }
    }

    /** */
    protected void performQueryingIntegrityCheck(Ignite ig) throws Exception {
        performQueryingIntegrityCheck(ig, 100);
    }

    /** */
    protected List<List<?>> query(Ignite ig, String sql) throws Exception {
        IgniteCache<Object, Object> cache = ig.cache(POI_CACHE_NAME).withKeepBinary();

        return cache.query(new SqlFieldsQuery(sql).setSchema(POI_SCHEMA_NAME)).getAll();
    }

    /** */
    protected void performQueryingIntegrityCheck(Ignite ig, int key) throws Exception {
        IgniteCache<Object, Object> cache = ig.cache(POI_CACHE_NAME).withKeepBinary();

        String sql = String.format("DELETE FROM %s WHERE %s = %d", POI_TABLE_NAME, ID_FIELD_NAME, key);
        List<List<?>> res = cache.query(new SqlFieldsQuery(sql).setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals(1, res.size());
        assertNull(cache.get(key));

        sql = String.format("INSERT INTO %s(%s) VALUES (%s)", POI_TABLE_NAME,
            String.join(",", ID_FIELD_NAME, NAME_FIELD_NAME), String.join(",", String.valueOf(key), "'test'"));

        res = cache.query(new SqlFieldsQuery(sql).setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals(1, res.size());
        assertNotNull(cache.get(key));

        sql = String.format("UPDATE %s SET %s = '%s' WHERE ID = %d", POI_TABLE_NAME, NAME_FIELD_NAME, "POI_" + key, key);
        res = cache.query(new SqlFieldsQuery(sql).setSchema(POI_SCHEMA_NAME)).getAll();

        assertEquals(1, res.size());
        assertEquals("POI_" + key, ((BinaryObject)cache.get(key)).field(NAME_FIELD_NAME));

        assertIndexUsed(cache, "SELECT * FROM " + POI_TABLE_NAME + " WHERE ID = " + key, KEY_PK_IDX_NAME);
    }

    /** */
    protected String explainPlan(IgniteCache<?, ?> cache, String sql) {
        return cache.query(new SqlFieldsQuery("EXPLAIN " + sql).setSchema(POI_SCHEMA_NAME))
                .getAll().get(0).get(0).toString().toLowerCase();
    }

    /** */
    protected void assertIndexUsed(IgniteCache<?, ?> cache, String sql, String idx) throws IgniteCheckedException {
        AtomicReference<String> currPlan = new AtomicReference<>();

        boolean res = GridTestUtils.waitForCondition(() -> {
            String plan = explainPlan(cache, sql);

            currPlan.set(plan);

            return plan.contains(idx.toLowerCase());
        }, 1_000);

        assertTrue("Query \"" + sql + "\" executed without usage of " + idx + ", see plan:\n\"" +
                currPlan.get() + "\"", res);
    }

    /** */
    protected void checkQueryParallelism(IgniteEx ig, CacheMode cacheMode) {
        int expectedParallelism = cacheMode != CacheMode.REPLICATED ? QUERY_PARALLELISM :
                CacheConfiguration.DFLT_QUERY_PARALLELISM;

        IgniteH2Indexing indexing = (IgniteH2Indexing)ig.context().query().getIndexing();

        SchemaManager schemaMgr = indexing.schemaManager();

        H2TableDescriptor descr = schemaMgr.tableForType(POI_SCHEMA_NAME, POI_CACHE_NAME, POI_CLASS_NAME);

        assertNotNull(descr);

        if (descr.table().getIndex(KEY_PK_IDX_NAME) instanceof H2TreeIndex) {
            H2TreeIndex pkIdx = (H2TreeIndex)descr.table().getIndex(KEY_PK_IDX_NAME);

            assertNotNull(pkIdx);

            assertEquals(expectedParallelism, pkIdx.segmentsCount());
        }

        CacheConfiguration<?, ?> cfg = ig.context().cache().cacheConfiguration(POI_CACHE_NAME);

        assertEquals(expectedParallelism, cfg.getQueryParallelism());
    }

    /** */
    protected static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(ATTR_FILTERED) == null;
        }
    }
}
