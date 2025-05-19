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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.deleteIndexBin;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Cache group JMX metrics test.
 */
public class CacheGroupMetricsWithIndexTest extends CacheGroupMetricsTest {
    /** */
    private static final String GROUP_NAME = "group1";

    /** */
    private static final String CACHE_NAME = "cache1";

    /** */
    private static final String OBJECT_NAME = "MyObject";

    /** */
    private static final String TABLE = "\"" + CACHE_NAME + "\"." + OBJECT_NAME;

    /** */
    private static final String KEY_NAME = "id";

    /** */
    private static final String COLUMN1_NAME = "col1";

    /** */
    private static final String COLUMN2_NAME = "col2";

    /** */
    private static final String COLUMN3_NAME = "col3";

    /** */
    private static final String INDEX_NAME = "testindex001";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        for (CacheConfiguration cacheCfg : cfg.getCacheConfiguration()) {
            if (GROUP_NAME.equals(cacheCfg.getGroupName()) && CACHE_NAME.equals(cacheCfg.getName())) {
                QueryEntity qryEntity = new QueryEntity(Long.class.getCanonicalName(), OBJECT_NAME);

                qryEntity.setKeyFieldName(KEY_NAME);

                LinkedHashMap<String, String> fields = new LinkedHashMap<>();

                fields.put(KEY_NAME, Long.class.getCanonicalName());

                fields.put(COLUMN1_NAME, Integer.class.getCanonicalName());

                fields.put(COLUMN2_NAME, String.class.getCanonicalName());

                qryEntity.setFields(fields);

                ArrayList<QueryIndex> indexes = new ArrayList<>();

                indexes.add(new QueryIndex(COLUMN1_NAME));

                indexes.add(new QueryIndex(COLUMN2_NAME));

                qryEntity.setIndexes(indexes);

                cacheCfg.setQueryEntities(Collections.singletonList(qryEntity));
            }
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test number of partitions need to finished indexes rebuilding.
     */
    @Test
    public void testIndexRebuildCountPartitionsLeft() throws Exception {
        pds = true;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_NAME);

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME)
                .setField(KEY_NAME, id)
                .setField(COLUMN1_NAME, i / 2)
                .setField(COLUMN2_NAME, "str" + Integer.toHexString(i));

            cache1.put(id, o.build());
        }

        ignite.cluster().state(ClusterState.INACTIVE);

        deleteIndexBin(ignite.context().pdsFolderResolver().fileTree());

        ignite.cluster().state(ClusterState.ACTIVE);

        MetricRegistry metrics = cacheGroupMetrics(0, GROUP_NAME);

        LongMetric idxBuildCntPartitionsLeft = metrics.findMetric("IndexBuildCountPartitionsLeft");

        assertTrue("Timeout wait start rebuild index",
            waitForCondition(() -> idxBuildCntPartitionsLeft.value() > 0, 30_000));

        assertTrue("Timeout wait finished rebuild index",
            GridTestUtils.waitForCondition(() -> idxBuildCntPartitionsLeft.value() == 0, 30_000));
    }

    /**
     * Test number of partitions need to finished create indexes.
     */
    @Test
    public void testIndexCreateCountPartitionsLeft() throws Exception {
        pds = true;

        Ignite ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_NAME);

        String addColSql = "ALTER TABLE " + TABLE + " ADD COLUMN " + COLUMN3_NAME + " BIGINT";

        cache1.query(new SqlFieldsQuery(addColSql)).getAll();

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME)
                .setField(KEY_NAME, id)
                .setField(COLUMN1_NAME, i / 2)
                .setField(COLUMN2_NAME, "str" + Integer.toHexString(i))
                .setField(COLUMN3_NAME, id * 10);

            cache1.put(id, o.build());
        }

        MetricRegistry metrics = cacheGroupMetrics(0, GROUP_NAME);

        GridTestUtils.runAsync(() -> {
            String createIdxSql = "CREATE INDEX " + INDEX_NAME + " ON " + TABLE + "(" + COLUMN3_NAME + ")";

            cache1.query(new SqlFieldsQuery(createIdxSql)).getAll();

            String selectIdxSql = "select * from information_schema.indexes where index_name='" + INDEX_NAME + "'";

            List<List<?>> all = cache1.query(new SqlFieldsQuery(selectIdxSql)).getAll();

            assertEquals("Index not found", 1, all.size());
        });

        LongMetric idxBuildCntPartitionsLeft = metrics.findMetric("IndexBuildCountPartitionsLeft");

        assertTrue("Timeout wait start build index",
            waitForCondition(() -> idxBuildCntPartitionsLeft.value() > 0, 30_000));

        assertTrue("Timeout wait finished build index",
            waitForCondition(() -> idxBuildCntPartitionsLeft.value() == 0, 30_000));
    }

    /**
     * Test number of partitions need to finished indexes rebuilding.
     * <p>Case:
     * <ul>
     *     <li>Start cluster, load data with indexes</li>
     *     <li>Kill single node, delete index.bin, start node.</li>
     *     <li>Make sure that index rebuild count is in range of total new index size and 0 and decreasing</li>
     *     <li>Wait until rebuild finished, assert that no index errors</li>
     * </ul>
     * </p>
     */
    @Test
    public void testIndexRebuildCountPartitionsLeftInCluster() throws Exception {
        pds = true;

        IgniteEx ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_NAME);

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME)
                    .setField(KEY_NAME, id)
                    .setField(COLUMN1_NAME, i / 2)
                    .setField(COLUMN2_NAME, "str" + Integer.toHexString(i));

            cache1.put(id, o.build());
        }

        NodeFileTree ft = ignite.context().pdsFolderResolver().fileTree();

        stopGrid(0);

        assertTrue(deleteIndexBin(ft) > 0);

        startGrid(0);

        MetricRegistry metrics = cacheGroupMetrics(0, GROUP_NAME);

        LongMetric idxBuildCntPartitionsLeft = metrics.findMetric("IndexBuildCountPartitionsLeft");

        assertTrue("Timeout wait start rebuild index",
                waitForCondition(() -> idxBuildCntPartitionsLeft.value() > 0, 30_000));

        assertTrue("Timeout wait finished rebuild index",
                GridTestUtils.waitForCondition(() -> idxBuildCntPartitionsLeft.value() == 0, 30_000));
    }

    /**
     * Test number of partitions need to finished create indexes.
     * <p>Case:
     * <ul>
     *     <li>Start cluster, load data with indexes</li>
     *     <li>Kill single node, create new index, start node.</li>
     *     <li>Make sure that index rebuild count is in range of total new index size and 0 and decreasing</li>
     *     <li>Wait until rebuild finished, assert that no index errors</li>
     * </ul>
     * </p>
     */
    @Test
    public void testIndexCreateCountPartitionsLeftInCluster() throws Exception {
        pds = true;

        Ignite ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_NAME);

        String addColSql = "ALTER TABLE " + TABLE + " ADD COLUMN " + COLUMN3_NAME + " BIGINT";

        cache1.query(new SqlFieldsQuery(addColSql)).getAll();

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME)
                    .setField(KEY_NAME, id)
                    .setField(COLUMN1_NAME, i / 2)
                    .setField(COLUMN2_NAME, "str" + Integer.toHexString(i))
                    .setField(COLUMN3_NAME, id * 10);

            cache1.put(id, o.build());
        }

        stopGrid(1);

        MetricRegistry metrics = cacheGroupMetrics(0, GROUP_NAME);

        GridTestUtils.runAsync(() -> {
            String createIdxSql = "CREATE INDEX " + INDEX_NAME + " ON " + TABLE + "(" + COLUMN3_NAME + ")";

            cache1.query(new SqlFieldsQuery(createIdxSql)).getAll();

            String selectIdxSql = "select * from information_schema.indexes where index_name='" + INDEX_NAME + "'";

            List<List<?>> all = cache1.query(new SqlFieldsQuery(selectIdxSql)).getAll();

            assertEquals("Index not found", 1, all.size());
        });

        final LongMetric idxBuildCntPartitionsLeft0 = metrics.findMetric("IndexBuildCountPartitionsLeft");

        assertTrue("Timeout wait start build index",
                waitForCondition(() -> idxBuildCntPartitionsLeft0.value() > 0, 30_000));

        assertTrue("Timeout wait finished build index",
                waitForCondition(() -> idxBuildCntPartitionsLeft0.value() == 0, 30_000));

        startGrid(1);

        metrics = cacheGroupMetrics(1, GROUP_NAME);

        final LongMetric idxBuildCntPartitionsLeft1 = metrics.findMetric("IndexBuildCountPartitionsLeft");

        assertTrue("Timeout wait start build index",
            waitForCondition(() -> idxBuildCntPartitionsLeft1.value() > 0, 30_000));

        assertTrue("Timeout wait finished build index",
            waitForCondition(() -> idxBuildCntPartitionsLeft1.value() == 0, 30_000));
    }
}
