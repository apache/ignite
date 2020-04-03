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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Cache group JMX metrics test.
 */
public class CacheGroupMetricsMBeanWithIndexTest extends CacheGroupMetricsMBeanTest {
    /** */
    private static final String GROUP_NAME = "group2";

    /** */
    private static final String CACHE_NAME2 = "cache2";

    /** */
    private static final String CACHE_NAME3 = "cache3";

    /** */
    private static final String OBJECT_NAME2 = "MyObject2";

    /** */
    private static final String OBJECT_NAME3 = "MyObject3";

    /** */
    private static final String TABLE = "\"" + CACHE_NAME2 + "\"." + OBJECT_NAME2;

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
            if (GROUP_NAME.equals(cacheCfg.getGroupName()) && CACHE_NAME2.equals(cacheCfg.getName())) {
                QueryEntity qryEntity = new QueryEntity(Long.class.getCanonicalName(), OBJECT_NAME2);

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
            else if (GROUP_NAME.equals(cacheCfg.getGroupName()) && CACHE_NAME3.equals(cacheCfg.getName())) {
                QueryEntity qryEntity = new QueryEntity(Long.class.getCanonicalName(), OBJECT_NAME3);

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

        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE_NAME2);

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME2)
                .setField(KEY_NAME, id)
                .setField(COLUMN1_NAME, i / 2)
                .setField(COLUMN2_NAME, "str" + Integer.toHexString(i));

            cache2.put(id, o.build());
        }

        ignite.cluster().active(false);

        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        IOFileFilter filter = FileFilterUtils.nameFileFilter("index.bin");

        Collection<File> idxBinFiles = FileUtils.listFiles(dir, filter, TrueFileFilter.TRUE);

        for (File indexBin : idxBinFiles)
            U.delete(indexBin);

        ignite.cluster().active(true);

        CacheGroupMetricsMXBean mxBean0Grp1 = mxBean(0, GROUP_NAME);

        assertTrue("Timeout wait start rebuild index",
            waitForCondition(() -> mxBean0Grp1.getIndexBuildCountPartitionsLeft() > 0, 30_000));

        assertTrue("Timeout wait finished rebuild index",
            waitForCondition(() -> mxBean0Grp1.getIndexBuildCountPartitionsLeft() == 0, 30_000));
    }

    /**
     * Test number of partitions need to finished create indexes.
     */
    @Test
    public void testIndexCreateCountPartitionsLeft() throws Exception {
        pds = true;

        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE_NAME2);

        String addColSql = "ALTER TABLE " + TABLE + " ADD COLUMN " + COLUMN3_NAME + " BIGINT";

        cache2.query(new SqlFieldsQuery(addColSql)).getAll();

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME2)
                .setField(KEY_NAME, id)
                .setField(COLUMN1_NAME, i / 2)
                .setField(COLUMN2_NAME, "str" + Integer.toHexString(i))
                .setField(COLUMN3_NAME, id * 10);

            cache2.put(id, o.build());
        }

        GridTestUtils.runAsync(() -> {
            String createIdxSql = "CREATE INDEX " + INDEX_NAME + " ON " + TABLE + "(" + COLUMN3_NAME + ")";

            cache2.query(new SqlFieldsQuery(createIdxSql)).getAll();

            String selectIdxSql = "select * from sys.indexes where index_name='" + INDEX_NAME + "'";

            List<List<?>> all = cache2.query(new SqlFieldsQuery(selectIdxSql)).getAll();

            assertEquals("Index not found", 1, all.size());
        });

        CacheGroupMetricsMXBean mxBean0Grp1 = mxBean(0, GROUP_NAME);

        assertTrue("Timeout wait start build index",
            waitForCondition(() -> mxBean0Grp1.getIndexBuildCountPartitionsLeft() > 0, 30_000));

        assertTrue("Timeout wait finished build index",
            waitForCondition(() -> mxBean0Grp1.getIndexBuildCountPartitionsLeft() == 0, 30_000));
    }

    /**
     * Test number of partitions need to finished indexes rebuilding.
     * <p>Case:
     * <ul>
     *     <li>Start cluster</li>
     *     <li>Load data for first cache in group</li>
     *     <li>Create new column for second cache in group</li>
     *     <li>Load data for second cache in group</li>
     *     <li>Kill single node, delete index.bin, start node.</li>
     *     <li>Make sure that index rebuild count is in range of total new index size and 0 and decreasing</li>
     *     <li>Create index for new column in second cache in group</li>
     *     <li>Wait until rebuild finished, assert that no index errors</li>
     * </ul>
     * </p>
     */
    @Test
    public void testIndexRebuildCountPartitionsLeftInCluster() throws Exception {
        pds = true;

        Ignite ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE_NAME2);

        IgniteCache<Object, Object> cache3 = ignite.cache(CACHE_NAME3);

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME2)
                    .setField(KEY_NAME, id)
                    .setField(COLUMN1_NAME, i / 2)
                    .setField(COLUMN2_NAME, "str" + Integer.toHexString(i));

            cache2.put(id, o.build());
        }

        String addColSql = "ALTER TABLE \"" + CACHE_NAME3 + "\"." + OBJECT_NAME3 + " ADD COLUMN " + COLUMN3_NAME + " BIGINT";

        cache3.query(new SqlFieldsQuery(addColSql)).getAll();

        for (long id = 100_000; id<200_000; id++){

            BinaryObjectBuilder o2 = ignite.binary().builder(OBJECT_NAME3)
                .setField(KEY_NAME, id*3)
                .setField(COLUMN1_NAME, (int)(id / 2))
                .setField(COLUMN2_NAME, "str" + Long.toHexString(id))
                .setField(COLUMN3_NAME, id * 10);

            cache3.put(id, o2.build());
        }

        String consistentId = ignite.cluster().localNode().consistentId().toString();

        stopGrid(0);

        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        IOFileFilter filter = FileFilterUtils.nameFileFilter("index.bin");

        Collection<File> idxBinFiles = FileUtils.listFiles(dir, filter, TrueFileFilter.TRUE);

        for (File indexBin : idxBinFiles)
            if (indexBin.getAbsolutePath().contains(consistentId))
                U.delete(indexBin);

        ignite = startGrid(0);

        CacheGroupMetricsMXBean mxBean0Grp1 = mxBean(0, GROUP_NAME);

        assertTrue("Timeout wait start rebuild index",
                waitForCondition(() -> mxBean0Grp1.getIndexBuildCountPartitionsLeft() > 0, 30_000));

        cache3 = ignite.cache(CACHE_NAME3);

        String createIdxSql = "CREATE INDEX ON \"" + CACHE_NAME3 + "\"." + OBJECT_NAME3 + "(" + COLUMN3_NAME + ")";

        cache3.query(new SqlFieldsQuery(createIdxSql)).getAll();

        assertTrue("Timeout wait finished rebuild index",
            waitForCondition(() -> {
                assertTrue("indexBuildCountPartitionsLeft below zero", mxBean0Grp1.getIndexBuildCountPartitionsLeft() >= 0);
                return mxBean0Grp1.getIndexBuildCountPartitionsLeft() == 0;
            }, 30_000));

        assertTrue("Checking for the absence of indexBuildCountPartitionsLeft below zero",
            mxBean0Grp1.getIndexBuildCountPartitionsLeft() == 0);
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

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE_NAME2);

        String addColSql = "ALTER TABLE " + TABLE + " ADD COLUMN " + COLUMN3_NAME + " BIGINT";

        cache2.query(new SqlFieldsQuery(addColSql)).getAll();

        for (int i = 0; i < 100_000; i++) {
            Long id = (long)i;

            BinaryObjectBuilder o = ignite.binary().builder(OBJECT_NAME2)
                    .setField(KEY_NAME, id)
                    .setField(COLUMN1_NAME, i / 2)
                    .setField(COLUMN2_NAME, "str" + Integer.toHexString(i))
                    .setField(COLUMN3_NAME, id * 10);

            cache2.put(id, o.build());
        }

        stopGrid(1);

        GridTestUtils.runAsync(() -> {
            String createIdxSql = "CREATE INDEX " + INDEX_NAME + " ON " + TABLE + "(" + COLUMN3_NAME + ")";

            cache2.query(new SqlFieldsQuery(createIdxSql)).getAll();

            String selectIdxSql = "select * from sys.indexes where index_name='" + INDEX_NAME + "'";

            List<List<?>> all = cache2.query(new SqlFieldsQuery(selectIdxSql)).getAll();

            assertEquals("Index not found", 1, all.size());
        });

        final CacheGroupMetricsMXBean mxBean0Grp = mxBean(0, GROUP_NAME);

        assertTrue("Timeout wait start build index",
                waitForCondition(() -> mxBean0Grp.getIndexBuildCountPartitionsLeft() > 0, 30_000));

        assertTrue("Timeout wait finished build index",
                waitForCondition(() -> mxBean0Grp.getIndexBuildCountPartitionsLeft() == 0, 30_000));

        startGrid(1);

        final CacheGroupMetricsMXBean mxBean1Grp = mxBean(1, GROUP_NAME);

        assertTrue("Timeout wait start build index",
            waitForCondition(() -> mxBean1Grp.getIndexBuildCountPartitionsLeft() > 0, 30_000));

        assertTrue("Timeout wait finished build index",
            waitForCondition(() -> mxBean1Grp.getIndexBuildCountPartitionsLeft() == 0, 30_000));
    }
}
