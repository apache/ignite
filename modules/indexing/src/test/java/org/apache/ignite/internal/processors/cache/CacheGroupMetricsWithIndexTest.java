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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest.BlockingIndexing;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
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
    private static final String GROUP_NAME_2 = "group2";

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
            if (GROUP_NAME.equals(cacheCfg.getGroupName()) || GROUP_NAME_2.equals(cacheCfg.getGroupName())) {
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

        GridQueryProcessor.idxCls = null;
    }

    /**
     * Test number of partitions need to finished indexes rebuilding.
     */
    @Test
    public void testIndexRebuildCountPartitionsLeft() throws Exception {
        pds = true;

        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        String cacheName2 = "cache2";
        String cacheName3 = "cache3";

        IgniteCache<Long, Object> cache2 = ignite.cache(cacheName2);
        IgniteCache<Long, Object> cache3 = ignite.cache(cacheName3);

        cache2.put(1L, 1L);
        cache3.put(1L, 1L);

        int parts2 = ignite.cachex(cacheName2).configuration().getAffinity().partitions();
        int parts3 = ignite.cachex(cacheName3).configuration().getAffinity().partitions();

        ignite.cluster().active(false);

        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        IOFileFilter filter = FileFilterUtils.nameFileFilter("index.bin");

        Collection<File> idxBinFiles = FileUtils.listFiles(dir, filter, TrueFileFilter.TRUE);

        for (File indexBin : idxBinFiles)
            U.delete(indexBin);

        ignite.cluster().active(true);

        BlockingIndexing blockingIndexing = (BlockingIndexing)ignite.context().query().getIndexing();

        while (!blockingIndexing.isBlock(cacheName2) || !blockingIndexing.isBlock(cacheName3))
            U.sleep(10);

        MetricRegistry grpMreg = cacheGroupMetrics(0, GROUP_NAME_2).get2();

        LongMetric indexBuildCountPartitionsLeft = grpMreg.findMetric("IndexBuildCountPartitionsLeft");

        assertEquals(parts2 + parts3, indexBuildCountPartitionsLeft.value());

        blockingIndexing.stopBlock(cacheName2);

        ignite.cache(cacheName2).indexReadyFuture().get(30_000);

        assertEquals(parts3, indexBuildCountPartitionsLeft.value());

        blockingIndexing.stopBlock(cacheName3);

        ignite.cache(cacheName3).indexReadyFuture().get(30_000);

        assertEquals(0, indexBuildCountPartitionsLeft.value());
    }

    /**
     * Test number of partitions need to finished create indexes.
     */
    @Test
    public void testIndexCreateCountPartitionsLeft() throws Exception {
        pds = true;

        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

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

        MetricRegistry grpMreg = cacheGroupMetrics(0, GROUP_NAME).get2();

        GridTestUtils.runAsync(() -> {
            String createIdxSql = "CREATE INDEX " + INDEX_NAME + " ON " + TABLE + "(" + COLUMN3_NAME + ")";

            cache1.query(new SqlFieldsQuery(createIdxSql)).getAll();

            String selectIdxSql = "select * from information_schema.indexes where index_name='" + INDEX_NAME + "'";

            List<List<?>> all = cache1.query(new SqlFieldsQuery(selectIdxSql)).getAll();

            Assert.assertEquals("Index not found", 1, all.size());
        });

        LongMetric indexBuildCountPartitionsLeft = grpMreg.findMetric("IndexBuildCountPartitionsLeft");

        Assert.assertTrue("Timeout wait start rebuild index",
            waitForCondition(() -> indexBuildCountPartitionsLeft.value() > 0, 30_000));

        Assert.assertTrue("Timeout wait finished rebuild index",
            waitForCondition(() -> indexBuildCountPartitionsLeft.value() == 0, 30_000));
    }
}
