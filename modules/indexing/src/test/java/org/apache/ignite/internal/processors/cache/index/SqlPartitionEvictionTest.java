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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.SECONDS;

/** */
@RunWith(Parameterized.class)
public class SqlPartitionEvictionTest extends GridCommonAbstractTest {
    /** */
    private static final String POI_CACHE_NAME = "POI_CACHE";

    /** */
    private static final String POI_SCHEMA_NAME = "DOMAIN";

    /** */
    private static final String POI_TABLE_NAME = "POI";

    /** */
    private static final String POI_CLASS_NAME = "PointOfInterest";

    /** */
    private static final String ID_FIELD_NAME = "id";

    /** */
    private static final String NAME_FIELD_NAME = "name";

    /** */
    private static final String LATITUDE_FIELD_NAME = "latitude";

    /** */
    private static final String LONGITUDE_FIELD_NAME = "longitude";

    /** */
    private static final int NUM_ENTITIES = 1_000;

    /** Test parameters. */
    @Parameterized.Parameters(name = "backups_count={0}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
                new Object[] { 0 },
                new Object[] { 1 },
                new Object[] { 2 }
            );
    }

    /**
     * Number of partition backups.
     */
    @Parameterized.Parameter
    public int backupsCount;

    /**
     * For awaiting of eviction start.
     */
    private static final CountDownLatch LATCH = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(POI_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setSqlSchema("DOMAIN")
            .setQueryEntities(Collections.singletonList(queryEntity()))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(backupsCount)
        );

        cfg.setActiveOnStart(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        super.afterTest();
    }

    /**
     * Tests sql query result after eviction partitions.
     */
    @Test
    public void testSqlConsistencyOnEviction() throws Exception {
        IgniteEx ig = null;

        int idx = 0;
        while (idx <= backupsCount)
            ig = ignitionStart(idx++);

        loadData(ig, 0, NUM_ENTITIES);

        ignitionStart(idx);

        awaitPartitionMapExchange();

        U.await(LATCH, 10, SECONDS);

        for (Ignite g: G.allGrids())
            assertEquals(NUM_ENTITIES, query(g, "SELECT * FROM " + POI_TABLE_NAME).size());
    }

    /** */
    private void loadData(IgniteEx node, int start, int end) {
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
    protected List<List<?>> query(Ignite ig, String sql) {
        IgniteCache<Object, Object> cache = ig.cache(POI_CACHE_NAME).withKeepBinary();

        return cache.query(new SqlFieldsQuery(sql).setSchema(POI_SCHEMA_NAME)).getAll();
    }

    /** */
    private QueryEntity queryEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(ID_FIELD_NAME, Integer.class.getName());
        fields.put(NAME_FIELD_NAME, String.class.getName());
        fields.put(LATITUDE_FIELD_NAME, Double.class.getName());
        fields.put(LONGITUDE_FIELD_NAME, Double.class.getName());

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setKeyFieldName(ID_FIELD_NAME)
            .setValueType(POI_CLASS_NAME)
            .setTableName(POI_TABLE_NAME)
            .setFields(fields);
    }

    /** */
    private IgniteEx ignitionStart(int idx) throws Exception {
        GridQueryProcessor.idxCls = BlockingIndexing.class;

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        return startGrid(cfg);
    }

    /**
     * Blocking indexing processor.
     */
    private static class BlockingIndexing extends IgniteH2Indexing {
        @Override public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type,
            CacheDataRow row) throws IgniteCheckedException {
            U.sleep(50);

            LATCH.countDown();

            super.remove(cctx, type, row);
        }
    }
}
