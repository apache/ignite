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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for all SQL based indexing SPI implementations.
 */
public abstract class GridIndexingSpiAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final LinkedHashMap<String, String> fieldsAA = new LinkedHashMap<>();

    /** */
    private static final LinkedHashMap<String, String> fieldsAB = new LinkedHashMap<>();

    /** */
    private IgniteEx ignite0;

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /*
     * Fields initialization.
     */
    static {
        fieldsAA.put("id", Long.class.getName());
        fieldsAA.put("name", String.class.getName());
        fieldsAA.put("age", Integer.class.getName());

        fieldsAB.putAll(fieldsAA);
        fieldsAB.put("txt", String.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite0 = startGrid(0);
    }

    /**
     */
    private CacheConfiguration cacheACfg() {
        CacheConfiguration<?,?> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setName("A");

        QueryEntity eA = new QueryEntity(Integer.class.getName(), "A");
        eA.setFields(fieldsAA);

        QueryEntity eB = new QueryEntity(Integer.class.getName(), "B");
        eB.setFields(fieldsAB);

        List<QueryEntity> list = new ArrayList<>(2);

        list.add(eA);
        list.add(eB);

        QueryIndex idx = new QueryIndex("txt");
        idx.setIndexType(QueryIndexType.FULLTEXT);
        eB.setIndexes(Collections.singleton(idx));

        cfg.setQueryEntities(list);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Indexing.
     */
    private IgniteH2Indexing getIndexing() {
        return U.field(ignite0.context().query(), "idx");
    }

    /**
     * @return {@code true} if OFF-HEAP mode should be tested.
     */
    protected boolean offheap() {
        return false;
    }

    /**
     * Test long queries write explain warnings into log.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked", "deprecation"})
    public void testLongQueries() throws Exception {
        IgniteH2Indexing spi = getIndexing();

        ignite0.createCache(cacheACfg());

        long longQryExecTime = IgniteConfiguration.DFLT_LONG_QRY_WARN_TIMEOUT;

        GridStringLogger log = new GridStringLogger(false, this.log);

        IgniteLogger oldLog = GridTestUtils.getFieldValue(spi, "log");

        try {
            GridTestUtils.setFieldValue(spi, "log", log);

            String sql = "select sum(x) FROM SYSTEM_RANGE(?, ?)";

            long now = U.currentTimeMillis();
            long time = now;

            long range = 1000000L;

            while (now - time <= longQryExecTime * 3 / 2) {
                time = now;
                range *= 3;

                GridQueryFieldsResult res = spi.queryLocalSqlFields(spi.schema("A"), sql, Arrays.<Object>asList(1,
                    range), null, false, false, 0, null, null);

                assert res.iterator().hasNext();

                now = U.currentTimeMillis();
            }

            String res = log.toString();

            assertTrue(res.contains("/* PUBLIC.RANGE_INDEX */"));
        }
        finally {
            GridTestUtils.setFieldValue(spi, "log", oldLog);
        }
    }
}
