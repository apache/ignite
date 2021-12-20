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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Sort aggregate integration test.
 */
public class SortAggregateIntegrationTest extends GridCommonAbstractTest {
    /** */
    public static final int ROWS = 103;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache("TEST").clear();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        LinkedHashMap<String, Boolean> fields1 = new LinkedHashMap<>();
        fields1.put("COL0", false);
        fields1.put("COL3", true);

        QueryEntity tbl1 = new QueryEntity()
            .setTableName("TBL1")
            .setKeyType(Integer.class.getName())
            .setValueType(TestValTbl1.class.getName())
            .setKeyFieldName("PK")
            .addQueryField("PK", Integer.class.getName(), null)
            .addQueryField("COL0", Integer.class.getName(), null)
            .addQueryField("COL1", Integer.class.getName(), null)
            .addQueryField("COL2", Integer.class.getName(), null)
            .addQueryField("COL3", Integer.class.getName(), null)
            .addQueryField("COL4", Integer.class.getName(), null)
            .setIndexes(Collections.singletonList(new QueryIndex(fields1, QueryIndexType.SORTED)));

        QueryEntity part = new QueryEntity()
            .setTableName("TEST")
            .setKeyType(Integer.class.getName())
            .setValueType(TestValTest.class.getName())
            .setKeyFieldName("ID")
            .addQueryField("ID", Integer.class.getName(), null)
            .addQueryField("GRP0", Integer.class.getName(), null)
            .addQueryField("GRP1", Integer.class.getName(), null)
            .addQueryField("VAL0", Integer.class.getName(), null)
            .addQueryField("VAL1", Integer.class.getName(), null)
            .setIndexes(Collections.singletonList(new QueryIndex(Arrays.asList("GRP0", "GRP1"), QueryIndexType.SORTED)));

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(part.getTableName())
                    .setAffinity(new RendezvousAffinityFunction(false, 8))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setQueryEntities(Arrays.asList(tbl1, part))
                    .setSqlSchema("PUBLIC")
            );
    }

    /** */
    @Test
    public void mapReduceAggregate() throws InterruptedException {
        fillCacheTest(grid(0).cache("TEST"), ROWS);

        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors = engine.query(
            null,
            "PUBLIC",
            "SELECT /*+ DISABLE_RULE('HashAggregateConverterRule') */" +
                "SUM(val0), SUM(val1), grp0 FROM TEST " +
                "GROUP BY grp0 " +
                "HAVING SUM(val1) > 10",
            X.EMPTY_OBJECT_ARRAY
        );

        List<List<?>> res = cursors.get(0).getAll();

        assertEquals(ROWS / 10, res.size());

        res.forEach(r -> {
            long s0 = (Long)r.get(0);
            long s1 = (Long)r.get(1);

            assertEquals(s0 * 2, s1);
        });
    }

    /** */
    @Test
    public void correctCollationsOnMapReduceSortAgg() throws InterruptedException {
        fillCacheTbl1(grid(0).cache("TEST"), ROWS);

        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> cursors = engine.query(
            null,
            "PUBLIC",
            "SELECT /*+ DISABLE_RULE('HashAggregateConverterRule' ,'HashSingleAggregateConverterRule', " +
                "'HashMapReduceAggregateConverterRule', 'LogicalTableScanConverterRule') */" +
                "PK FROM TBL1 WHERE (col3 < 54) AND col3 IN (SELECT col0 FROM TBL1 WHERE col3 >= 55) AND col4 " +
                "IN (SELECT col1 FROM TBL1 WHERE (((col0 IS NULL AND (col4 > 56 AND col0 = 45))) OR col3 BETWEEN 20 AND 60)) " +
                "ORDER BY 1 DESC",
            X.EMPTY_OBJECT_ARRAY
        );

        List<List<?>> res = cursors.get(0).getAll();

        assertEquals(0, res.size());
    }

    /**
     * @param c Cache.
     * @param rows Rows count.
     */
    private void fillCacheTbl1(IgniteCache c, int rows) throws InterruptedException {
        c.clear();

        for (int i = 0; i < rows; ++i)
            c.put(i, new TestValTbl1(i));

        awaitPartitionMapExchange();
    }

    /**
     * @param c Cache.
     * @param rows Rows count.
     */
    private void fillCacheTest(IgniteCache c, int rows) throws InterruptedException {
        c.clear();

        for (int i = 0; i < rows; ++i)
            c.put(i, new TestValTest(i));

        awaitPartitionMapExchange();
    }

    /** */
    public static class TestValTbl1 {
        /** */
        int col0;

        /** */
        int col1;

        /** */
        int col2;

        /** */
        int col3;

        /** */
        int col4;

        /** */
        TestValTbl1(int k) {
            col0 = col1 = col2 = col3 = col4 = k;
        }
    }

    /** */
    public static class TestValTest {
        /** */
        int grp0;

        /** */
        int grp1;

        /** */
        int val0;

        /** */
        int val1;

        /** */
        TestValTest(int k) {
            grp0 = k / 10;
            grp1 = k / 100;

            val0 = 1;
            val1 = 2;
        }
    }
}
