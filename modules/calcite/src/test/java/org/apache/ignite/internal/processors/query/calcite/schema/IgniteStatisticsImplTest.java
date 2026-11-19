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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.GridCommonAbstractWrapperTest;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl.DFLT_ROW_CNT;
import static org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl.STATS_REFRESH_INTERVAL;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class IgniteStatisticsImplTest extends GridCommonAbstractWrapperTest {
    /** */
    private static final String TBL_NAME = "TEST";

    /** */
    private static final int ROW_CNT = (int)DFLT_ROW_CNT + 1;

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** */
    @BeforeEach
    public void startNodes() throws Exception {
        startGrids(2);

        client = startClientGrid(2);
    }

    /** */
    @AfterEach
    public void stopNodes() {
        stopAllGrids();
    }

    /** */
    @ParameterizedTest
    @EnumSource(value = CacheMode.class, names = {"PARTITIONED", "REPLICATED"})
    public void testPrimaryRowsOnly(CacheMode mode) {
        createAndFillTable(mode);

        assertEquals(ROW_CNT, statistics().getRowCount(), 0.0);
    }

    /** */
    @Test
    public void testTimeoutKeepsPreviousEstimate() throws Exception {
        createAndFillTable(CacheMode.PARTITIONED);

        IgniteStatisticsImpl stat = statistics();

        assertEquals(ROW_CNT, stat.getRowCount(), 0.0);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(client);

        spi.blockMessages(GridJobExecuteRequest.class, grid(1).name());

        try {
            assertEquals(ROW_CNT, statistics().getRowCount(), 0.0);
            assertFalse(spi.hasBlockedMessages());

            stat.lastRefreshTs = 0;

            assertEquals(ROW_CNT, runAsync(() -> statistics().getRowCount()).get(5_000), 0.0);
            assertTrue(spi.hasBlockedMessages());
        }
        finally {
            spi.stopBlock(false);
        }
    }

    /** */
    @Test
    public void testNodeLeftKeepsPreviousEstimate() throws Exception {
        createAndFillTable(CacheMode.PARTITIONED);

        IgniteStatisticsImpl stat = statistics();

        assertEquals(ROW_CNT, stat.getRowCount(), 0.0);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(client);

        spi.blockMessages(GridJobExecuteRequest.class, grid(1).name());

        try {
            stat.lastRefreshTs = 0;

            IgniteInternalFuture<Double> fut = runAsync(stat::getRowCount);

            assertTrue(waitForCondition(spi::hasBlockedMessages, 5_000));

            stopGrid(1);

            assertEquals(ROW_CNT, fut.get(5_000), 0.0);
        }
        finally {
            spi.stopBlock(false);
        }
    }

    /** */
    @Test
    public void testRefreshByTime() throws Exception {
        createAndFillTable(CacheMode.PARTITIONED);

        assertEquals(ROW_CNT, statistics().getRowCount(), 0.0);

        sql(String.format("INSERT INTO %s VALUES (%d, %d)", TBL_NAME, ROW_CNT, ROW_CNT));

        assertTrue(waitForCondition(() -> statistics().getRowCount() == ROW_CNT + 1, 2 * STATS_REFRESH_INTERVAL));
    }

    /** */
    @Test
    public void testIndexScanPlan() throws Exception {
        createAndFillTable(CacheMode.PARTITIONED, 0, 1);
        sql("CREATE INDEX idxVal ON %s(val)".formatted(TBL_NAME));

        String sql = "EXPLAIN PLAN FOR SELECT * FROM %s WHERE val > 100 AND val < 200".formatted(TBL_NAME);
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        try (FieldsQueryCursor<List<?>> cur = client.context().query().querySqlFields(qry, false)) {
            List<List<?>> curr = cur.getAll();

            String plan = (String)curr.get(0).get(0);

            // Plan for single item table need to goes through table scan,
            // but due to IgniteStatisticsImpl.DFLT_ROW_CNT hack it plans through index scan, this part
            // need to be fixed after cache for plans invalidation is done.
            assertTrue(plan, !plan.contains(IgniteTableScan.class.getSimpleName()));
        }

        createAndFillTable(CacheMode.PARTITIONED, 1, ROW_CNT);

        assertTrue(waitForCondition(() -> {
            try (FieldsQueryCursor<List<?>> cur = client.context().query().querySqlFields(qry, false)) {
                List<List<?>> curr = cur.getAll();

                String plan = (String)curr.get(0).get(0);
                return !plan.contains(IgniteTableScan.class.getSimpleName());
            }
        }, 2 * STATS_REFRESH_INTERVAL));
    }

    /** */
    @Test
    public void testConcurrentSelectsUseIndex() throws Exception {
        String query = "select _key, _val from Person where salary >= ? and salary <= ?";

        IgniteEx client = startClientGrid(3);

        int tableScans = 0;
        int tables = 20;
        String tableScanPlan = null;

        for (int t = 0; t < tables; t++) {
            String cacheName = "person" + t;

            IgniteCache<Integer, Person> cache = client.createCache(new CacheConfiguration<Integer, Person>(cacheName)
                .setIndexedTypes(Integer.class, Person.class));

            for (int i = 0; i < 50; i++)
                cache.put(i, new Person(i * 1000));

            CyclicBarrier start = new CyclicBarrier(2);

            runMultiThreaded(() -> {
                start.await();

                return cache.query(new SqlFieldsQuery(query).setArgs(0, 1000)).getAll();
            }, 2, "select");

            SqlFieldsQuery qry = new SqlFieldsQuery("EXPLAIN PLAN FOR " + query).setArgs(0, 1000);
            String plan;

            try (FieldsQueryCursor<List<?>> cur = cache.withKeepBinary().query(new SqlFieldsQuery(qry).setArgs(0, 1000))) {
                List<List<?>> curr = cur.getAll();

                plan = (String)curr.get(0).get(0);
            }

            boolean tableScan = plan.contains(IgniteTableScan.class.getSimpleName());

            if (tableScan) {
                tableScans++;
                tableScanPlan = plan;
            }

            client.destroyCache(cacheName);
        }

        assertEquals("Tables where the select chose a full table scan: " + tableScans + " of " + tables + ", e.g.\n"
            + tableScanPlan, 0, tableScans);
    }

    /** */
    private IgniteStatisticsImpl statistics() {
        CalciteQueryProcessor proc = Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);
        CacheTableImpl table = (CacheTableImpl)proc.schemaHolder().schema("PUBLIC").getTable(TBL_NAME);

        return (IgniteStatisticsImpl)table.getStatistic();
    }

    /**  */
    private void createAndFillTable(CacheMode mode) {
        createAndFillTable(mode, 0, ROW_CNT);
    }

    /**  */
    private void createAndFillTable(CacheMode mode, int begin, int end) {
        sql(String.format("CREATE TABLE IF NOT EXISTS %s(id INT PRIMARY KEY, val INT) WITH template=%s, backups=1", TBL_NAME, mode.name()));

        String values = IntStream.range(begin, end)
            .mapToObj(i -> "(" + i + "," + i + ")")
            .collect(Collectors.joining(","));

        sql(String.format("INSERT INTO %s VALUES %s", TBL_NAME, values));
    }

    /**  */
    private void sql(String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        try (FieldsQueryCursor<List<?>> cur = client.context().query().querySqlFields(qry, false)) {
            cur.getAll();
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        private final double salary;

        /** */
        Person(double salary) {
            this.salary = salary;
        }
    }
}
