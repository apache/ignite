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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.GridCommonAbstractWrapperTest;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class IgniteStatisticsImplTest extends GridCommonAbstractWrapperTest {
    /** */
    private static final String TBL_NAME = "TEST";

    /** */
    private static final int ROW_CNT = 100;

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
            stat.cliReqCnt.set(0);

            assertEquals(ROW_CNT, runAsync(stat::getRowCount).get(5_000), 0.0);
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
            stat.cliReqCnt.set(0);

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
    private IgniteStatisticsImpl statistics() {
        CalciteQueryProcessor proc = Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);
        CacheTableImpl table = (CacheTableImpl) proc.schemaHolder().schema("PUBLIC").getTable(TBL_NAME);

        return (IgniteStatisticsImpl) table.getStatistic();
    }

    /**  */
    private void createAndFillTable(CacheMode mode) {
        sql(String.format("CREATE TABLE %s(id INT PRIMARY KEY, val INT) WITH template=replicated, backups=1", TBL_NAME, mode.name()));

        String values = IntStream.range(0, ROW_CNT)
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
}
