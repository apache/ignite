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
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.stat.StatisticsCacheSizeTask;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** */
public class IgniteStatisticsImplRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    @BeforeAll
    static void init() {
        beforeFirstTest0();
    }

    /** {@inheritDoc} */
    @BeforeEach
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override protected void afterTest() throws Exception {
        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name, ver);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        return cfg;
    }

    /** */
    @Test
    public void testRefreshAfterFinalization() throws Exception {
        startGrid(0, TEST_DEFAULT_VER);
        startGrid(1, TEST_DEFAULT_VER);

        ru(0).enableVersionUpgrade();

        upgradeNodeVersion(0, MASTER_VER_REF);

        IgniteEx node = grid(0);
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(node);

        spi.record((remote, msg) -> msg instanceof GridJobExecuteRequest req
            && StatisticsCacheSizeTask.class.getName().equals(req.taskClassName()));

        sql(node, "CREATE TABLE TEST (id INT PRIMARY KEY, val INT) WITH template=replicated");
        sql(node, "INSERT INTO TEST VALUES (1, 42)");

        assertEquals(1_000, refreshRowCount(node), 0.0);
        assertTrue(spi.recordedMessages(false).isEmpty());

        upgradeNodeVersion(1, MASTER_VER_REF);
        awaitPartitionMapExchange();

        assertEquals(1_000, refreshRowCount(node), 0.0);
        assertTrue(spi.recordedMessages(false).isEmpty());

        finalizeClusterVersion(0, MASTER_VER_REF);

        assertEquals(1, refreshRowCount(node), 0.0);
        assertFalse(spi.recordedMessages(false).isEmpty());
    }

    /** */
    private static double refreshRowCount(IgniteEx node) {
        CalciteQueryProcessor proc = Commons.lookupComponent(node.context(), CalciteQueryProcessor.class);
        CacheTableImpl tbl = (CacheTableImpl)proc.schemaHolder().schema("PUBLIC").getTable("TEST");
        IgniteStatisticsImpl stat = (IgniteStatisticsImpl)tbl.getStatistic();

        stat.cliReqCnt.set(0);

        return stat.getRowCount();
    }

    /** */
    private static void sql(IgniteEx node, String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        try (FieldsQueryCursor<List<?>> cur = node.context().query().querySqlFields(qry, false)) {
            cur.getAll();
        }
    }
}
