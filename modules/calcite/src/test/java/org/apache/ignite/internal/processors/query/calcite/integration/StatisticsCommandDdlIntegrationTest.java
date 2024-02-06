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

import java.util.List;
import java.util.function.Predicate;
import com.google.common.collect.Lists;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManager;
import org.apache.ignite.internal.processors.query.stat.ObjectStatistics;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Integration tests for statistics collection.
 */
public class StatisticsCommandDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /** */
    private static final String PUBLIC_SCHEMA = "PUBLIC";

    /** */
    private static final long TIMEOUT = 5_000;

    /** */
    private static final String TABLE_NAME = "TEST";

    /** */
    private static final String TABLE_1_NAME = "TEST1";

    /** */
    private static final String ID_FIELD = "ID";

    /** */
    private static final String NAME_FIELD = "NAME";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, true);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, true);

        String creatTblFmt = "CREATE TABLE %s(%s INT PRIMARY KEY, %s VARCHAR)";
        sql(String.format(creatTblFmt, TABLE_NAME, ID_FIELD, NAME_FIELD));
        sql(String.format(creatTblFmt, TABLE_1_NAME, ID_FIELD, NAME_FIELD));

        sql(String.format("CREATE INDEX TEXT_%s ON %s(%s);", NAME_FIELD, TABLE_NAME, NAME_FIELD));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        clearStat();
    }

    /**
     * 1) Analyze two test table one by one and test statistics collected
     * 2) Clear collected statistics
     * 3) Analyze it in single batch
     */
    @Test
    public void testAnalyze() throws IgniteCheckedException {
        sql(String.format("ANALYZE %s", TABLE_NAME));
        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, false);

        sql(String.format("ANALYZE %s.%s(%s)", PUBLIC_SCHEMA, TABLE_1_NAME, NAME_FIELD));

        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, false);

        clearStat();

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, true);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, true);

        sql(String.format("ANALYZE %s.%s, %s", PUBLIC_SCHEMA, TABLE_NAME, TABLE_1_NAME));

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, false);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, false);
    }

    /**
     * Tests analyze command options.
     */
    @Test
    public void testAnalyzeOptions() {
        assertTrue(sql(grid(0), "SELECT * FROM SYS.STATISTICS_CONFIGURATION WHERE NAME = ?", TABLE_NAME).isEmpty());

        sql(String.format(
            "ANALYZE %s(%s) WITH \"DISTINCT=5,NULLS=6,TOTAL=7,SIZE=8,MAX_CHANGED_PARTITION_ROWS_PERCENT=10\"",
            TABLE_NAME, ID_FIELD
        ));

        // MAX_CHANGED_PARTITION_ROWS_PERCENT overrides old settings for all columns.
        sql(String.format(
            "ANALYZE %s(%s) WITH DISTINCT=6,NULLS=7,TOTAL=8,MAX_CHANGED_PARTITION_ROWS_PERCENT=2",
            TABLE_NAME, NAME_FIELD
        ));

        List<List<?>> res = sql(grid(0), "SELECT * FROM SYS.STATISTICS_CONFIGURATION WHERE NAME = ?", TABLE_NAME);

        assertFalse(res.isEmpty());
        assertFalse(res.get(0).isEmpty() || res.get(0).size() < 10);

        long ver = (Long)res.get(0).get(9);

        assertThat(res, hasItems(
            Lists.newArrayList(PUBLIC_SCHEMA, "TABLE", TABLE_NAME, ID_FIELD, (byte)2, 6L, 5L, 7L, 8, ver),
            Lists.newArrayList(PUBLIC_SCHEMA, "TABLE", TABLE_NAME, NAME_FIELD, (byte)2, 7L, 6L, 8L, null, ver)
        ));
    }

    /**
     * 0) Ensure that there are no statistics before test (old id after schema implementation).
     * 1) Refresh statistics in batch.
     * 2) Test that there are statistics collected (new after schema implementation).
     * 3) Clear statistics and refresh one again.
     * 4) Test that now only one statistics exists.
     */
    @Test
    public void testRefreshStatistics() throws IgniteCheckedException {
        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, true);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, true);

        sql(String.format("ANALYZE %s.%s, %s", PUBLIC_SCHEMA, TABLE_NAME, TABLE_1_NAME));

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, false);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, false);

        long testVer = sumStatisticsVersion(PUBLIC_SCHEMA, TABLE_NAME);
        long test2Ver = sumStatisticsVersion(PUBLIC_SCHEMA, TABLE_1_NAME);

        sql(String.format("REFRESH STATISTICS %s.%s, %s", PUBLIC_SCHEMA, TABLE_NAME, TABLE_1_NAME));

        testStatisticsVersion(PUBLIC_SCHEMA, TABLE_NAME, newVer -> newVer > testVer);
        testStatisticsVersion(PUBLIC_SCHEMA, TABLE_1_NAME, newVer -> newVer > test2Ver);
    }

    /**
     * 1) Refresh not exist statistics for table.
     * 2) Refresh not exist statistics for column.
     *
     * Check that correct exception is thrown in all cases.
     */
    @Test
    public void testRefreshNotExistStatistics() throws IgniteInterruptedCheckedException {
        GridTestUtils.assertThrows(
            log,
            () -> sql("REFRESH STATISTICS PUBLIC.TEST"),
            IgniteSQLException.class,
            "Statistic doesn't exist for [schema=PUBLIC, obj=TEST]"
        );

        sql("ANALYZE PUBLIC.TEST(id)");

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, false);

        long testVer = sumStatisticsVersion(PUBLIC_SCHEMA, TABLE_NAME);

        GridTestUtils.assertThrows(
            log,
            () -> sql("REFRESH STATISTICS PUBLIC.TEST (id, name)"),
            IgniteSQLException.class,
            "Statistic doesn't exist for [schema=PUBLIC, obj=TEST, col=NAME]"
        );

        testStatisticsVersion(PUBLIC_SCHEMA, TABLE_NAME, newVer -> newVer == testVer);
    }

    /**
     * Test drop statistics command:
     * 1) Collect and test that statistics exists.
     * 2) Drop statistics by single column.
     * 3) Test statistics exists for the rest columns.
     * 4) Drop statistics by the rest column.
     * 5) Test statistics not exists
     */
    @Test
    public void testDropStatistics() throws IgniteInterruptedCheckedException {
        sql(String.format("ANALYZE %s.%s, %s", PUBLIC_SCHEMA, TABLE_NAME, TABLE_1_NAME));

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, false);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, false);

        sql(String.format("DROP STATISTICS %s.%s(%s)", PUBLIC_SCHEMA, TABLE_NAME, NAME_FIELD));

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, false);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, false);

        sql(String.format("DROP STATISTICS %s.%s", PUBLIC_SCHEMA, TABLE_NAME));

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, true);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, false);

        sql(String.format("ANALYZE %s.%s, %s", PUBLIC_SCHEMA, TABLE_NAME, TABLE_1_NAME));

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, false);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, false);

        sql(String.format("DROP STATISTICS %s.%s, %s", PUBLIC_SCHEMA, TABLE_NAME, TABLE_1_NAME));

        testStatistics(PUBLIC_SCHEMA, TABLE_NAME, true);
        testStatistics(PUBLIC_SCHEMA, TABLE_1_NAME, true);
    }

    /**
     * 1) Drop not exist statistics for table.
     * 2) Drop not exist statistics for column.
     *
     * Check that correct exception is thrown in all cases.
     */
    @Test
    public void testDropNotExistStatistics() {
        GridTestUtils.assertThrows(
            log,
            () -> sql(String.format("DROP STATISTICS %s.%s", PUBLIC_SCHEMA, TABLE_NAME)),
            IgniteSQLException.class,
            String.format("Statistic doesn't exist for [schema=%s, obj=%s]", PUBLIC_SCHEMA, TABLE_NAME)
        );

        sql("ANALYZE PUBLIC.TEST(id)");

        GridTestUtils.assertThrows(
            log,
            () -> sql(String.format("DROP STATISTICS %S.%s(%s, %s)",
                PUBLIC_SCHEMA, TABLE_NAME, ID_FIELD, NAME_FIELD)),
            IgniteSQLException.class,
            String.format("Statistic doesn't exist for [schema=%s, obj=%s, col=%s]",
                PUBLIC_SCHEMA, TABLE_NAME, NAME_FIELD)
        );
    }

    /**
     * Test ability to create table, index and statistics on table named STATISTICS:
     *
     * 1) Create table STATISTICS with column STATISTICS.
     * 2) Create index STATISTICS_STATISTICS on STATISTICS(STATISTICS).
     * 3) Analyze STATISTICS and check that statistics collected.
     * 4) Refresh STATISTICS.
     * 5) Drop statistics for table STATISTICS.
     */
    @Test
    public void statisticsLexemaTest() throws IgniteInterruptedCheckedException {
        sql("CREATE TABLE STATISTICS(id int primary key, statistics varchar)");
        sql("CREATE INDEX STATISTICS_STATISTICS ON STATISTICS(STATISTICS);");

        testStatistics(PUBLIC_SCHEMA, "STATISTICS", true);

        sql("ANALYZE PUBLIC.STATISTICS(STATISTICS)");

        testStatistics(PUBLIC_SCHEMA, "STATISTICS", false);

        sql("REFRESH STATISTICS PUBLIC.STATISTICS(STATISTICS)");

        testStatistics(PUBLIC_SCHEMA, "STATISTICS", false);

        sql("DROP STATISTICS PUBLIC.STATISTICS(STATISTICS)");

        testStatistics(PUBLIC_SCHEMA, "STATISTICS", true);
    }

    /**
     * Clear statistics on two test tables;
     *
     * @throws IgniteCheckedException In case of errors.
     */
    private void clearStat() throws IgniteCheckedException {
        statisticsMgr(0).dropAll();
    }

    /**
     * Test statistics existence on all nodes.
     *
     * @param schema Schema name.
     * @param obj Object name.
     * @param isNull If {@code true} - test that statistics is null, if {@code false} - test that they are not null.
     */
    private void testStatistics(String schema, String obj, boolean isNull) throws IgniteInterruptedCheckedException {
        assertTrue("Unable to wait statistics by " + schema + "." + obj + " if null=" + isNull, waitForCondition(() -> {
            for (Ignite node : G.allGrids()) {
                if (node.cluster().localNode().isClient())
                    continue;

                ObjectStatistics locStat = statisticsMgr((IgniteEx)node).getLocalStatistics(
                    new StatisticsKey(schema, obj));

                if (!(isNull == (locStat == null)))
                    return false;
            }
            return true;
        }, TIMEOUT));
    }

    /**
     * Test statistics existence on all nodes.
     *
     * @param schema Schema name.
     * @param obj Object name.
     */
    private void testStatisticsVersion(
        String schema,
        String obj,
        Predicate<Long> verChecker
    ) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            for (Ignite node : G.allGrids()) {
                if (node.cluster().localNode().isClient())
                    continue;

                ObjectStatisticsImpl locStat = (ObjectStatisticsImpl)statisticsMgr((IgniteEx)node)
                    .getLocalStatistics(new StatisticsKey(schema, obj));

                long sumVer = locStat.columnsStatistics().values().stream()
                    .mapToLong(ColumnStatistics::version)
                    .sum();

                if (!verChecker.test(sumVer))
                    return false;
            }

            return true;
        }, TIMEOUT));
    }

    /**
     * Get average version of the column statistics for specified DB object.
     */
    private long sumStatisticsVersion(String schema, String obj) {
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl)statisticsMgr(0)
            .getLocalStatistics(new StatisticsKey(schema, obj));

        if (locStat == null)
            return -1;

        return locStat.columnsStatistics().values().stream()
            .mapToLong(ColumnStatistics::version)
            .sum();
    }

    /** */
    private IgniteStatisticsManager statisticsMgr(int idx) {
        return statisticsMgr(grid(idx));
    }

    /** */
    private IgniteStatisticsManager statisticsMgr(IgniteEx ign) {
        return ign.context().query().statsManager();
    }
}
