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

package org.apache.ignite.internal.processors.query.stat;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.junit.Test;

/**
 * Tests for statistics related views.
 */
public abstract class StatisticsViewsTest extends StatisticsAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        cleanPersistenceDir();

        startGrid(0);
        grid(0).cluster().state(ClusterState.ACTIVE);

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        createSmallTable(null);
        collectStatistics(SMALL_TARGET);
    }

    /**
     * Check small table configuration in statistics column configuration view.
     */
    @Test
    public void testConfigurationView() throws Exception {
        List<List<Object>> config = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", (byte)15, null, null, null, null, 1L),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", (byte)15, null, null, null, null, 1L),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", (byte)15, null, null, null, null, 1L)
        );

        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION", null, config::equals);
    }

    /**
     * Check that no deleted configuration available throw view:
     * 1) Create table,
     * 2) Create statistics for new table.
     * 3) Check statistics configuration presence.
     * 4) Drop statistics for some column of new table.
     * 5) Check statistics configuration without dropped column.
     * 6) Drop statistics for new table.
     * 7) Check statistics configuration without it.
     *
     * @throws Exception
     */
    @Test
    public void testConfigurationViewDeletion() throws Exception {
        // 1) Create table,
        String name = createSmallTable("DELETE");
        name = name.toUpperCase();

        // 2) Create statistics for new table.
        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("ANALYZE " + name)).getAll();

        // 3) Check statistics configuration presence.
        List<List<Object>> config = new ArrayList<>();
        config.add(Arrays.asList(SCHEMA, "TABLE", name, "A", (byte)15, null, null, null, null, 1L));
        config.add(Arrays.asList(SCHEMA, "TABLE", name, "B", (byte)15, null, null, null, null, 1L));
        config.add(Arrays.asList(SCHEMA, "TABLE", name, "C", (byte)15, null, null, null, null, 1L));

        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION where NAME = '" + name + "'", null, config::equals);

        // 4) Drop statistics for some column of new table.
        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("DROP STATISTICS " + name + "(A);")).getAll();

        // 5) Check statistics configuration without dropped column.
        List<Object> removed = config.remove(0);
        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION where NAME = '" + name + "'", null,
            act -> testContains(config, act) == null && testContains(Arrays.asList(removed), act) != null);

        // 6) Drop statistics for new table.
        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("DROP STATISTICS " + name)).getAll();

        // 7) Check statistics configuration without it.
        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION where NAME = '" + name + "'", null, List::isEmpty);
    }

    /**
     * Check partition from small table in statistics partition data view.
     */
    @Test
    public void testPartitionDataView() throws Exception {
        List<List<Object>> partLines = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", 0, null, null, null, 0L, null, null, null, null)
        );

        checkSqlResult("select * from SYS.STATISTICS_PARTITION_DATA where PARTITION < 10", null, act -> {
            checkContains(partLines, act);
            return true;
        });
    }

    /**
     * Check that all expected lines exist in actual. If not - fail.
     *
     * @param expected Expected lines, nulls mean any value.
     * @param actual Actual lines.
     */
    private void checkContains(List<List<Object>> expected, List<List<?>> actual) {
        List<Object> notExisting = testContains(expected, actual);
        if (notExisting != null)
            fail("Unable to found " + notExisting + " in specified dataset");
    }

    /**
     * Test that all expected lines exist in actual.
     *
     * @param expected Expected lines, nulls mean any value.
     * @param actual Actual lines.
     * @return First not existing line or {@code null} if all lines presented.
     */
    private List<Object> testContains(List<List<Object>> expected, List<List<?>> actual) {
        assertTrue(expected.size() <= actual.size());

        assertTrue("Test may take too long with such datasets of actual = " + actual.size(), actual.size() <= 1024);

        for (List<Object> exp : expected) {
            boolean found = false;

            for (List<?> act : actual) {
                found = checkEqualWithNull(exp, act);

                if (found)
                    break;
            }

            if (!found)
                return exp;
        }

        return null;
    }

    /**
     * Compare expected line with actual one.
     *
     * @param expected Expected line, {@code null} value mean any value.
     * @param actual Actual line.
     * @return {@code true} if line are equal, {@code false} - otherwise.
     */
    private boolean checkEqualWithNull(List<Object> expected, List<?> actual) {
        assertEquals(expected.size(), actual.size());

        for (int i = 0; i < expected.size(); i++) {
            Object exp = expected.get(i);
            Object act = actual.get(i);
            if (exp != null && !exp.equals(act) && act != null)
                return false;
        }

        return true;
    }

    /**
     * Check small table local data in statistics local data view.
     */
    @Test
    public void testLocalDataView() throws Exception {
        long size = SMALL_SIZE;
        ObjectStatisticsImpl smallStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(smallStat);

        Timestamp tsA = new Timestamp(smallStat.columnStatistics("A").createdAt());
        Timestamp tsB = new Timestamp(smallStat.columnStatistics("B").createdAt());
        Timestamp tsC = new Timestamp(smallStat.columnStatistics("C").createdAt());

        List<List<Object>> localData = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", size, size, 0L, size, 4, 1L, tsA.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", size, size, 0L, size, 4, 1L, tsB.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", size, 10L, 0L, size, 4, 1L, tsC.toString())
        );

        checkSqlResult("select * from SYS.STATISTICS_LOCAL_DATA", null, localData::equals);
    }

    /**
     * Check statistics rowCount=size, analyze with overridden values and check values.
     *
     * @throws Exception in case of error.
     */
    @Test
    public void testEnforceStatisticValues() throws Exception {
        long size = SMALL_SIZE;

        ObjectStatisticsImpl smallStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(smallStat);
        assertEquals(size, smallStat.rowCount());

        sql("DROP STATISTICS SMALL");

        sql("ANALYZE SMALL (A) WITH \"DISTINCT=5,NULLS=6,TOTAL=7,SIZE=8\"");
        sql("ANALYZE SMALL (B) WITH \"DISTINCT=6,NULLS=7,TOTAL=8\"");
        sql("ANALYZE SMALL (C)");

        checkSqlResult("select * from SYS.STATISTICS_LOCAL_DATA where NAME = 'SMALL'", null,
            list -> !list.isEmpty());

        smallStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(smallStat);
        assertEquals(8, smallStat.rowCount());

        Timestamp tsA = new Timestamp(smallStat.columnStatistics("A").createdAt());
        Timestamp tsB = new Timestamp(smallStat.columnStatistics("B").createdAt());
        Timestamp tsC = new Timestamp(smallStat.columnStatistics("C").createdAt());

        List<List<Object>> localData = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", 8L, 5L, 6L, 7L, 8, 3L, tsA.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", 8L, 6L, 7L, 8L, 4, 3L, tsB.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", 8L, 10L, 0L, size, 4, 3L, tsC.toString())
        );

        checkSqlResult("select * from SYS.STATISTICS_LOCAL_DATA where NAME = 'SMALL'", null,
            localData::equals);
    }
}
