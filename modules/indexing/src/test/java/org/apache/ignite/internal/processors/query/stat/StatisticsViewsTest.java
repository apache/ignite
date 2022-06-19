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
import java.util.Collections;
import java.util.List;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnOverrides;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
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
        collectStatistics(StatisticsType.GLOBAL, SMALL_TARGET);
    }


    /**
     * Check small table configuration in statistics column configuration view.
     */
    @Test
    public void testConfigurationView() throws Exception {
        collectStatistics(StatisticsType.GLOBAL, SMALL_TARGET);

        ObjectStatisticsImpl smallStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        long aVer = smallStat.columnStatistics("A").version();
        long bVer = smallStat.columnStatistics("B").version();
        long cVer = smallStat.columnStatistics("C").version();

        List<List<Object>> config = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", (byte)15, null, null, null, null, aVer),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", (byte)15, null, null, null, null, bVer),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", (byte)15, null, null, null, null, cVer)
        );

        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION where name = 'SMALL'", null, config::equals);
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
        // TODO: revert after IGNITE-15455
        //grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("ANALYZE " + name)).getAll();
        collectStatistics(StatisticsType.GLOBAL, name);

        // 3) Check statistics configuration presence.
        List<List<Object>> config = new ArrayList<>();
        config.add(Arrays.asList(SCHEMA, "TABLE", name, "A", (byte)15, null, null, null, null, 1L));
        config.add(Arrays.asList(SCHEMA, "TABLE", name, "B", (byte)15, null, null, null, null, 1L));
        config.add(Arrays.asList(SCHEMA, "TABLE", name, "C", (byte)15, null, null, null, null, 1L));

        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION where NAME = '" + name + "'", null, config::equals);

        // 4) Drop statistics for some column of new table.
        //grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("DROP STATISTICS " + name + "(A);")).getAll();
        statisticsMgr(0).statisticConfiguration().dropStatistics(
            Collections.singletonList(new StatisticsTarget(SCHEMA, name, "A")), true);

        // 5) Check statistics configuration without dropped column.
        List<Object> removed = config.remove(0);
        checkSqlResult("select * from SYS.STATISTICS_CONFIGURATION where NAME = '" + name + "'", null,
            act -> testContains(config, act) == null && testContains(Arrays.asList(removed), act) != null);

        // 6) Drop statistics for new table.
        // TODO: revert after IGNITE-15455
        //grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("DROP STATISTICS " + name)).getAll();
        statisticsMgr(0).statisticConfiguration().dropStatistics(
            Collections.singletonList(new StatisticsTarget(SCHEMA, name)), true);

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

        Configurator.setLevel(StatisticsProcessor.class.getName(), Level.TRACE);
        ObjectStatisticsImpl smallStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(smallStat);
        assertEquals(size, smallStat.rowCount());

        // TODO: revert after IGNITE-15455
        // sql("DROP STATISTICS SMALL");

        // sql("ANALYZE SMALL (A) WITH \"DISTINCT=5,NULLS=6,TOTAL=7,SIZE=8\"");
        // sql("ANALYZE SMALL (B) WITH \"DISTINCT=6,NULLS=7,TOTAL=8\"");
        // sql("ANALYZE SMALL (C)");
        IgniteStatisticsConfigurationManager cfgMgr = statisticsMgr(0).statisticConfiguration();

        cfgMgr.dropStatistics(Collections.singletonList(SMALL_TARGET), true);

        StatisticsColumnConfiguration aCfg = new StatisticsColumnConfiguration("A",
            new StatisticsColumnOverrides(6L, 5L, 7L, 8));
        StatisticsObjectConfiguration smallACfg = new StatisticsObjectConfiguration(SMALL_KEY,
            Collections.singleton(aCfg), StatisticsObjectConfiguration.DEFAULT_OBSOLESCENCE_MAX_PERCENT);

        cfgMgr.updateStatistics(smallACfg);

        StatisticsColumnConfiguration bCfg = new StatisticsColumnConfiguration("B",
            new StatisticsColumnOverrides(7L, 6L, 8L, null));
        StatisticsObjectConfiguration smallBCfg = new StatisticsObjectConfiguration(SMALL_KEY,
            Collections.singleton(bCfg), StatisticsObjectConfiguration.DEFAULT_OBSOLESCENCE_MAX_PERCENT);

        cfgMgr.updateStatistics(smallBCfg);

        StatisticsColumnConfiguration cCfg = new StatisticsColumnConfiguration("C", null);
        StatisticsObjectConfiguration smallCCfg = new StatisticsObjectConfiguration(SMALL_KEY,
            Collections.singleton(cCfg), StatisticsObjectConfiguration.DEFAULT_OBSOLESCENCE_MAX_PERCENT);

        cfgMgr.updateStatistics(smallCCfg);

        checkSqlResult("select * from SYS.STATISTICS_LOCAL_DATA where NAME = 'SMALL' and COLUMN = 'C'", null,
            list -> !list.isEmpty());

        assertTrue(GridTestUtils.waitForCondition(() -> {
            ObjectStatisticsImpl stat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            return stat != null && stat.rowCount() == 8;
        }, TIMEOUT));

        smallStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        Timestamp tsA = new Timestamp(smallStat.columnStatistics("A").createdAt());
        Timestamp tsB = new Timestamp(smallStat.columnStatistics("B").createdAt());
        Timestamp tsC = new Timestamp(smallStat.columnStatistics("C").createdAt());

        long aVer = smallStat.columnStatistics("A").version();
        long bVer = smallStat.columnStatistics("B").version();
        long cVer = smallStat.columnStatistics("C").version();

        List<List<Object>> localData = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", 8L, 5L, 6L, 7L, 8, aVer, tsA.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", 8L, 6L, 7L, 8L, 4, bVer, tsB.toString()),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", 8L, 10L, 0L, size, 4, cVer, tsC.toString())
        );

        checkSqlResult("select * from SYS.STATISTICS_LOCAL_DATA where NAME = 'SMALL'", null,
            localData::equals);
    }
}
