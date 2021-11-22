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

import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for statistics storage.
 */
public abstract class StatisticsStorageTest extends StatisticsStorageAbstractTest {
    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        collectStatistics(SMALL_TARGET);
    }

    /**
     * Test that statistics manager will return local statistics after cleaning of statistics repository.
     */
    @Test
    public void clearAllTest() {
        IgniteStatisticsRepository statsRepo = statisticsMgr(0).statisticsRepository();
        IgniteStatisticsStore statsStore = statsRepo.statisticsStore();

        statsStore.clearAllStatistics();

        ObjectStatistics locStat = statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(locStat);
    }

    /**
     * Collect statistics by whole table twice and compare collected statistics:
     * 1) collect statistics by table and get it.
     * 2) collect same statistics by table again and get it.
     * 3) compare obtained statistics.
     */
    @Test
    public void testRecollection() throws Exception {
        updateStatistics(SMALL_TARGET);

        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        updateStatistics(SMALL_TARGET);

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat2.columnsStatistics().values()) {
            GridTestUtils.setFieldValue(c, "ver", 0);
            GridTestUtils.setFieldValue(c, "createdAt", 0);
        }

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat.columnsStatistics().values()) {
            GridTestUtils.setFieldValue(c, "ver", 0);
            GridTestUtils.setFieldValue(c, "createdAt", 0);
        }

        assertEquals(locStat, locStat2);
    }

    /**
     * Collect statistics partially twice and compare collected statistics:
     * 1) collect statistics by column and get it.
     * 2) collect same statistics by column again and get it.
     * 3) compare obtained statistics.
     *
     */
    @Test
    public void testPartialRecollection() throws Exception {
        updateStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));
        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl)statisticsMgr(0)
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"));

        updateStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));
        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl)statisticsMgr(0)
            .getLocalStatistics(new StatisticsKey(SCHEMA, "SMALL"));

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat2.columnsStatistics().values()) {
            GridTestUtils.setFieldValue(c, "ver", 0);
            GridTestUtils.setFieldValue(c, "createdAt", 0);
        }

        // Reset version to compare statistic.
        for (ColumnStatistics c : locStat.columnsStatistics().values()) {
            GridTestUtils.setFieldValue(c, "ver", 0);
            GridTestUtils.setFieldValue(c, "createdAt", 0);
        }

        assertEquals(locStat, locStat2);
    }

    /**
     * 1) Check that statistics available and usage state is ON
     * 2) Switch usage state to NO_UPDATE and check that statistics still availabel
     * 3) Disable statistics usage (OFF) and check it became unavailable.
     * 4) Turn ON again and check statistics availability
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testDisableGet() throws Exception {
        assertNotNull(statisticsMgr(0).getLocalStatistics(SMALL_KEY));
        assertNotNull(statisticsMgr(1).getLocalStatistics(SMALL_KEY));

        statisticsMgr(0).usageState(StatisticsUsageState.NO_UPDATE);

        assertNotNull(statisticsMgr(0).getLocalStatistics(SMALL_KEY));
        assertNotNull(statisticsMgr(1).getLocalStatistics(SMALL_KEY));

        statisticsMgr(0).usageState(StatisticsUsageState.OFF);

        assertNull(statisticsMgr(0).getLocalStatistics(SMALL_KEY));
        assertNull(statisticsMgr(1).getLocalStatistics(SMALL_KEY));

        statisticsMgr(0).usageState(StatisticsUsageState.ON);

        assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(0).getLocalStatistics(SMALL_KEY) != null, TIMEOUT));
        assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(1).getLocalStatistics(SMALL_KEY) != null, TIMEOUT));
    }

    /**
     * Clear statistics twice and check that .
     */
    @Test
    public void testDoubleDeletion() throws Exception {
        statisticsMgr(0).dropStatistics(SMALL_TARGET);

        assertTrue(GridTestUtils.waitForCondition(() -> null == statisticsMgr(0).getLocalStatistics(SMALL_KEY), TIMEOUT));

        GridTestUtils.assertThrows(
            log,
            () -> statisticsMgr(0).dropStatistics(SMALL_TARGET),
            IgniteException.class,
            "Statistic doesn't exist for [schema=PUBLIC, obj=SMALL]"
        );

        Thread.sleep(TIMEOUT);

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNull(locStat2);
    }

    /**
     * Clear statistics partially twice.
     */
    @Test
    public void testDoublePartialDeletion() throws Exception {
        statisticsMgr(0).dropStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B"));

        assertTrue(GridTestUtils.waitForCondition(() -> null == ((ObjectStatisticsImpl) statisticsMgr(0)
            .getLocalStatistics(SMALL_KEY)).columnsStatistics().get("B"), TIMEOUT));

        ObjectStatisticsImpl locStat = (ObjectStatisticsImpl) statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(locStat);
        assertNotNull(locStat.columnsStatistics().get("A"));

        GridTestUtils.assertThrows(
            log,
            () -> statisticsMgr(0).dropStatistics(new StatisticsTarget(SCHEMA, "SMALL", "B")),
            IgniteException.class,
            "Statistic doesn't exist for [schema=PUBLIC, obj=SMALL, col=B]"
        );

        Thread.sleep(TIMEOUT);

        ObjectStatisticsImpl locStat2 = (ObjectStatisticsImpl) statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(locStat2);
        assertNotNull(locStat.columnsStatistics().get("A"));
        assertNull(locStat.columnsStatistics().get("B"));
    }
}
