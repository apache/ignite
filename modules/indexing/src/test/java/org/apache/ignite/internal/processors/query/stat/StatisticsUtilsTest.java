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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for StatisticsUtils methods
 */
public class StatisticsUtilsTest extends GridCommonAbstractTest {
    /** Test key. */
    private static final StatisticsKey KEY = new StatisticsKey("SCHEMA", "TABLE");

    /** Test COL1 statistics. */
    private static final ColumnStatistics COL_1_STAT =
        new ColumnStatistics(null, null, 0, 0, 0, 0, null, 1, 0);

    /** Test COL1 statistics configuration. */
    private static final StatisticsColumnConfiguration COL_1_CFG =
        new StatisticsColumnConfiguration("COL1", null);

    /** Test COL2 statistics. */
    private static final ColumnStatistics COL_2_STAT =
        new ColumnStatistics(null, null, 0, 0, 0, 0, null, 2, 0);

    /** Test COL2 statistics configuration. */
    private static final StatisticsColumnConfiguration COL_2_CFG =
        new StatisticsColumnConfiguration("COL2", null).refresh();

    /** Test COL3 statistics. */
    private static final ColumnStatistics COL_3_STAT =
        new ColumnStatistics(null, null, 0, 0, 0, 0, null, 3, 0);

    /** Test COL3 statistics configuration. */
    private static final StatisticsColumnConfiguration COL_3_CFG =
        new StatisticsColumnConfiguration("COL3", null).refresh().refresh();

    /** Test required version. */
    private static final Map<String, Long> VERSIONS = F.asMap("COL1", 1L, "COL2", 2L);

    /**
     * Test checkStatisticsVersions with stat equals to {@code null} returns {@code false}.
     */
    @Test
    public void testCheckStatisticsVersionsNullStat() {
        assertFalse(StatisticsUtils.checkStatisticsVersions(null, VERSIONS));
    }

    /**
     * Test checkStatisticsVersions with stat contains no columns returns {@code false}.
     */
    @Test
    public void testtestCheckStatisticsVersionsEmptyStat() {
        ObjectStatisticsImpl objStat = new ObjectStatisticsImpl(100, Collections.emptyMap());

        assertFalse(StatisticsUtils.checkStatisticsVersions(objStat, VERSIONS));
    }

    /**
     * Test checkStatisticsVersions with stat contains all columns returns {@code true}.
     */
    @Test
    public void testCheckStatisticsVersionsAllStat() {
        Map<String, ColumnStatistics> colStats = F.asMap("COL1", COL_1_STAT, "COL2", COL_2_STAT);
        ObjectStatisticsImpl objStat = new ObjectStatisticsImpl(100, colStats);

        assertTrue(StatisticsUtils.checkStatisticsVersions(objStat, VERSIONS));
    }

    /**
     * Test checkStatisticsVersions with stat with never columns returns {@code true}.
     */
    @Test
    public void testCheckStatisticsVersionsNeverStat() {
        Map<String, ColumnStatistics> colStats = F.asMap("COL1", COL_1_STAT, "COL2", COL_3_STAT);
        ObjectStatisticsImpl objStat = new ObjectStatisticsImpl(100, colStats);

        assertTrue(StatisticsUtils.checkStatisticsVersions(objStat, VERSIONS));
    }

    /**
     * Test checkStatisticsVersions with stat with extra columns returns {@code true}.
     */
    @Test
    public void testCheckStatisticsVersionsExtraColumn() {
        Map<String, ColumnStatistics> colStats =
            F.asMap("COL1", COL_1_STAT, "COL2", COL_3_STAT, "COL3", COL_2_STAT);
        ObjectStatisticsImpl objStat = new ObjectStatisticsImpl(100, colStats);

        assertTrue(StatisticsUtils.checkStatisticsVersions(objStat, VERSIONS));
    }

    /**
     * Test checkStatisticsConfigurationVersions with statistics configuration equals to {@code null}
     * returns {@code false}.
     */
    @Test
    public void testCheckStatisticsConfigurationVersionsNull() {
        assertFalse(StatisticsUtils.checkStatisticsConfigurationVersions(null, VERSIONS));
    }

    /**
     * Test checkStatisticsConfigurationVersions with stat contains no columns returns {@code false}.
     */
    @Test
    public void testtestCheckStatisticsConfigurationVersionsEmptyStat() {
        StatisticsObjectConfiguration objCfg = new StatisticsObjectConfiguration(KEY);

        assertFalse(StatisticsUtils.checkStatisticsConfigurationVersions(objCfg, VERSIONS));
    }

    /**
     * Test checkStatisticsConfigurationVersions with stat contains all columns returns {@code true}.
     */
    @Test
    public void testCheckStatisticsConfigurationVersionsAllStat() {
        List<StatisticsColumnConfiguration> colCfgs = Arrays.asList(COL_1_CFG, COL_2_CFG);
        StatisticsObjectConfiguration objCfg = new StatisticsObjectConfiguration(KEY, colCfgs, (byte)50);

        assertTrue(StatisticsUtils.checkStatisticsConfigurationVersions(objCfg, VERSIONS));
    }

    /**
     * Test checkStatisticsConfigurationVersions with stat with never columns returns {@code true}.
     */
    @Test
    public void testCheckStatisticsConfigurationVersionsNeverStat() {
        List<StatisticsColumnConfiguration> colCfgs = Arrays.asList(COL_1_CFG, COL_2_CFG.refresh());
        StatisticsObjectConfiguration objCfg = new StatisticsObjectConfiguration(KEY, colCfgs, (byte)50);

        assertTrue(StatisticsUtils.checkStatisticsConfigurationVersions(objCfg, VERSIONS));
    }

    /**
     * Test checkStatisticsConfigurationVersions with stat with extra columns returns {@code true}.
     */
    @Test
    public void testCheckStatisticsConfigurationVersionsExtraColumn() {
        List<StatisticsColumnConfiguration> colCfgs = Arrays.asList(COL_1_CFG, COL_2_CFG, COL_3_CFG);
        StatisticsObjectConfiguration objCfg = new StatisticsObjectConfiguration(KEY, colCfgs, (byte)50);

        assertTrue(StatisticsUtils.checkStatisticsConfigurationVersions(objCfg, VERSIONS));
    }
}
