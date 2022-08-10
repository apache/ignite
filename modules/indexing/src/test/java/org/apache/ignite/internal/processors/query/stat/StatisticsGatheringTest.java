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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;

/**
 * Test cluster wide gathering.
 */
public class StatisticsGatheringTest extends StatisticsRestartAbstractTest {
    /** {@inheritDoc} */
    @Override public int nodes() {
        return 2;
    }

    /**
     * Try to collect statistics on inactive cluster. Check that error will be thrown.
     *
     * @throws IgniteInterruptedCheckedException In case of error.
     */
    @Test
    public void testInactiveClusterGathering() throws IgniteInterruptedCheckedException {
        IgniteStatisticsManagerImpl statMgr0 = statisticsMgr(0);
        StatisticsTarget t101 = createStatisticTarget(101);

        sql("select * from SMALL101");
        statMgr0.statisticConfiguration().dropStatistics(Collections.singletonList(t101), false);

        grid(0).cluster().state(ClusterState.INACTIVE);

        GridTestUtils.assertThrows(null, () -> collectStatistics(StatisticsType.LOCAL, t101), IgniteException.class,
            "Unable to perform collect statistics due to cluster state [state=INACTIVE]");

        GridTestUtils.assertThrows(null, () -> collectStatistics(StatisticsType.GLOBAL, t101), IgniteException.class,
            "Unable to perform collect statistics due to cluster state [state=INACTIVE]");
    }

    /**
     * Test statistics gathering:
     * 1) Get local statistics from each nodes and check: not null, equals columns length, not null numbers
     * 2) Get global statistics (with delay) and check its equality in all nodes.
     */
    @Test
    public void testGathering() throws InterruptedException, IgniteCheckedException {
        ObjectStatisticsImpl localStats[] = getStats("SMALL", StatisticsType.LOCAL);

        testCond(Objects::nonNull, localStats);

        testCond(stat -> stat.columnsStatistics().size() == localStats[0].columnsStatistics().size(), localStats);

        testCond(this::checkStat, localStats);

        ObjectStatisticsImpl globalStat = getStatsFromNode(0, "SMALL", StatisticsType.GLOBAL);

        assertNotNull(globalStat);
    }

    /**
     * Test that all node contains the same global statistics.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testGlobalIsEqual() throws Exception {
        ObjectStatisticsImpl globalStats[] = getStats("SMALL", StatisticsType.GLOBAL);

        testCond(Objects::nonNull, globalStats);
        testCond(this::checkStat, globalStats);

        ObjectStatisticsImpl globalStat = globalStats[0];

        assertTrue(globalStats.length > 1);

        for (int i = 1; i < globalStats.length; i++)
            testEquaData(globalStat, globalStats[i]);
    }

    /**
     * Check specified statistics contains equal data (all, except collection time and versions).
     *
     * @param expected Expected statistics.
     * @param actual Actual statistics.
     */
    private static void testEquaData(ObjectStatisticsImpl expected, ObjectStatisticsImpl actual) {
        assertEquals(expected.rowCount(), actual.rowCount());

        assertEquals(expected.columnsStatistics().size(), actual.columnsStatistics().size());

        for (Map.Entry<String, ColumnStatistics> expectedColStatEntry : expected.columnsStatistics().entrySet()) {
            ColumnStatistics expColStat = expectedColStatEntry.getValue();
            ColumnStatistics actColStat = actual.columnStatistics(expectedColStatEntry.getKey());

            assertNotNull(actColStat);
            assertEquals(expColStat.min() != null ? expColStat.min() : null,
                actColStat.min() != null ? actColStat.min() : null);
            assertEquals(expColStat.max() != null ? expColStat.max() : null,
                actColStat.max() != null ? actColStat.max() : null);
            assertEquals(expColStat.size(), actColStat.size());
            assertEquals(expColStat.distinct(), actColStat.distinct());
            assertEquals(expColStat.total(), actColStat.total());
            assertEquals(expColStat.nulls(), actColStat.nulls());
        }
    }

    /**
     * Collect statistics for group of object at once and check it collected in each node.
     */
    @Test
    public void testGroupGathering() {
        StatisticsTarget t100 = createStatisticTarget(100);
        StatisticsTarget t101 = createStatisticTarget(101);
        StatisticsTarget tWrong = new StatisticsTarget(t101.schema(), t101.obj() + "wrong");

        GridTestUtils.assertThrows(
            log,
            () -> statisticsMgr(0).collectStatistics(buildDefaultConfigurations(t100, t101, tWrong)),
            IgniteException.class,
            "Table doesn't exist [schema=PUBLIC, table=SMALL101wrong]"
        );

        updateStatistics(StatisticsType.GLOBAL, t100, t101);

        ObjectStatisticsImpl[] stats100 = getStats(t100.obj(), StatisticsType.LOCAL);
        ObjectStatisticsImpl[] stats101 = getStats(t101.obj(), StatisticsType.LOCAL);

        testCond(this::checkStat, stats100);
        testCond(this::checkStat, stats101);
    }

    /**
     * Check if specified SMALL table stats is OK (have not null values).
     *
     * @param stat Object statistics to check.
     * @return {@code true} if statistics OK, otherwise - throws exception.
     */
    private boolean checkStat(ObjectStatisticsImpl stat) {
        assertTrue(stat.columnStatistics("A").total() > 0);
        assertTrue(stat.columnStatistics("B").distinct() > 0);
        ColumnStatistics statC = stat.columnStatistics("C");
        assertTrue(statC.min() != null);
        assertTrue(statC.max() != null);
        assertTrue(statC.size() > 0);
        assertTrue(statC.nulls() == 0);
        assertTrue(statC.raw().length > 0);
        assertTrue(statC.total() == stat.rowCount());

        return true;
    }
}
