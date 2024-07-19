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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;
import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManagerImpl.OBSOLESCENCE_INTERVAL;

/**
 * Test for statistics obsolescence.
 */
public class StatisticsObsolescenceTest extends StatisticsAbstractTest {
    /** */
    private long testTimeout = super.getTestTimeout();

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return testTimeout;
    }

    /** */
    @Test
    public void testObsolescenceWithInserting() throws Exception {
        doTestObsolescenceUnderLoad(0, 0L, 0L, 1,
            key -> sql(String.format("INSERT INTO small(a, b, c) VALUES(%d, %d, %d)", key, key, key)));
    }

    /** */
    @Test
    public void testObsolescenceWithUpdating() throws Exception {
        doTestObsolescenceUnderLoad(10000, 10000L, 1L, 0, key -> sql("UPDATE small set b=b+1 where a=" + key));
    }

    /** */
    private void doTestObsolescenceUnderLoad(int preloadCnt, long maxKey, long firstKey, int rowCntCmp, Consumer<Long> crud) throws Exception {
        // Ensured IgniteStatisticsManagerImpl#OBSOLESCENCE_INTERVAL.
        testTimeout = 3L * OBSOLESCENCE_INTERVAL * 1000L;

        AtomicBoolean stop = new AtomicBoolean();

        try {
            startGridsMultiThreaded(2);

            createSmallTable(preloadCnt, null);

            statisticsMgr(0).usageState(StatisticsUsageState.ON);
            statisticsMgr(0).collectStatistics(buildDefaultConfigurations(SMALL_TARGET));

            // Initialized statistics.
            assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(0).getLocalStatistics(SMALL_KEY) != null, getTestTimeout()));
            assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(1).getLocalStatistics(SMALL_KEY) != null, getTestTimeout()));

            ObjectStatisticsImpl initStat1 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);
            ObjectStatisticsImpl initStat2 = (ObjectStatisticsImpl)statisticsMgr(1).getLocalStatistics(SMALL_KEY);

            assertEquals(preloadCnt, initStat1.rowCount() + initStat2.rowCount());

            GridTestUtils.runAsync(() -> {
                AtomicLong key = new AtomicLong(firstKey);

                long a;

                while (!stop.get()) {
                    a = key.incrementAndGet();

                    if (maxKey > 0 && a > maxKey)
                        key.set(a = firstKey);

                    crud.accept(a);
                }
            });

            waitForStatsUpdates(initStat1);

            ObjectStatisticsImpl updatedStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            assertTrue(rowCntCmp > 0 ? updatedStat.rowCount() > initStat1.rowCount() :
                (rowCntCmp < 0 ? initStat1.rowCount() < updatedStat.rowCount() : initStat1.rowCount() == updatedStat.rowCount()));

            // Continuing data loading, the table is being updated. Since the row count is inreasing, we must obtain a
            // new statistics, greather than {@code firstNotEmpty}.
            waitForStatsUpdates(updatedStat);

            ObjectStatisticsImpl updatedStat2 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            assertTrue(rowCntCmp > 0 ? updatedStat2.rowCount() > updatedStat.rowCount() :
                (rowCntCmp < 0 ? updatedStat2.rowCount() < updatedStat.rowCount() : updatedStat2.rowCount() == updatedStat.rowCount()));
        }
        finally {
            stop.set(true);
        }
    }

    /** */
    private void waitForStatsUpdates(ObjectStatisticsImpl compareTo) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() -> {
            ObjectStatisticsImpl updatedStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            if (updatedStat == null)
                return false;

            AtomicBoolean passed = new AtomicBoolean(true);

            updatedStat.columnsStatistics().forEach((col, stat) -> {
                ColumnStatistics compared = compareTo.columnStatistics(col);

                assert compared != null;

                if (compared.createdAt() >= stat.createdAt())
                    passed.set(false);
            });

            return passed.get();
        }, getTestTimeout()));
    }

    /**
     * Test statistics refreshing after significant changes of base table:
     * 1) Create and populate small table
     * 2) Analyze it and get local statistics
     * 3) Insert same number of rows into small table
     * 4) Check that statistics refreshed and its values changed.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testObsolescence() throws Exception {
        startGridsMultiThreaded(1);

        createSmallTable(null);

        statisticsMgr(0).collectStatistics(buildDefaultConfigurations(SMALL_TARGET));

        assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(0).getLocalStatistics(SMALL_KEY) != null, TIMEOUT));

        ObjectStatisticsImpl stat1 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(stat1);

        for (int i = SMALL_SIZE; i < 2 * SMALL_SIZE; i++)
            sql(String.format("INSERT INTO small(a, b, c) VALUES(%d, %d, %d)", i, i, i % 10));

        statisticsMgr(0).processObsolescence();

        assertTrue(GridTestUtils.waitForCondition(() -> {
            ObjectStatisticsImpl stat2 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            return stat2 != null && stat2.rowCount() > stat1.rowCount();
        }, TIMEOUT));
    }

    /**
     * Test activation with statistics with topology changes.
     *
     * 1) Start two node cluster.
     * 2) Activate cluster.
     * 3) Create table and analyze it.
     * 4) Inactivate cluster and change it's topology.
     * 5) Get obsolescence map size for created table.
     * 6) Activate cluster again.
     * 7) Check that obsolescence map size changed due to new topology.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testInactiveLoad() throws Exception {
        Ignite ignite = startGrid(0);
        Ignite ignite1 = startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        createSmallTable(null);
        collectStatistics(StatisticsType.GLOBAL, "SMALL");

        ignite.cluster().state(ClusterState.INACTIVE);

        ignite1.close();

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> statObs = GridTestUtils
            .getFieldValue(statisticsMgr(0).statisticsRepository(), "statObs");

        Integer oldSize = statObs.get(SMALL_KEY).size();

        ignite.cluster().state(ClusterState.ACTIVE);

        assertTrue(GridTestUtils.waitForCondition(() -> statObs.get(SMALL_KEY).size() > oldSize, TIMEOUT));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }
}
