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
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test for statistics obsolescence.
 */
public class StatisticsObsolescenceTest extends StatisticsAbstractTest {
    /** */
    private long testTimeout;

    /** */
    @Test
    public void testObsolescenceWithInsert() throws Exception {
        doTestObsolescenceUnderLoad(false, 1,
            key -> sql(String.format("insert into SMALL(A, B, C) values(%d, %d, %d)", key, key, key)));
    }

    /** */
    @Test
    public void testObsolescenceWithUpdate() throws Exception {
        doTestObsolescenceUnderLoad(true, 0, key -> sql("update SMALL set B=B+1 where A=" + key));
    }

    /** */
    @Test
    public void testObsolescenceWithDelete() throws Exception {
        doTestObsolescenceUnderLoad(true, -1, key -> sql("delete from SMALL where A=" + key));
    }

    /** */
    private void doTestObsolescenceUnderLoad(boolean preload, int rowCntCmp, Consumer<Long> op) throws Exception {
        int preloadCnt = preload ? 1000 : 0;
        long opFirstKey = 1L;
        long opDelayKeysCnt = 300L;

        // Ensured IgniteStatisticsManagerImpl#OBSOLESCENCE_INTERVAL.
        testTimeout = 3L * OBSOLESCENCE_INTERVAL * 1000L;

        AtomicBoolean stop = new AtomicBoolean();

        try {
            startGridsMultiThreaded(2);

            createSmallTable(preloadCnt, null);

            statisticsMgr(0).usageState(StatisticsUsageState.ON);
            statisticsMgr(0).collectStatistics(buildDefaultConfigurations(SMALL_TARGET));

            // Initialized statistics.
            assertTrue(waitForCondition(() -> statisticsMgr(0).getLocalStatistics(SMALL_KEY) != null, getTestTimeout()));
            assertTrue(waitForCondition(() -> statisticsMgr(1).getLocalStatistics(SMALL_KEY) != null, getTestTimeout()));

            ObjectStatisticsImpl initStat1 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);
            ObjectStatisticsImpl initStat2 = (ObjectStatisticsImpl)statisticsMgr(1).getLocalStatistics(SMALL_KEY);

            assertEquals(preloadCnt, initStat1.rowCount() + initStat2.rowCount());

            AtomicBoolean statReadyFlag = new AtomicBoolean(false);

            GridTestUtils.runAsync(() -> {
                AtomicLong key = new AtomicLong(opFirstKey);

                long opCnt = 0;

                while (!stop.get()) {
                    op.accept(key.getAndIncrement());

                    if (++opCnt == opDelayKeysCnt) {
                        opCnt = 0;

                        statReadyFlag.set(true);

                        assertTrue(waitForCondition(() -> stop.get() || !statReadyFlag.get(), getTestTimeout()));
                    }
                }
            });

            assertTrue(waitForCondition(statReadyFlag::get, getTestTimeout()));

            waitForStatsUpdates(initStat1);

            ObjectStatisticsImpl updatedStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            assertTrue(rowCntCmp > 0 ? updatedStat.rowCount() > initStat1.rowCount() :
                (rowCntCmp < 0 ? updatedStat.rowCount() < initStat1.rowCount() : updatedStat.rowCount() == initStat1.rowCount()));

            statReadyFlag.set(false);

            assertTrue(waitForCondition(statReadyFlag::get, getTestTimeout()));

            // Continuing data loading, the table is being updated. Since the row count is inreasing, we must obtain a
            // new statistics, greather than {@code firstNotEmpty}.
            waitForStatsUpdates(updatedStat);

            ObjectStatisticsImpl finalStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            assertTrue(rowCntCmp > 0 ? finalStat.rowCount() > updatedStat.rowCount() :
                (rowCntCmp < 0 ? finalStat.rowCount() < updatedStat.rowCount() : finalStat.rowCount() == updatedStat.rowCount()));
        }
        finally {
            stop.set(true);
        }
    }

    /** */
    private void waitForStatsUpdates(ObjectStatisticsImpl compareTo) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
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

        assertTrue(waitForCondition(() -> statisticsMgr(0).getLocalStatistics(SMALL_KEY) != null, TIMEOUT));

        ObjectStatisticsImpl stat1 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

        assertNotNull(stat1);

        for (int i = SMALL_SIZE; i < 2 * SMALL_SIZE; i++)
            sql(String.format("INSERT INTO small(a, b, c) VALUES(%d, %d, %d)", i, i, i % 10));

        statisticsMgr(0).processObsolescence();

        assertTrue(waitForCondition(() -> {
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

        assertTrue(waitForCondition(() -> statObs.get(SMALL_KEY).size() > oldSize, TIMEOUT));
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
    @Override protected long getTestTimeout() {
        return testTimeout > 0 ? testTimeout : super.getTestTimeout();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }
}
