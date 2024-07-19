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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;

/**
 * Test for statistics obsolescence.
 */
public class StatisticsObsolescenceTest extends StatisticsAbstractTest {
    /** Ensured IgniteStatisticsManagerImpl#OBSOLESCENCE_INTERVAL. 5 minutes. */
    @Override protected long getTestTimeout() {
        return 5L * 60L * 1000L;
    }

    /** */
    @Test
    public void testObsolescenceUnderLoad() throws Exception {
        int servers = 2;

        AtomicBoolean stop = new AtomicBoolean();

        try {
            startGridsMultiThreaded(servers);

            long top = grid(0).cluster().topologyVersion();

            createSmallTable(0, null);

            statisticsMgr(0).usageState(StatisticsUsageState.ON);

            if (log.isInfoEnabled())
                log.info("Enabling statistic for the table " + SMALL_TARGET);

            StatisticsObjectConfiguration[] statCfgs = buildDefaultConfigurations(SMALL_TARGET);

            // This FIXES the test (workaround).
            // statCfgs[0] = new StatisticsObjectConfiguration(statCfgs[0].key(), statCfgs[0].columns().values(), (byte)-1);

            statisticsMgr(0).collectStatistics(statCfgs);

            // Initialized, empty statistics.
            assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(0).getLocalStatistics(SMALL_KEY) != null, getTestTimeout()));
            assertTrue(GridTestUtils.waitForCondition(() -> statisticsMgr(1).getLocalStatistics(SMALL_KEY) != null, getTestTimeout()));

            if (log.isInfoEnabled())
                log.info("Got first, empty statistic of the table " + SMALL_TARGET);

            ObjectStatisticsImpl emptyStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            assertTrue(emptyStat.rowCount() == 0);

            GridTestUtils.runAsync(() -> {
                AtomicLong key = new AtomicLong();

                if (log.isInfoEnabled())
                    log.info("Starting the loading...");

                long time = System.nanoTime();

                while (!stop.get()) {
                    sql(String.format("INSERT INTO small(a, b, c) VALUES(%d, %d, %d)", key.incrementAndGet(), key.get(), key.get() % 10));

                    if (U.nanosToMillis(System.nanoTime() - time) > 10_000L) {
                        time = System.nanoTime();

                        if (log.isInfoEnabled())
                            log.info("Loaded " + grid(0).cache("SMALLnull").size() + " records.");
                    }
                }

                if (log.isInfoEnabled())
                    log.info("The loading stopped.");
            });

            // Here we get non-zero, updated statistics.
            assertTrue(GridTestUtils.waitForCondition(() -> {
                ObjectStatisticsImpl updatedStat = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

                return updatedStat != null && updatedStat.rowCount() > emptyStat.rowCount();
            }, getTestTimeout()));

            // First not empty statistics (rowCount > 0).
            ObjectStatisticsImpl firstNotEmpty = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            assertTrue(firstNotEmpty.rowCount() > 0);

            if (log.isInfoEnabled())
                log.info("Got first not empty statistic: " + firstNotEmpty);

            // FAILS here with a timeout.
            // Continuing data loading, the table is being updated. Since the row count is inreasing, we must obtain a
            // new statistics, greather than {@code firstNotEmpty}.
            assertTrue(GridTestUtils.waitForCondition(() -> {
                ObjectStatisticsImpl updatedStat2 = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

                return updatedStat2 != null && updatedStat2.rowCount() > firstNotEmpty.rowCount();
            }, getTestTimeout()));

            // Ensure the topology hasn't changed and didn't trigger the statistics.
            assertEquals(top, grid(0).cluster().topologyVersion());

            ObjectStatisticsImpl secondNotEmpty = (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(SMALL_KEY);

            if (log.isInfoEnabled())
                log.info("Got second not empty statistic: " + secondNotEmpty);
        }
        finally {
            stop.set(true);
        }
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
