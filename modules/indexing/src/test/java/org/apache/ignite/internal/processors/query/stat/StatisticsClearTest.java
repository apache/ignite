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

import java.util.function.Consumer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;

/**
 * Statistics cleaning tests.
 */
public class StatisticsClearTest extends StatisticsRestartAbstractTest {
    /** {@inheritDoc} */
    @Override public int nodes() {
        return 2;
    }

    /**
     * 1) Collect statistics on two nodes cluster.
     * 2) Test that it available on each node.
     * 3) Clear statistics.
     * 4) Test that it cleaned on each node.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testStatisticsClear() throws Exception {
        updateStatistics(SMALL_TARGET);

        Assert.assertNotNull(statisticsMgr(0).getLocalStatistics(SMALL_KEY));

        Assert.assertNotNull(statisticsMgr(1).getLocalStatistics(SMALL_KEY));

        statisticsMgr(1).dropStatistics(SMALL_TARGET);

        GridTestUtils.waitForCondition(
            () -> null == statisticsMgr(0).getLocalStatistics(SMALL_KEY)
                && null == statisticsMgr(1).getLocalStatistics(SMALL_KEY), TIMEOUT);

        statisticsMgr(1).collectStatistics(buildDefaultConfigurations(SMALL_TARGET));
    }

    /**
     * Clear statistics by non existing table.
     *
     * @throws Exception In case of errors.
     */
    @Test(expected = Throwable.class)
    public void testStatisticsClearOnNotExistingTable() throws Exception {
        statisticsMgr(1).dropStatistics(new StatisticsTarget(SCHEMA, "NO_NAME"));
    }

    /**
     * Acquire statistics by non existing table.
     */
    @Test
    public void testGetNonExistingTableStatistics() {
        Assert.assertNull(statisticsMgr(0).getLocalStatistics(new StatisticsKey(SCHEMA, "NO_NAME")));
        Assert.assertNull(statisticsMgr(1).getLocalStatistics(new StatisticsKey(SCHEMA, "NO_NAME")));
    }

    /**
     * 1) Restart without statistics version
     * 2) Check that statistics was refreshed.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testRestartWrongVersion() throws Exception {
        testRestartVersion(metaStorage -> {
            try {
                metaStorage.write("stats.version", 2);
            }
            catch (IgniteCheckedException e) {
                Assert.fail();
            }
        });
    }

    /**
     * Test without any statistics version.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testRestartNoVersion() throws Exception {
        testRestartVersion(metaStorage -> {
            try {
                metaStorage.remove("stats.version");
            }
            catch (IgniteCheckedException e) {
                Assert.fail();
            }
        });
    }

    /**
     * Test with corrupted (from the current version point of view) statistics version.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testRestartCorruptedVersion() throws Exception {
        testRestartVersion(metaStorage -> {
            try {
                metaStorage.write("stats.version", "corrupted");
            }
            catch (IgniteCheckedException e) {
                Assert.fail();
            }
        });
    }

    /**
     * Apply function to metastorage and restart node to verify that it will lead to metadata removal.
     *
     * @param verCorruptor statistics version corruptor.
     * @throws Exception In case of errors.
     */
    private void testRestartVersion(Consumer<MetaStorage> verCorruptor) throws Exception {
        IgniteCacheDatabaseSharedManager db = grid(0).context().cache().context().database();

        checkStatisticsExist(db, TIMEOUT);

        db.checkpointReadLock();

        try {
            verCorruptor.accept(db.metaStorage());
        }
        finally {
            db.checkpointReadUnlock();
        }

        stopGrid(0);
        startGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);

        db = grid(0).context().cache().context().database();

        db.checkpointReadLock();

        try {
            assertEquals(IgniteStatisticsPersistenceStoreImpl.VERSION, db.metaStorage().read("stats.version"));
        }
        finally {
            db.checkpointReadUnlock();
        }

        db = grid(0).context().cache().context().database();

        checkStatisticsExist(db, TIMEOUT);
    }

    /**
     * Test that statistics for table SMALL exists in local metastorage.
     *
     * @param db IgniteCacheDatabaseSharedManager to test in.
     * @throws IgniteCheckedException In case of errors.
     */
    private void checkStatisticsExist(IgniteCacheDatabaseSharedManager db, long timeout) throws IgniteCheckedException {
        assertTrue(
            GridTestUtils.waitForCondition(() -> {
                db.checkpointReadLock();

                try {
                    boolean found[] = new boolean[1];

                    db.metaStorage().iterate("stats.data.PUBLIC.SMALL.", (k, v) -> found[0] = true, true);

                    return found[0];
                }
                catch (Throwable ex) {
                    return false;
                }
                finally {
                    db.checkpointReadUnlock();
                }
            }, timeout)
        );
    }
}
