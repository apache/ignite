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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.NO_UPDATE;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.OFF;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.ON;

/**
 * Tests for statistics configuration.
 */
@RunWith(Parameterized.class)
public class StatisticsConfigurationTest extends StatisticsAbstractTest {
    /** Columns to check.*/
    private static final String[] COLUMNS = {"A", "B", "C"};

    /** Lazy mode. */
    @Parameterized.Parameter(value = 0)
    public boolean persist;

    /** */
    @Parameterized.Parameters(name = "persist={0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        boolean[] arrBool = new boolean[] {true, false};

        for (boolean persist0 : arrBool)
            params.add(new Object[] {persist0});

        return params;
    }

    /** Statistic checker: total row count. */
    private Consumer<List<ObjectStatisticsImpl>> checkTotalRows = stats -> {
        long rows = stats.stream()
            .mapToLong(s -> {
                assertNotNull(s);

                return s.rowCount();
            })
            .sum();

        assertEquals(SMALL_SIZE, rows);
    };

    /** Statistic checker: check columns statistic. */
    private Consumer<List<ObjectStatisticsImpl>> checkColumStats = stats -> {
        for (ObjectStatisticsImpl stat : stats) {
            for (String col : COLUMNS) {
                ColumnStatistics colStat = stat.columnStatistics(col);
                assertNotNull("Column: " + col, colStat);

                assertTrue("Column: " + col, colStat.distinct() > 0);
                assertTrue("Column: " + col, colStat.max().getInt() > 0);
                assertTrue("Column: " + col, colStat.total() == stat.rowCount());
            }
        }
    };

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(persist)
                    )
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    protected IgniteEx startGridAndChangeBaseline(int nodeIdx) throws Exception {
        IgniteEx ign = startGrid(nodeIdx);

        ign.cluster().state(ClusterState.ACTIVE);

        if (persist)
            ign.cluster().setBaselineTopology(ign.cluster().topologyVersion());

        awaitPartitionMapExchange();

        return ign;
    }

    /** */
    protected void stopGridAndChangeBaseline(int nodeIdx) {
        stopGrid(nodeIdx);

        if (persist)
            F.first(G.allGrids()).cluster().setBaselineTopology(F.first(G.allGrids()).cluster().topologyVersion());

        try {
            awaitPartitionMapExchange();
        }
        catch (InterruptedException e) {
            // No-op.
        }
    }

    /**
     * Check statistics on cluster after change topology.
     * 1. Create statistic for a table;
     * 2. Restart node;
     * 3. Check statistics.
     */
    @Test
    public void updateStatisticsOnRestartSingleNode() throws Exception {
        if (!persist)
            return;

        startGridAndChangeBaseline(0);

        createSmallTable(null);

        collectStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(0);

        startGrid(0);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);
    }

    /**
     * Check statistics on cluster after change topology.
     * 1. Create statistic for a table;
     * 2. Check statistics on all nodes of the cluster;
     * 3. Stop node;
     * 4. Check statistics on remaining node.
     */
    @Test
    public void stopNodeWithoutChangeBaseline() throws Exception {
        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createSmallTable(null);

        collectStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(1);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);
    }

    /**
     * Check statistics on cluster after change topology.
     * 1. Create statistic for a table;
     * 2. Check statistics on all nodes of the cluster;
     * 3. Change topology (add or remove node);
     * 4. Go to p.2;
     */
    @Test
    public void updateStatisticsOnChangeTopology() throws Exception {
        startGridAndChangeBaseline(0);

        createSmallTable(null);

        collectStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        startClientGrid("cli");
        startGridAndChangeBaseline(1);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        startGridAndChangeBaseline(2);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        startGridAndChangeBaseline(3);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        stopGridAndChangeBaseline(0);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        stopGridAndChangeBaseline(2);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        stopGridAndChangeBaseline(3);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        startGridAndChangeBaseline(3);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);
    }

    /**
     * Check drop statistics.
     * - Create statistic for a table;
     * - Check statistics on all nodes of the cluster;
     * - Drop stat for one columns
     * - Check that statistic is dropped for specified column on all nodes of the cluster;
     * - Re-create statistics
     * - Check statistics on all nodes of the cluster;
     */
    @Test
    public void dropUpdate() throws Exception {
        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createSmallTable(null);

        collectStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        statisticsMgr(0).dropStatistics(new StatisticsTarget("PUBLIC", "SMALL", "A"));

        waitForStats(SCHEMA, "SMALL", TIMEOUT,
            (stats) -> stats.forEach(s -> assertNull(s.columnStatistics("A"))));

        collectStatistics(new StatisticsTarget(SCHEMA, "SMALL", "A"));

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);
    }

    /**
     * Check drop statistics and topology change.
     * - Create statistic for a table;
     * - Check statistics on all nodes of the cluster;
     * - stop a node;
     * - Drop stat for one columns
     * - Check that statistic is dropped for specified column on all nodes of the cluster
     * and check statistic local storage;
     * - Starting the node that was stopped;
     * - Check that statistic is dropped for specified column on all nodes of the cluster;
     * and check statistic local storage on started node;
     */
    @Test
    public void dropSingleColumnStatisticWhileNodeDown() throws Exception {
        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createSmallTable(null);

        collectStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(1);

        statisticsMgr(0).dropStatistics(new StatisticsTarget("PUBLIC", "SMALL", "A"));

        waitForStats(SCHEMA, "SMALL", TIMEOUT,
            (stats) -> stats.forEach(s -> assertNull("Invalid stats: " + stats, s.columnStatistics("A"))));

        checkStatisticsInMetastore(grid(0).context().cache().context().database(), TIMEOUT,
            SCHEMA, "SMALL", (s -> assertNull(s.data().get("A"))));
        checkStatisticsInMetastore(grid(2).context().cache().context().database(), TIMEOUT,
            SCHEMA, "SMALL", (s -> assertNull(s.data().get("A"))));

        startGrid(1);

        checkStatisticsInMetastore(grid(1).context().cache().context().database(), TIMEOUT,
            SCHEMA, "SMALL", (s -> assertNull(s.data().get("A"))));

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows,
            (stats) -> stats.forEach(s -> assertNull("Invalid stats: " + stats, s.columnStatistics("A"))));
    }

    /**
     * Check drop statistics when table is dropped.
     */
    @Test
    public void dropTable() throws Exception {
        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createSmallTable(null);
        createSmallTable("_A");

        collectStatistics(
            new StatisticsTarget(SCHEMA, "SMALL"),
            new StatisticsTarget(SCHEMA, "SMALL_A"));

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);
        waitForStats(SCHEMA, "SMALL_A", TIMEOUT, checkTotalRows, checkColumStats);

        dropSmallTable(null);

        // TODO: remove after fix IGNITE-14814
        if (persist)
            statisticsMgr(0).dropStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, (stats) -> stats.forEach(s -> assertNull(s)));

        waitForStats(SCHEMA, "SMALL_A", TIMEOUT, checkTotalRows, checkColumStats);

        for (Ignite ign : G.allGrids()) {
            checkStatisticsInMetastore(((IgniteEx)ign).context().cache().context().database(), TIMEOUT,
                SCHEMA, "SMALL", (s -> assertNull(s)));
        }
    }

    /**
     * Check drop statistics when table's column is dropped.
     */
    @Test
    public void dropColumn() throws Exception {
        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createSmallTable(null);

        collectStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        sql("DROP INDEX SMALL_B");
        sql("ALTER TABLE SMALL DROP COLUMN B");

        waitForStats(SCHEMA, "SMALL", TIMEOUT,
            (stats) -> stats.forEach(s -> {
                assertNotNull(s.columnStatistics("A"));
                assertNotNull(s.columnStatistics("C"));
                assertNull(s.columnStatistics("B"));
            }));

        for (Ignite ign : G.allGrids()) {
            checkStatisticsInMetastore(((IgniteEx)ign).context().cache().context().database(), TIMEOUT,
                SCHEMA, "SMALL", (s -> assertNull(s.data().get("B"))));
        }
    }

    /**
     * Check drop statistics when table's column is dropped and node with old statistics joins to cluster
     * after drop the column.
     */
    @Test
    public void dropColumnWhileNodeDown() throws Exception {
        if (persist)
            return;

        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createSmallTable(null);

        collectStatistics(SMALL_TARGET);

        waitForStats(SCHEMA, "SMALL", TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(2);

        sql("DROP INDEX SMALL_B");
        sql("ALTER TABLE SMALL DROP COLUMN B");

        startGrid(2);

        waitForStats(SCHEMA, "SMALL", TIMEOUT,
            (stats) -> stats.forEach(s -> {
                assertNotNull(s.columnStatistics("A"));
                assertNotNull(s.columnStatistics("C"));
                assertNull(s.columnStatistics("B"));
            }));

        for (Ignite ign : G.allGrids()) {
            checkStatisticsInMetastore(((IgniteEx)ign).context().cache().context().database(), TIMEOUT,
                SCHEMA, "SMALL", (s -> assertNull(s.data().get("B"))));
        }
    }

    /**
     * Try statistics configuration commands in different statistics state.
     *
     * 1) Start grid and check state is ON.
     * 2) Create table and gather/get/refresh/drop statistics on it from "local" and "remote" hosts.
     * 3) Change state to NO_UPDATE and gather/get/refresh/drop statistics on created table.
     * 4) Change state to OFF and check exception throws on gather/get/refresh/drop statistics.
     * 5) Change state to NO_UPDATE gather/get/refresh/drop statistics on created table.
     * 6) Change state to ON and gather/get/refresh/drop statistics on created table.
     *
     * @throws Exception In case of errors:
     */
    @Test
    public void testChangeState() throws Exception {
        IgniteEx ign0 = startGrids(2);

        ign0.cluster().state(ClusterState.ACTIVE);

        IgniteEx ign1 = grid(1);

        assertEquals(ON, statisticsMgr(0).usageState());

        createSmallTable(null);

        assertTrue(executeStatisticsConfigurationCommands(ign0));
        assertTrue(executeStatisticsConfigurationCommands(ign1));

        statisticsMgr(0).usageState(NO_UPDATE);

        assertTrue(executeStatisticsConfigurationCommands(ign0));
        assertTrue(executeStatisticsConfigurationCommands(ign1));

        statisticsMgr(0).usageState(OFF);

        assertFalse(executeStatisticsConfigurationCommands(ign0));
        assertFalse(executeStatisticsConfigurationCommands(ign1));

        statisticsMgr(0).usageState(NO_UPDATE);

        assertTrue(executeStatisticsConfigurationCommands(ign0));
        assertTrue(executeStatisticsConfigurationCommands(ign1));

        statisticsMgr(0).usageState(ON);

        assertTrue(executeStatisticsConfigurationCommands(ign0));
        assertTrue(executeStatisticsConfigurationCommands(ign1));
    }

    /**
     * Run analyze/get/refresh/drop commands on specified node.
     *
     * @param ign Node to test.
     * @return {@code true} if all commands pass successfully, {@code false} - otherwise.
     */
    private boolean executeStatisticsConfigurationCommands(IgniteEx ign) throws IgniteInterruptedCheckedException {
        IgniteH2Indexing indexing = (IgniteH2Indexing)ign.context().query().getIndexing();
        IgniteStatisticsManager statMgr = indexing.statsManager();

        int success = 0;
        try {
            statMgr.collectStatistics(buildDefaultConfigurations(SMALL_TARGET));
            success++;
        } catch (Exception e) {
            if (!(e instanceof IgniteException && e.getMessage().contains("while statistics usage state is OFF.")))
                fail("Unknown error: " + e);
        }

        if (GridTestUtils.waitForCondition(() -> statMgr.getLocalStatistics(SMALL_KEY) != null, TIMEOUT))
            success++;

        try {
            statMgr.refreshStatistics(SMALL_TARGET);
            success++;
        } catch (Exception e) {
            if (!(e instanceof IgniteException && e.getMessage().contains("while statistics usage state is OFF.")))
                fail("Unknown error: " + e);
        }

        try {
            statMgr.dropStatistics(SMALL_TARGET);
            success++;
        } catch (Exception e) {
            if (!(e instanceof IgniteException && e.getMessage().contains("while statistics usage state is OFF.")))
                fail("Unknown error: " + e);
        }

        if (success == 4)
            return true;

        if (success == 0)
            return false;

        fail("Partially success execution");
        return false;
    }

    /**
     * If persistence enabled - run specified checkers against all object statistics in metastore.
     *
     * @param db IgniteCacheDatabaseSharedManager to test metastore by.
     * @param timeout Timeout.
     * @param schema Schema name.
     * @param obj Object name.
     * @param checkers Checkers to run against statistics from db.
     * @throws IgniteCheckedException In case of errors.
     */
    private void checkStatisticsInMetastore(
        IgniteCacheDatabaseSharedManager db,
        long timeout,
        String schema,
        String obj,
        Consumer<StatisticsObjectData>... checkers
    ) throws IgniteCheckedException {
        if (!persist)
            return;

        long t0 = U.currentTimeMillis();

        while (true) {
            db.checkpointReadLock();

            try {
                db.metaStorage().iterate(
                    "stats.data." + schema + '.' + obj + '.',
                    (k, v) -> Arrays.stream(checkers).forEach(ch -> ch.accept((StatisticsObjectData)v)),
                    true);

                return;
            }
            catch (Throwable ex) {
                if (t0 + timeout < U.currentTimeMillis())
                    throw ex;
                else
                    U.sleep(200);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
    }

    /** */
    private void waitForStats(
        String schema,
        String objName,
        long timeout,
        Consumer<List<ObjectStatisticsImpl>>... statsCheckers
    ) {
        long t0 = U.currentTimeMillis();

        while (true) {
            try {
                List<ObjectStatisticsImpl> stats = statisticsAllNodes(schema, objName);

                for (Consumer<List<ObjectStatisticsImpl>> statChecker : statsCheckers)
                    statChecker.accept(stats);

                return;
            }
            catch (Throwable ex) {
                if (t0 + timeout < U.currentTimeMillis()) {
                    log.error("Unexpected stats");

                    List<ObjectStatisticsImpl> stats = statisticsAllNodes(schema, objName);

                    stats.forEach(s -> log.error("Stat: " + s));

                    throw ex;
                }
                else {
                    try {
                        U.sleep(200);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        // No-op.
                    }
                }
            }
        }
    }

    /**
     * Collect local object statistics by all grid nodes (client and server ones).
     *
     * @param schema Schema name.
     * @param objName Object name.
     * @return List of all nodes local statistics (with {@code null} if there is no statistics in some nodes).
     */
    @NotNull private List<ObjectStatisticsImpl> statisticsAllNodes(String schema, String objName) {
        List<IgniteStatisticsManager> mgrs = G.allGrids().stream()
            .filter(ign -> !((IgniteEx)ign).context().clientNode())
            .map(ign -> ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing()).statsManager())
            .collect(Collectors.toList());

        return mgrs.stream()
            .map(m -> (ObjectStatisticsImpl)m.getLocalStatistics(new StatisticsKey(schema, objName)))
            .collect(Collectors.toList());
    }
}
