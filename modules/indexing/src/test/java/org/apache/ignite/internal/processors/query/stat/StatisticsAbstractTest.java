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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;

/**
 * Base test for table statistics.
 */
public abstract class StatisticsAbstractTest extends GridCommonAbstractTest {
    /** Default SQL schema. */
    public static final String SCHEMA = "PUBLIC";

    /** Counter to avoid query caching. */
    private static final AtomicInteger queryRandomizer = new AtomicInteger(0);

    /** Big table size. */
    static final int BIG_SIZE = 1000;

    /** Medium table size. */
    static final int MED_SIZE = 500;

    /** Small table size. */
    static final int SMALL_SIZE = 100;

    /** Statistics key for small table. */
    static final StatisticsKey SMALL_KEY = new StatisticsKey(SCHEMA, "SMALL");

    /** Statistics target for the whole small table. */
    static final StatisticsTarget SMALL_TARGET = new StatisticsTarget(SMALL_KEY, null);

    /** Async operation timeout for test */
    static final int TIMEOUT = 3_000;

    static {
        assertTrue(SMALL_SIZE < MED_SIZE && MED_SIZE < BIG_SIZE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /**
     * Compare different index used for the given query.
     *
     * @param grid Grid to run queries on.
     * @param optimal Array of optimal indexes.
     * @param sql Query with placeholders to hint indexes (i1, i2, ...).
     * @param indexes Arrays of indexes to put into placeholders.
     */
    protected void checkOptimalPlanChosenForDifferentIndexes(
        IgniteEx grid,
        String[] optimal,
        String sql,
        String[][] indexes
    ) {
        int size = -1;
        for (String[] idxs : indexes) {
            if (size == -1)
                size = (idxs == null) ? 0 : idxs.length;

            assert idxs == null || idxs.length == size;
        }

        sql = replaceIndexHintPlaceholders(sql, indexes);

        int spaces = queryRandomizer.incrementAndGet();
        StringBuilder spaceBuilder = new StringBuilder(spaces);

        for (int i = 0; i < spaces; i++)
            spaceBuilder.append(' ');

        sql = sql.replaceFirst(" ", spaceBuilder.toString());

        String actual[] = runLocalExplainIdx(grid, sql);

        assertTrue(String.format("got %s, expected %s in query %s", Arrays.asList(actual), Arrays.asList(optimal), sql),
            Arrays.equals(actual, optimal));
    }

    /**
     * Compares different orders of joins for the given query.
     *
     * @param grid Grid to run queries on.
     * @param sql Query text with placeholder (t0, t1, ...) instead of table names.
     * @param tbls Table names.
     */
    protected void checkOptimalPlanChosenForDifferentJoinOrders(Ignite grid, String sql, String... tbls) {
        String directOrder = replaceTablePlaceholders(sql, tbls);

        if (log.isDebugEnabled())
            log.debug("Direct join order=" + directOrder);

        ensureOptimalPlanChosen(grid, directOrder);

        // Reverse tables order.
        List<String> dirOrdTbls = Arrays.asList(tbls);

        Collections.reverse(dirOrdTbls);

        String reversedOrder = replaceTablePlaceholders(sql, dirOrdTbls.toArray(new String[dirOrdTbls.size()]));

        if (log.isDebugEnabled())
            log.debug("Reversed join order=" + reversedOrder);

        ensureOptimalPlanChosen(grid, reversedOrder);
    }

    /**
     * Compares join orders by actually scanned rows. Join command is run twice:
     * with {@code enforceJoinOrder = true} and without. The latest allows join order optimization
     * based or table row count.
     *
     * Actual scan row count is obtained from the EXPLAIN ANALYZE command result.
     */
    private void ensureOptimalPlanChosen(Ignite grid, String sql, String... tbls) {
        int cntNoStats = runLocalExplainAnalyze(grid, true, sql);

        int cntStats = runLocalExplainAnalyze(grid, false, sql);

        String res = "Scanned rows count [noStats=" + cntNoStats + ", withStats=" + cntStats +
            ", diff=" + (cntNoStats - cntStats) + ']';

        if (log.isInfoEnabled())
            log.info(res);

        assertTrue(res, cntStats <= cntNoStats);
    }

    /**
     * Run specified query with EXPLAIN and return array of used indexes.
     *
     * @param grid Grid where query should be executed.
     * @param sql Query to explain.
     * @return Array of selected indexes.
     */
    protected String[] runLocalExplainIdx(Ignite grid, String sql) {
        List<List<?>> res = grid.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("EXPLAIN " + sql).setLocal(true))
            .getAll();
        String explainRes = (String)res.get(0).get(0);

        // Extract scan count from EXPLAIN ANALYZE with regex: return all numbers after "scanCount: ".
        Matcher m = Pattern.compile(".*\\/\\*.+?\\.(\\w+):.*\\R*.*\\R*.*\\R*.*\\R*\\*\\/.*").matcher(explainRes);
        List<String> result = new ArrayList<>();
        while (m.find())
            result.add(m.group(1).trim());

        return result.toArray(new String[result.size()]);
    }

    /**
     * Runs local join sql in EXPLAIN ANALYZE mode and extracts actual scanned row count from the result.
     *
     * @param enfJoinOrder Enforce join order flag.
     * @param sql Sql string.
     * @return Actual scanned rows count.
     */
    protected int runLocalExplainAnalyze(Ignite grid, boolean enfJoinOrder, String sql) {
        List<List<?>> res = grid.cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx("EXPLAIN ANALYZE " + sql, null).setEnforceJoinOrder(enfJoinOrder)
                .setLocal(true))
            .getAll();

        if (log.isDebugEnabled())
            log.debug("ExplainAnalyze enfJoinOrder=" + enfJoinOrder + ", res=" + res);

        return extractScanCountFromExplain(res);
    }

    /**
     * Process obsolescence on given grid.
     *
     * @param grid Grid to process obsolescence on.
     */
    private void processObsolescence(IgniteEx grid) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)grid.context().query().getIndexing();

        ((IgniteStatisticsManagerImpl)indexing.statsManager()).processObsolescence();
    }

    /**
     * Extracts actual scanned rows count from EXPLAIN ANALYZE result.
     *
     * @param res EXPLAIN ANALYZE result.
     * @return Actual scanned rows count.
     */
    private int extractScanCountFromExplain(List<List<?>> res) {
        String explainRes = (String)res.get(0).get(0);

        // Extract scan count from EXPLAIN ANALYZE with regex: return all numbers after "scanCount: ".
        Matcher m = Pattern.compile("scanCount: (?=(\\d+))").matcher(explainRes);

        int scanCnt = 0;

        while (m.find())
            scanCnt += Integer.parseInt(m.group(1));

        return scanCnt;
    }

    /**
     * Run specified SQL on grid0.
     *
     * @param sql Statement to execute.
     */
    protected List<List<?>> sql(String sql) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /**
     * Create SQL table with the given index.
     *
     * @param suffix Table idx, if {@code null} - name "SMALL" without index will be used.
     * @return Table name.
     */
    protected String createSmallTable(String suffix) {
        String tblName = "small" + ((suffix != null) ? suffix : "");

        sql("DROP TABLE IF EXISTS " + tblName);

        sql(String.format("CREATE TABLE %s (a INT PRIMARY KEY, b INT, c INT)" +
                " with \"BACKUPS=1,CACHE_NAME=SMALL%s\"",
            tblName, suffix));

        sql(String.format("CREATE INDEX %s_b ON %s(b)", tblName, tblName));

        sql(String.format("CREATE INDEX %s_c ON %s(c)", tblName, tblName));

        for (int i = 0; i < SMALL_SIZE; i++)
            sql(String.format("INSERT INTO %s(a, b, c) VALUES(%d, %d, %d)", tblName, i, i, i % 10));

        return tblName;
    }

    /**
     * Drop SQL table with the given index.
     *
     * @param suffix Table idx, if {@code null} - name "SMALL" without index will be used.
     */
    protected void dropSmallTable(String suffix) {
        suffix = suffix != null ? suffix : "";

        sql("DROP TABLE IF EXISTS small" + suffix);
    }

    /**
     * Replaces index hint placeholder like "i1", "i2" with specified index names in the ISQL query.
     *
     * @param sql SQL to replace index placeholders.
     * @param idxs Index names array.
     * @return SQL with actual index names.
     */
    private static String replaceIndexHintPlaceholders(String sql, String[][] idxs) {
        assert !sql.contains("i0");

        int i = 0;

        for (String idx[] : idxs) {
            String idxPlaceHolder = "i" + (++i);

            assert sql.contains(idxPlaceHolder);

            if (!F.isEmpty(idx)) {
                String idxStr = "USE INDEX (" + String.join(",", idx) + ")";
                sql = sql.replaceAll(idxPlaceHolder, idxStr);
            }
            else
                sql = sql.replaceAll(idxPlaceHolder, "");

        }

        assert !sql.contains("i" + (i + 1));

        return sql;
    }

    /**
     * Replaces table placeholders like "t1", "t2" and others with actual table names in the SQL query.
     *
     * @param sql SQL query.
     * @param tbls Actual table names.
     * @return SQL with place holders replaced by the actual names.
     */
    private static String replaceTablePlaceholders(String sql, String... tbls) {
        assert !sql.contains("t0");

        int i = 0;

        for (String tbl : tbls) {
            String tblPlaceHolder = "t" + (++i);

            assert sql.contains(tblPlaceHolder);

            sql = sql.replace(tblPlaceHolder, tbl);
        }

        assert !sql.contains("t" + (i + 1));

        return sql;
    }

    /**
     * Update statistics on specified objects in PUBLIC schema.
     *
     * @param tables Tables where to update statistics.
     */
    protected void updateStatistics(@NotNull String... tables) {
        StatisticsTarget[] targets = Arrays.stream(tables).map(tbl -> new StatisticsTarget(SCHEMA, tbl.toUpperCase()))
            .toArray(StatisticsTarget[]::new);

        updateStatistics(targets);
    }

    /**
     * Collect statistics for specified objects in PUBLIC schema.
     *
     * @param tables Tables where to collect statistics.
     */
    protected void collectStatistics(@NotNull String... tables) {
        StatisticsTarget[] targets = Arrays.stream(tables).map(tbl -> new StatisticsTarget(SCHEMA, tbl.toUpperCase()))
            .toArray(StatisticsTarget[]::new);

        collectStatistics(targets);
    }

    /**
     * Update statistics on specified objects.
     *
     * @param targets Targets to refresh statistics by.
     */
    protected void updateStatistics(StatisticsTarget... targets) {
        makeStatistics(false, targets);
    }

    /**
     * Update statistics on specified objects.
     *
     * @param targets Targets to collect statistics by.
     */
    protected void collectStatistics(StatisticsTarget... targets) {
        makeStatistics(true, targets);
    }

    /**
     * Collect or refresh statistics.
     *
     * @param collect If {@code true} - collect new statistics, if {@code false} - update existing.
     * @param targets
     */
    private void makeStatistics(boolean collect, StatisticsTarget... targets) {
        try {
            Map<StatisticsTarget, Long> expectedVersion = new HashMap<>();
            IgniteStatisticsManagerImpl statMgr = statisticsMgr(0);
            for (StatisticsTarget target : targets) {
                StatisticsObjectConfiguration currCfg = statMgr.statisticConfiguration().config(target.key());

                Predicate<StatisticsColumnConfiguration> pred;
                if (F.isEmpty(target.columns()))
                    pred = c -> true;
                else {
                    Set<String> cols = Arrays.stream(target.columns()).collect(Collectors.toSet());

                    pred = c -> cols.contains(c.name());
                }

                Long expVer = (currCfg == null) ? 1L : currCfg.columnsAll().values().stream().filter(pred)
                    .mapToLong(StatisticsColumnConfiguration::version).min().orElse(0L) + 1;

                expectedVersion.put(target, expVer);
            }

            if (collect)
                statisticsMgr(0).collectStatistics(buildDefaultConfigurations(targets));
            else
                statisticsMgr(0).refreshStatistics(targets);

            awaitStatistics(TIMEOUT, expectedVersion);
        }
        catch (Exception ex) {
            throw new IgniteException(ex);
        }
    }

    /**
     * Get object statistics.
     *
     * @param rowsCnt Rows count.
     * @return Object statistics.
     */
    protected ObjectStatisticsImpl getStatistics(long rowsCnt) {
        ColumnStatistics colStatistics = new ColumnStatistics(null, null, 100, 0, 100,
            0, new byte[0], 0, U.currentTimeMillis());
        return new ObjectStatisticsImpl(rowsCnt, Collections.singletonMap("col1", colStatistics));
    }

    /**
     * Get object partition statistics.
     *
     * @param partId Partition id.
     * @return Object partition statistics with specified partition id.
     */
    protected ObjectPartitionStatisticsImpl getPartitionStatistics(int partId) {
        ColumnStatistics colStatistics = new ColumnStatistics(null, null, 100, 0,
            100, 0, new byte[0], 0, U.currentTimeMillis());

        return new ObjectPartitionStatisticsImpl(
            partId, 0, 0,
            Collections.singletonMap("col1", colStatistics)
        );
    }

    /** Check that all statistics collections related tasks is empty in specified node. */
    protected void checkStatisticTasksEmpty(IgniteEx ign) {
        Map<StatisticsKey, LocalStatisticsGatheringContext> currColls = GridTestUtils.getFieldValue(
            statisticsMgr(ign), "gatherer", "gatheringInProgress"
        );

        assertTrue(currColls.toString(), currColls.isEmpty());

        IgniteThreadPoolExecutor mgmtPool = GridTestUtils.getFieldValue(statisticsMgr(ign), "mgmtPool");

        assertTrue(mgmtPool.getQueue().isEmpty());

        IgniteThreadPoolExecutor gatherPool = GridTestUtils.getFieldValue(statisticsMgr(ign), "gatherPool");

        assertTrue(gatherPool.getQueue().isEmpty());
    }

    /**
     * Await statistic gathering is complete on whole cluster.
     *
     * @param timeout Timeout.
     * @param expectedVersions Expected versions for specified targets.
     * @throws Exception In case of errors.
     */
    protected void awaitStatistics(long timeout, Map<StatisticsTarget, Long> expectedVersions) throws Exception {
        for (Ignite ign : G.allGrids()) {
            if (!((IgniteEx)ign).context().clientNode())
                awaitStatistics(timeout, expectedVersions, (IgniteEx)ign);
        }
    }

    /**
     * Await statistic gathering is complete on specified node.
     *
     * @param timeout Timeout.
     * @param expectedVersions Expected versions for specified targets.
     * @param ign Node to await.
     * @throws Exception In case of errors.
     */
    protected void awaitStatistics(long timeout, Map<StatisticsTarget, Long> expectedVersions, IgniteEx ign)
        throws Exception {
        long t0 = U.currentTimeMillis();

        IgniteH2Indexing indexing = (IgniteH2Indexing)ign.context().query().getIndexing();

        while (true) {
            try {
                checkStatisticTasksEmpty(ign);
                for (Map.Entry<StatisticsTarget, Long> targetVersionEntry : expectedVersions.entrySet()) {
                    StatisticsTarget target = targetVersionEntry.getKey();
                    Long ver = targetVersionEntry.getValue();

                    ObjectStatisticsImpl s = (ObjectStatisticsImpl)indexing.statsManager().getLocalStatistics(target.key());
                    assertNotNull(s);

                    long minVer = Long.MAX_VALUE;

                    Set<String> cols;
                    if (F.isEmpty(target.columns()))
                        cols = s.columnsStatistics().keySet();
                    else
                        cols = Arrays.stream(target.columns()).collect(Collectors.toSet());

                    for (String col : cols) {
                        if (s.columnStatistics(col).version() < minVer)
                            minVer = s.columnStatistics(col).version();
                    }

                    if (minVer == Long.MAX_VALUE)
                        minVer = -1;

                    assertEquals((long)ver, minVer);
                }

                return;
            }
            catch (Throwable ex) {
                if (t0 + timeout < U.currentTimeMillis())
                    throw ex;
                else
                    U.sleep(200);
            }
        }
    }

    /**
     * Get nodes StatisticsGatheringRequestCrawlerImpl.msgMgmtPool lock.
     * Put additional task into it and return lock to complete these task.
     *
     * @param nodeIdx Node idx.
     * @return Lock to complete pool task and allow it to process next one.
     */
    protected Lock nodeMsgsLock(int nodeIdx) throws Exception {
        IgniteThreadPoolExecutor pool = GridTestUtils.getFieldValue(statisticsMgr(nodeIdx), "statCrawler", "msgMgmtPool");

        return lockPool(pool);
    }

    /**
     * Get nodes StatisticsGatheringImpl.gatMgmtPool lock.
     * Put additional task into it and return lock to complete these task.
     *
     * @param nodeIdx Node idx.
     * @return Lock to complete pool task and allow it to process next one.
     */
    protected Lock nodeGathLock(int nodeIdx) throws Exception {
        IgniteThreadPoolExecutor pool = GridTestUtils.getFieldValue(statisticsMgr(nodeIdx), "statGathering", "gatMgmtPool");

        return lockPool(pool);
    }

    /**
     * Lock specified pool with task, waiting for lock release.
     *
     * @param pool Pool to block.
     * @return Lock.
     */
    private Lock lockPool(IgniteThreadPoolExecutor pool) {
        Lock res = new ReentrantLock();
        res.lock();
        pool.submit(res::lock);

        return res;
    }

    /**
     * Get local or global object statistics from all server nodes.
     *
     * @param tblName Object name to get statistics by.
     * @param type Desired statistics type.
     * @return Array of local statistics from nodes.
     */
    protected ObjectStatisticsImpl[] getStats(String tblName, StatisticsType type) {
        int nodes = G.allGrids().size();
        ObjectStatisticsImpl res[] = new ObjectStatisticsImpl[nodes];

        for (int i = 0; i < nodes; i++)
            res[i] = getStatsFromNode(i, tblName, type);

        return res;
    }

    /**
     * Test specified predicate on each object statistics.
     *
     * @param cond Predicate to test.
     * @param stats Statistics to test on.
     */
    protected void testCond(Function<ObjectStatisticsImpl, Boolean> cond, ObjectStatisticsImpl... stats) {
        assertFalse(F.isEmpty(stats));

        for (ObjectStatisticsImpl stat : stats)
            assertTrue(cond.apply(stat));
    }

    /**
     * Get local table statistics by specified node.
     *
     * @param nodeIdx Node index to get statistics from.
     * @param tblName Table name.
     * @param type Desired statistics type.
     * @return Local table statistics or {@code null} if there are no such statistics in specified node.
     */
    protected ObjectStatisticsImpl getStatsFromNode(int nodeIdx, String tblName, StatisticsType type) {
        switch (type) {
            case LOCAL:
                return (ObjectStatisticsImpl)statisticsMgr(nodeIdx).getLocalStatistics(new StatisticsKey(SCHEMA, tblName));
            case PARTITION:
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Get statistics manager by node id.
     *
     * @param nodeIdx Node id.
     * @return Statistics manager implementation.
     */
    public IgniteStatisticsManagerImpl statisticsMgr(int nodeIdx) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)grid(nodeIdx).context().query().getIndexing();

        return (IgniteStatisticsManagerImpl)indexing.statsManager();
    }

    /**
     * Get statistics manager by node.
     *
     * @param ign IgniteEx.
     * @return Statistics manager implementation.
     */
    public IgniteStatisticsManagerImpl statisticsMgr(IgniteEx ign) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)ign.context().query().getIndexing();

        return (IgniteStatisticsManagerImpl)indexing.statsManager();
    }

    /**
     * Make set from array.
     *
     * @param vals Values to populate into the set.
     * @return Set of specified values.
     */
    public <T> Set<T> setOf(T... vals) {
        if (F.isEmpty(vals))
            return Collections.emptySet();

        if (vals.length == 1)
            return Collections.singleton(vals[0]);

        Set<T> res = new HashSet<>(vals.length);

        Collections.addAll(res, vals);

        return res;
    }

    /**
     * Run specified sql and test result.
     *
     * @param sql Sql to execute.
     * @param nodeFilter Node filter, if {@code null} - run on all nodes.
     * @param checker Result checker.
     * @throws Exception In case of error.
     */
    protected void checkSqlResult(
        String sql,
        Predicate<Ignite> nodeFilter,
        Predicate<List<List<?>>> checker
    ) throws Exception {
        List<Ignite> nodes = G.allGrids();

        if (nodeFilter != null)
            nodes = nodes.stream().filter(nodeFilter).collect(Collectors.toList());

        for (Ignite ign : nodes) {
            assertTrue(GridTestUtils.waitForCondition(() -> {
                List<List<?>> res = ign.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)).getAll();

                return checker.test(res);
            }, 1000));
        }
    }
}
