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
package org.apache.ignite.internal.processors.query.h2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test cases to ensure that proper join order is chosen by H2 optimizer when row count statistics is collected.
 */
@RunWith(Parameterized.class)
public class H2RowCountStatisticsUsageTest extends GridCommonAbstractTest {
    /** */
    private static final int BIG_SIZE = 1000;

    /** */
    private static final int MED_SIZE = 500;

    /** */
    private static final int SMALL_SIZE = 100;

    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            { REPLICATED },
            { PARTITIONED },
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assertTrue(SMALL_SIZE < MED_SIZE && MED_SIZE < BIG_SIZE);

        Ignite grid = startGrid(0);

        grid.createCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runSql("DROP TABLE IF EXISTS big");
        runSql("DROP TABLE IF EXISTS med");
        runSql("DROP TABLE IF EXISTS small");

        runSql("CREATE TABLE big (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");
        runSql("CREATE TABLE med (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");
        runSql("CREATE TABLE small (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");

        runSql("CREATE INDEX big_b ON big(b)");
        runSql("CREATE INDEX med_b ON med(b)");
        runSql("CREATE INDEX small_b ON small(b)");

        runSql("CREATE INDEX big_c ON big(c)");
        runSql("CREATE INDEX med_c ON med(c)");
        runSql("CREATE INDEX small_c ON small(c)");

        for (int i = 0; i < BIG_SIZE; i++)
            runSql("INSERT INTO big(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        for (int i = 0; i < MED_SIZE; i++)
            runSql("INSERT INTO med(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        for (int i = 0; i < SMALL_SIZE; i++)
            runSql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + ","+ i % 10 + ")");
    }

    /**
     *
     */
    @Test
    public void compareJoinsWithConditionsOnBothTables() {
        String sql  = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c " +
            "WHERE t1.b >= 0 AND t2.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(sql, "small", "big");
    }

    /**
     *
     */
    @Test
    public void compareJoinsWithoutConditions() {
        String sql  = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c";

        checkOptimalPlanChosenForDifferentJoinOrders(sql, "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsConditionSingleTable() {
        final String sql  = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c WHERE t1.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(sql, "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsThreeTablesNoConditions() {
        String sql  = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c JOIN t3 ON t3.c = t2.c ";

        checkOptimalPlanChosenForDifferentJoinOrders(sql, "big", "med", "small");
        checkOptimalPlanChosenForDifferentJoinOrders(sql, "small", "big", "med");
        checkOptimalPlanChosenForDifferentJoinOrders(sql, "small", "med", "big");
        checkOptimalPlanChosenForDifferentJoinOrders(sql, "med", "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsThreeTablesConditionsOnAllTables() {
        String sql  = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c JOIN t3 ON t3.c = t2.c " +
            " WHERE t1.b >= 0 AND t2.b >= 0 AND t3.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(sql, "big", "med", "small");
        checkOptimalPlanChosenForDifferentJoinOrders(sql, "small", "big", "med");
        checkOptimalPlanChosenForDifferentJoinOrders(sql, "small", "med", "big");
        checkOptimalPlanChosenForDifferentJoinOrders(sql, "med", "big", "small");
    }

    /**
     * Checks if statistics is updated when table size is changed.
     */
    @Test
    public void checkUpdateStatisticsOnTableSizeChange() {
        // t2 size is bigger than t1
        String sql  = "SELECT COUNT(*) FROM t2 JOIN t1 ON t1.c = t2.c " +
            "WHERE t1.b > " + 0  + " AND t2.b > " + 0;

        checkOptimalPlanChosenForDifferentJoinOrders(sql, "small", "big");

        // Make small table bigger than a big table
        for (int i = SMALL_SIZE; i < BIG_SIZE * 2; i++)
            runSql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        // t1 size is now bigger than t2
        sql  = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c " +
            "WHERE t1.b > " + 0  + " AND t2.b > " + 0;

        checkOptimalPlanChosenForDifferentJoinOrders(sql, "small", "big");
    }

    /**
     * Compares different orders of joins for the given query.
     *
     * @param sql Query.
     * @param tbls Table names.
     */
    private void checkOptimalPlanChosenForDifferentJoinOrders(String sql, String ... tbls) {
        String directOrder = replaceTablePlaceholders(sql, tbls);

        if (log.isDebugEnabled())
            log.debug("Direct join order=" + directOrder);

        ensureOptimalPlanChosen(directOrder);

        // Reverse tables order.
        List<String> dirOrdTbls = Arrays.asList(tbls);

        Collections.reverse(dirOrdTbls);

        String reversedOrder = replaceTablePlaceholders(sql, (String[]) dirOrdTbls.toArray());

        if (log.isDebugEnabled())
            log.debug("Reversed join order=" + reversedOrder);

        ensureOptimalPlanChosen(reversedOrder);
    }

    /**
     * Compares join orders by actually scanned rows. Join command is run twice:
     * with {@code enforceJoinOrder = true} and without. The latest allows join order optimization
     * based or table row count.
     *
     * Actual scan row count is obtained from the EXPLAIN ANALYZE command result.
     */
    private void ensureOptimalPlanChosen(String sql, String ... tbls) {
        int cntNoStats = runLocalExplainAnalyze(true, sql);

        int cntStats = runLocalExplainAnalyze(false, sql);

        String res = "Scanned rows count [noStats=" + cntNoStats + ", withStats=" + cntStats +
            ", diff=" + (cntNoStats - cntStats) + ']';

        if (log.isInfoEnabled())
            log.info(res);

        assertTrue(res, cntStats <= cntNoStats);
    }

    /**
     * Runs local join sql in EXPLAIN ANALYZE mode and extracts actual scanned row count from the result.
     *
     * @param enfJoinOrder Enforce join order flag.
     * @param sql Sql string.
     * @return Actual scanned rows count.
     */
    private int runLocalExplainAnalyze(boolean enfJoinOrder, String sql) {
        List<List<?>> res = grid(0).cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery("EXPLAIN ANALYZE " + sql)
                .setEnforceJoinOrder(enfJoinOrder)
                .setLocal(true))
            .getAll();

        if (log.isDebugEnabled())
            log.debug("ExplainAnalyze enfJoinOrder=" + enfJoinOrder + ", res=" + res);

        return extractScanCountFromExplain(res);
    }

    /**
     * Extracts actual scanned rows count from EXPLAIN ANALYZE result.
     *
     * @param res EXPLAIN ANALYZE result.
     * @return actual scanned rows count.
     */
    private int extractScanCountFromExplain(List<List<?>> res) {
        String explainRes = (String)res.get(0).get(0);

        // Extract scan count from EXPLAIN ANALYZE with regex: return all numbers after "scanCount: ".
        Matcher m = Pattern.compile("scanCount: (?=(\\d+))").matcher(explainRes);

        int scanCnt = 0;

        while(m.find())
            scanCnt += Integer.valueOf(m.group(1));

        return scanCnt;
    }

    /**
     * @param sql Statement.
     */
    private void runSql(String sql) {
        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * Replaces table placeholders like "t1", "t2" and others with actual table names in the SQL query.
     *
     * @param sql Sql query.
     * @param tbls Actual table names.
     * @return Sql with place holders replaced by the actual names.
     */
    private static String replaceTablePlaceholders(String sql, String ... tbls) {
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
}