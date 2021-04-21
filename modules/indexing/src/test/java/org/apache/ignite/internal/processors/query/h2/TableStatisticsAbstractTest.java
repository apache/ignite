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
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base test for table statistics.
 */
public abstract class TableStatisticsAbstractTest extends GridCommonAbstractTest {
    /** */
    static final int BIG_SIZE = 1000;

    /** */
    static final int MED_SIZE = 500;

    /** */
    static final int SMALL_SIZE = 100;

    static {
        assertTrue(SMALL_SIZE < MED_SIZE && MED_SIZE < BIG_SIZE);
    }

    /**
     * Compares different orders of joins for the given query.
     *
     * @param sql Query.
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
     * Runs local join sql in EXPLAIN ANALYZE mode and extracts actual scanned row count from the result.
     *
     * @param enfJoinOrder Enforce join order flag.
     * @param sql Sql string.
     * @return Actual scanned rows count.
     */
    private int runLocalExplainAnalyze(Ignite grid, boolean enfJoinOrder, String sql) {
        List<List<?>> res = grid.cache(DEFAULT_CACHE_NAME)
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

        while (m.find())
            scanCnt += Integer.valueOf(m.group(1));

        return scanCnt;
    }

    /**
     * @param sql Statement.
     */
    protected void runSql(String sql) {
        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * Replaces table placeholders like "t1", "t2" and others with actual table names in the SQL query.
     *
     * @param sql Sql query.
     * @param tbls Actual table names.
     * @return Sql with place holders replaced by the actual names.
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

}
