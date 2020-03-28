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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Check that data page scan property defined in the thin driver correctly passed to Indexing.
 */
public class JdbcThinDataPageScanPropertySelfTest extends GridCommonAbstractTest {
    /** Batch size for streaming/batch mode. */
    private static final int BATCH_SIZE = 10;

    /** How many queries to execute in total in case bach/streaming mode. Should be greater than batch size. */
    private static final int TOTAL_QUERIES_TO_EXECUTE = 25;

    /** Initial size of the test table. */
    private static final int INITIAL_ROWS_CNT = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        GridQueryProcessor.idxCls = IndexingWithQueries.class;

        startGrids(3);
    }

    /**
     * Execute provided sql update query.
     */
    private void executeUpdate(String sql) throws Exception {
        try (Connection conn = GridTestUtils.connect(grid(0), null)) {
            try (PreparedStatement upd = conn.prepareStatement(sql)) {
                upd.executeUpdate();
            }
        }
    }

    /**
     * Create some table to query, fill data.
     */
    @Before
    public void inint() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS TEST");

        executeUpdate("CREATE TABLE TEST (id INT PRIMARY KEY, val INT)");

        IgniteCache<Integer, Integer> cache = grid(0).cache("SQL_PUBLIC_TEST");

        for (int i = 0; i < INITIAL_ROWS_CNT; i++)
            executeUpdate("INSERT INTO TEST VALUES (" + i + ", " + (i + 1) + ")");

        IndexingWithQueries.queries.clear();
    }

    /**
     * Verify single queries.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11998")
    public void testDataPageScanSingle() throws Exception {
        checkDataPageScan("SELECT * FROM TEST WHERE val > 42", null);
        checkDataPageScan("UPDATE TEST SET val = val + 1 WHERE val > 10", null);

        checkDataPageScan("SELECT id FROM TEST WHERE val < 3", true);
        checkDataPageScan("UPDATE TEST SET val = val + 3 WHERE val < 3", true);

        checkDataPageScan("SELECT val FROM TEST WHERE id = 5", false);
        checkDataPageScan("UPDATE TEST SET val = val - 5 WHERE val < 100", false);
    }

    /**
     * Verify the case property is set on connection and batched operations are performed.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11998")
    public void testDataPageScanBatching() throws Exception {
        checkDataPageScanInBatch("UPDATE TEST SET val = ? WHERE val > 10", null);

        checkDataPageScanInBatch("UPDATE TEST SET val = val + 3 WHERE val < ?", true);

        checkDataPageScanInBatch("UPDATE TEST SET val = val - 5 WHERE val < ?", false);
    }

    /**
     * Checks that executed query executed has the same data page scan flag value as we specified in the connection
     * properties. Queries are executed with batching.
     *
     * @param qryWithParam query that has one int positional parameter to run.
     * @param dps data page scan value to test.
     */
    private void checkDataPageScanInBatch(String qryWithParam, @Nullable Boolean dps) throws Exception {
        String params = (dps == null) ? null : "dataPageScanEnabled=" + dps;

        int expCnt = 0;

        try (Connection conn = GridTestUtils.connect(grid(0), params)) {
            try (PreparedStatement upd = conn.prepareStatement(qryWithParam)) {
                for (int i = 0; i < TOTAL_QUERIES_TO_EXECUTE; i++) {
                    upd.setInt(1, i);
                    upd.addBatch();

                    if ((i + 1) % BATCH_SIZE == 0 || (i + 1) == TOTAL_QUERIES_TO_EXECUTE) {
                        upd.executeBatch();

                        expCnt++;
                    }
                }
            }
        }

        // Some update operations may produce additional SELECTS, but all the queries should have specified flag value.
        boolean containsOrig = IndexingWithQueries.queries.stream()
            .anyMatch(executedQry -> qryWithParam.equals(executedQry.getSql()));

        assertTrue("Original query have not been executed.", containsOrig);

        IndexingWithQueries.queries.forEach(query ->
            assertEquals("Data page scan flag value is unexpected for query " + query, dps, null)
        );

        int executed = IndexingWithQueries.queries.size();

        assertEquals(expCnt, executed);

        IndexingWithQueries.queries.clear();
    }

    /**
     * Checks that executed query has the same data page scan flag value as we specified in the connection properties.
     *
     * @param qry query with no positional parameters.
     * @param dps data page scan value to test.
     */
    private void checkDataPageScan(String qry, @Nullable Boolean dps) throws Exception {
        String params = (dps == null) ? null : "dataPageScanEnabled=" + dps;

        try (Connection conn = GridTestUtils.connect(grid(0), params)) {
            try (PreparedStatement stmt = conn.prepareStatement(qry)) {
                stmt.execute();
            }
        }

        // Some update operations may produce additional SELECTS, but all the queries should have specified flag value.
        boolean containsOrig = IndexingWithQueries.queries.stream()
            .anyMatch(executedQry -> qry.equals(executedQry.getSql()));

        assertTrue("Original query have not been executed.", containsOrig);

        IndexingWithQueries.queries.forEach(executedQry ->
            assertEquals("Data page scan flag value is unexpected for query " + executedQry, dps, null)
        );

        int executed = IndexingWithQueries.queries.size();

        assertTrue(
            "Expected that there are executed at least one query. " +
                "But executed only " + executed,
            executed >= 1);

        IndexingWithQueries.queries.clear();
    }

    /**
     * Indexing that remembers all the sql fields queries have been executed.
     */
    private static class IndexingWithQueries extends IgniteH2Indexing {
        /** All the queries that have been executed using this indexing. */
        static final Queue<SqlFieldsQuery> queries = new LinkedBlockingQueue<>();

        /** {@inheritDoc} */
        @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(
            String schemaName,
            SqlFieldsQuery qry,
            @Nullable SqlClientContext cliCtx,
            boolean keepBinary,
            boolean failOnMultipleStmts,
            GridQueryCancel cancel
        ) {
            queries.add(qry);

            return super.querySqlFields(
                schemaName,
                qry,
                cliCtx,
                keepBinary,
                failOnMultipleStmts,
                cancel
            );
        }
    }
}
