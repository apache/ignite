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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.processors.query.QueryHistory;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Jdbc thin transactional partition awareness test.
 */
public class JdbcThinPartitionAwarenessTransactionsSelfTest extends JdbcThinAbstractSelfTest {
    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1:10800..10802?partitionAwareness=true";

    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Query execution multiplier. */
    private static final int QUERY_EXECUTION_MULTIPLIER = 5;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @SuppressWarnings({"deprecation", "unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME).setNearConfiguration(null))
            .setMarshaller(new BinaryMarshaller());
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) {
        return defaultCacheConfiguration()
            .setName(name)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES_CNT);

        try (Connection c = prepareConnection(true, NestedTxMode.ERROR)) {
            try (Statement stmt = c.createStatement()) {
                stmt.execute("CREATE TABLE Person (id int primary key, firstName varchar, lastName varchar, age int) " +
                    "WITH \"cache_name=persons,wrap_value=true,atomicity=transactional_snapshot\"");

                stmt.executeUpdate("insert into Person (id, firstName, lastName, age) values (1, 'John1', 'Dow1', 1);" +
                    "insert into Person (id, firstName, lastName, age) values (2, 'John2', 'Dow2', 2);" +
                    "insert into Person (id, firstName, lastName, age) values (3, 'John3', 'Dow3', 3);");
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(stmt);

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();
    }

    /**
     * @param autoCommit Auto commit mode.
     * @param nestedTxMode Nested transactions mode.
     * @return Connection.
     * @throws SQLException if failed.
     */
    private static Connection prepareConnection(boolean autoCommit, NestedTxMode nestedTxMode) throws SQLException {
        Connection res = DriverManager.getConnection(URL + "&nestedTransactionsMode=" + nestedTxMode.name());

        res.setAutoCommit(autoCommit);

        return res;
    }

    /**
     * Check that queries goes to the same node within transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQueries() throws Exception {
        stmt.execute("BEGIN");
        checkNodesUsage("select * from Person where _key = 1", 1, false);
        stmt.execute("COMMIT");

        stmt.execute("BEGIN");
        checkNodesUsage("select * from Person where _key = 1 or _key = 2", 2, false);
        stmt.execute("COMMIT");

        stmt.execute("BEGIN");
        checkNodesUsage("select * from Person where _key in (1, 2)", 2, false);
        stmt.execute("COMMIT");
    }

    /**
     * Check that dml queries(updates) goes to the same node within transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateQueries() throws Exception {
        stmt.execute("BEGIN");
        checkNodesUsage("update Person set firstName = 'TestFirstName' where _key = 1", 1,
            true);
        stmt.execute("COMMIT");

        stmt.execute("BEGIN");
        checkNodesUsage("update Person set firstName = 'TestFirstName' where _key = 1 or _key = 2",
            2, true);
        stmt.execute("COMMIT");

        stmt.execute("BEGIN");
        checkNodesUsage("update Person set firstName = 'TestFirstName' where _key in (1, 2)",
            2, true);
        stmt.execute("COMMIT");
    }

    /**
     * Check that dml queries(delete) goes to the same node within transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeleteQueries() throws Exception {
        stmt.execute("BEGIN");
        checkNodesUsage("delete from Person where _key = 1000 or _key = 2000", 0, true);
        stmt.execute("COMMIT");

        stmt.execute("BEGIN");
        checkNodesUsage("delete from Person where _key in (1000, 2000)", 0, true);
        stmt.execute("COMMIT");
    }

    /**
     * Utility method that:
     *   1. warms up an affinity cache;
     *   2. resets query history;
     *   3. executes given query multiple times;
     *   4. checks query history metrics in order to verify that not more than expected nodes were used.
     *
     * @param sql Sql query, either prepared statement or sql query should be used.
     * @param expRowsCnt Expected rows count within result.
     * @param dml Flag that signals whether we execute dml or not.
     * @throws Exception If failed.
     */
    private void checkNodesUsage(String sql, int expRowsCnt, boolean dml)
        throws Exception {
        // Warm up an affinity cache.
        if (dml)
            stmt.executeUpdate(sql);
        else
            stmt.executeQuery(sql);

        // Reset query history.
        for (int i = 0; i < NODES_CNT; i++) {
            ((IgniteH2Indexing)grid(i).context().query().getIndexing())
                .runningQueryManager().resetQueryHistoryMetrics();
        }

        // Execute query multiple times
        for (int i = 0; i < NODES_CNT * QUERY_EXECUTION_MULTIPLIER; i++) {
            ResultSet rs = null;

            int updatedRowsCnt = 0;

            if (dml)
                updatedRowsCnt = stmt.executeUpdate(sql);
            else
                rs = stmt.executeQuery(sql);

            if (dml) {
                assertEquals("Unexpected updated rows count: expected [" + expRowsCnt + "]," +
                    " got [" + updatedRowsCnt + "]", expRowsCnt, updatedRowsCnt);
            }
            else {
                assert rs != null;

                int gotRowsCnt = 0;

                while (rs.next())
                    gotRowsCnt++;

                assertEquals("Unexpected rows count: expected [" + expRowsCnt + "], got [" + gotRowsCnt + "]",
                    expRowsCnt, gotRowsCnt);
            }
        }

        // Check query history metrics in order to verify that not more than expected nodes were used.
        int nonEmptyMetricsCntr = 0;
        int qryExecutionsCntr = 0;

        for (int i = 0; i < NODES_CNT; i++) {
            Collection<QueryHistory> metrics = ((IgniteH2Indexing)grid(i).context().query().getIndexing())
                .runningQueryManager().queryHistoryMetrics().values();

            if (!metrics.isEmpty()) {
                nonEmptyMetricsCntr++;
                qryExecutionsCntr += new ArrayList<>(metrics).get(0).executions();
            }
        }

        assertTrue("Unexpected amount of used nodes: expected [0 < nodesCnt <= 1" +
                "], got [" + nonEmptyMetricsCntr + "]",
            nonEmptyMetricsCntr == 1);

        assertEquals("Executions count doesn't match expeted value: expected [" +
                NODES_CNT * QUERY_EXECUTION_MULTIPLIER + "], got [" + qryExecutionsCntr + "]",
            NODES_CNT * QUERY_EXECUTION_MULTIPLIER, qryExecutionsCntr);
    }
}
