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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/** Savepoint tests for thin JDBC connection. */
@RunWith(Parameterized.class)
public class JdbcThinConnectionSavepointTest extends AbstractJdbcTest {
    /** */
    private static final String TBL = "SAVEPOINT_TEST_TABLE";

    /** JDBC URL. */
    private static final String SAVEPOINT_URL = URL + "?queryEngine=" + CalciteQueryEngineConfiguration.ENGINE_NAME;

    /** Transaction concurrency. */
    @Parameter
    public TransactionConcurrency txConcurrency;

    /**
     * @return Test parameters.
     */
    @Parameters(name = "{0}")
    public static Iterable<Object[]> testData() {
        return Arrays.asList(new Object[][] {
            {PESSIMISTIC},
            {OPTIMISTIC}
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setTransactionConfiguration(new TransactionConfiguration()
                .setTxAwareQueriesEnabled(true))
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        try (Connection conn = connection()) {
            execute(conn, "DROP TABLE IF EXISTS " + TBL);
            execute(conn, "CREATE TABLE " + TBL +
                "(ID INT PRIMARY KEY, VAL VARCHAR) WITH atomicity=transactional");
        }
    }

    /** */
    @Test
    public void testJdbcSavepointApiRollsBackSqlDmlChanges() throws Exception {
        try (Connection conn = connection()) {
            assertTrue(conn.getMetaData().supportsSavepoints());

            conn.setAutoCommit(false);

            try {
                execute(conn, "INSERT INTO " + TBL + " VALUES (1, 'before_sp1')");

                Savepoint sp1 = conn.setSavepoint("sp1");

                execute(conn, "UPDATE " + TBL + " SET VAL = 'after_sp1' WHERE ID = 1");
                execute(conn, "INSERT INTO " + TBL + " VALUES (2, 'after_sp1')");

                Savepoint sp2 = conn.setSavepoint("sp2");

                execute(conn, "DELETE FROM " + TBL + " WHERE ID = 1");
                execute(conn, "INSERT INTO " + TBL + " VALUES (3, 'after_sp2')");

                assertQuery(conn, 2, "after_sp1", 3, "after_sp2");

                conn.rollback(sp2);

                assertQuery(conn, 1, "after_sp1", 2, "after_sp1");

                conn.releaseSavepoint(sp2);
                conn.rollback(sp1);

                assertQuery(conn, 1, "before_sp1");

                conn.releaseSavepoint(sp1);
                conn.commit();
            }
            catch (Throwable t) {
                conn.rollback();

                throw t;
            }
        }

        try (Connection conn = connection()) {
            assertQuery(conn, 1, "before_sp1");
        }
    }

    /** */
    @Test
    public void testJdbcSavepointCanStartTransactionBeforeSqlDml() throws Exception {
        try (Connection conn = connection()) {
            conn.setAutoCommit(false);

            try {
                Savepoint sp1 = conn.setSavepoint("sp1");

                execute(conn, "INSERT INTO " + TBL + " VALUES (1, 'after_sp1')");

                assertQuery(conn, 1, "after_sp1");

                conn.rollback(sp1);
                conn.commit();
            }
            catch (Throwable t) {
                conn.rollback();

                throw t;
            }
        }

        try (Connection conn = connection()) {
            assertQuery(conn);
        }
    }

    /** */
    @Test
    public void testJdbcUnnamedSavepointApiRollsBackSqlDmlChanges() throws Exception {
        try (Connection conn = connection()) {
            conn.setAutoCommit(false);

            try {
                execute(conn, "INSERT INTO " + TBL + " VALUES (1, 'before_sp')");

                Savepoint sp = conn.setSavepoint();

                execute(conn, "UPDATE " + TBL + " SET VAL = 'after_sp' WHERE ID = 1");
                execute(conn, "INSERT INTO " + TBL + " VALUES (2, 'after_sp')");

                assertQuery(conn, 1, "after_sp", 2, "after_sp");

                conn.rollback(sp);

                assertQuery(conn, 1, "before_sp");

                conn.releaseSavepoint(sp);
                conn.commit();
            }
            catch (Throwable t) {
                conn.rollback();

                throw t;
            }
        }

        try (Connection conn = connection()) {
            assertQuery(conn, 1, "before_sp");
        }
    }

    /** */
    @Test
    public void testSqlDmlChangesCanBeRolledBackToSavepointUsingJdbc() throws Exception {
        try (Connection conn = connection(); Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            try {
                stmt.executeUpdate("INSERT INTO " + TBL + " VALUES (1, 'before_sp1')");

                stmt.execute("SAVEPOINT sp1");

                stmt.executeUpdate("UPDATE " + TBL + " SET VAL = 'after_sp1' WHERE ID = 1");
                stmt.executeUpdate("INSERT INTO " + TBL + " VALUES (2, 'after_sp1')");

                stmt.execute("SAVEPOINT sp2");

                stmt.executeUpdate("DELETE FROM " + TBL + " WHERE ID = 1");
                stmt.executeUpdate("INSERT INTO " + TBL + " VALUES (3, 'after_sp2')");

                assertQuery(conn, 2, "after_sp1", 3, "after_sp2");

                stmt.execute("ROLLBACK TO SAVEPOINT sp2");

                assertQuery(conn, 1, "after_sp1", 2, "after_sp1");

                stmt.execute("ROLLBACK TO SAVEPOINT sp1");

                Savepoint sp = conn.setSavepoint("sp1");
                conn.rollback(sp);
                conn.releaseSavepoint(sp);

                assertThrows(log, () -> {
                    conn.rollback(sp);

                    return null;
                }, SQLException.class, "Savepoint has been released.");

                assertQuery(conn, 1, "before_sp1");

                conn.commit();
            }
            catch (Throwable t) {
                conn.rollback();

                throw t;
            }
        }

        try (Connection conn = connection()) {
            assertQuery(conn, 1, "before_sp1");
        }
    }

    /**
     * @return Connection.
     */
    private Connection connection() throws SQLException {
        return DriverManager.getConnection(SAVEPOINT_URL + "&transactionConcurrency=" + txConcurrency);
    }

    /**
     * @param conn Connection.
     * @param sql SQL.
     */
    private void execute(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * @param conn Connection.
     * @param exp Expected values as column pairs.
     */
    private void assertQuery(Connection conn, Object... exp) throws SQLException {
        List<List<Object>> rows = executeQuery(conn, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID");

        assertEquals(exp.length / 2, rows.size());

        for (int i = 0; i < exp.length; i += 2)
            assertEqualsCollections(Arrays.asList(exp[i], exp[i + 1]), rows.get(i / 2));
    }
}
