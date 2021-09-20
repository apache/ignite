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

package org.apache.ignite.internal.runner.app.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Statement test.
 */
@SuppressWarnings({"ThrowableNotThrown"})
public class ITJdbcStatementSelfTest extends AbstractJdbcSelfTest {
    /** SQL query. */
    private static final String SQL =
        "select 1::INTEGER, true, 1::TINYINT, 1::SMALLINT, 1::INTEGER, 1::BIGINT, 1.0::FLOAT, 1.0::DOUBLE, 1.0::DOUBLE, '1';";

    /** Statement. */
    private Statement stmt;

    /** Connection. */
    private Connection conn;

    /**
     * Create the connection ant statement.
     *
     * @throws Exception if failed.
     */
    @BeforeEach
    protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);
        
        stmt = conn.createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    /**
     * Close the connection and statement.
     *
     * @throws Exception if failed.
     */
    @AfterEach
    protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assertTrue(stmt.isClosed());
        }

        conn.close();

        assertTrue(stmt.isClosed());
        assertTrue(conn.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled("IGNITE-15108")
    public void testExecuteQuery0() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals("Mike", rs.getString("firstName"));
                assertEquals("Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQuery1() throws Exception {
        final String sqlText = "select 5;";

        try (ResultSet rs = stmt.executeQuery(sqlText)) {
            assertNotNull(rs);

            assertTrue(rs.next());

            int val = rs.getInt(1);

            assertTrue(val >= 1 && val <= 10, "Invalid val: " + val);
        }

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(() -> stmt.executeQuery(sqlText));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled("IGNITE-15108")
    public void testExecute() throws Exception {
        assertTrue(stmt.execute(SQL));

        assertEquals(-1, stmt.getUpdateCount(), "Update count must be -1 for SELECT query");

        ResultSet rs = stmt.getResultSet();

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals( "Mike", rs.getString("firstName"));
                assertEquals( "Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);

        assertFalse(stmt.getMoreResults(), "Statement has more results.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled("IGNITE-15108")
    public void testMaxRows() throws Exception {
        stmt.setMaxRows(1);

        assertEquals(1, stmt.getMaxRows());

        ResultSet rs = stmt.executeQuery(SQL);

        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals( "Mike", rs.getString("firstName"));
                assertEquals( "Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(1, cnt);

        stmt.setMaxRows(0);

        rs = stmt.executeQuery(SQL);

        assertNotNull(rs);

        cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assertEquals("Joe", rs.getString("firstName"));
                assertEquals("Black", rs.getString("lastName"));
                assertEquals(35, rs.getInt("age"));
            }
            else if (id == 3) {
                assertEquals( "Mike", rs.getString("firstName"));
                assertEquals( "Green", rs.getString("lastName"));
                assertEquals(40, rs.getInt("age"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCloseResultSet0() throws Exception {
        ResultSet rs0 = stmt.executeQuery(SQL);
        ResultSet rs1 = stmt.executeQuery(SQL);
        ResultSet rs2 = stmt.executeQuery(SQL);

        assertTrue(rs0.isClosed(), "ResultSet must be implicitly closed after re-execute statement");
        assertTrue(rs1.isClosed(), "ResultSet must be implicitly closed after re-execute statement");

        assertFalse(rs2.isClosed(), "Last result set must be available");

        stmt.close();

        assertTrue(rs2.isClosed(), "ResultSet must be explicitly closed after close statement");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCloseResultSet1() throws Exception {
        stmt.execute(SQL);

        ResultSet rs = stmt.getResultSet();

        stmt.close();

        assertTrue(rs.isClosed(), "ResultSet must be explicitly closed after close statement");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCloseResultSetByConnectionClose() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        conn.close();

        assertTrue(stmt.isClosed(), "Statement must be implicitly closed after close connection");
        assertTrue(rs.isClosed(), "ResultSet must be implicitly closed after close connection");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCloseOnCompletionAfterQuery() throws Exception {
        assertFalse(stmt.isCloseOnCompletion(), "Invalid default closeOnCompletion");

        ResultSet rs0 = stmt.executeQuery(SQL);

        ResultSet rs1 = stmt.executeQuery(SQL);

        assertTrue(rs0.isClosed(), "Result set must be closed implicitly");

        assertFalse(stmt.isClosed(), "Statement must not be closed");

        rs1.close();

        assertFalse(stmt.isClosed(), "Statement must not be closed");

        ResultSet rs2 = stmt.executeQuery(SQL);

        stmt.closeOnCompletion();

        assertTrue(stmt.isCloseOnCompletion(), "Invalid closeOnCompletion");

        rs2.close();

        assertTrue(stmt.isClosed(), "Statement must be closed");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCloseOnCompletionBeforeQuery() throws Exception {
        assertFalse(stmt.isCloseOnCompletion(), "Invalid default closeOnCompletion");

        ResultSet rs0 = stmt.executeQuery(SQL);

        ResultSet rs1 = stmt.executeQuery(SQL);

        assertTrue(rs0.isClosed(), "Result set must be closed implicitly");

        assertFalse(stmt.isClosed(), "Statement must not be closed");

        rs1.close();

        assertFalse(stmt.isClosed(), "Statement must not be closed");

        stmt.closeOnCompletion();

        ResultSet rs2 = stmt.executeQuery(SQL);

        assertTrue(stmt.isCloseOnCompletion(), "Invalid closeOnCompletion");

        rs2.close();

        assertTrue(stmt.isClosed(), "Statement must be closed");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQueryMultipleOnlyResultSets() throws Exception {
//        assertTrue(conn.getMetaData().supportsMultipleResultSets());

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder();

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("select ").append(i).append("; ");

        assertTrue(stmt.execute(sql.toString()));

        for (int i = 0; i < stmtCnt - 1; ++i) {
            ResultSet rs = stmt.getResultSet();

            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertFalse(rs.next());

            assertTrue(stmt.getMoreResults());
        }

        ResultSet rs = stmt.getResultSet();

        assertTrue(rs.next());
        assertEquals(stmtCnt - 1, rs.getInt(1));
        assertFalse(rs.next());

        assertFalse(stmt.getMoreResults());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled("IGNITE-15108")
    public void testExecuteQueryMultipleOnlyDml() throws Exception {
        conn.setSchema(null);

        Statement stmt0 = conn.createStatement();

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder("drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("insert into test (ID, NAME) values (" + i + ", 'name_" + i + "'); ");

        assertFalse(stmt0.execute(sql.toString()));

        // DROP TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        stmt0.getMoreResults();

        // CREATE TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        for (int i = 0; i < stmtCnt; ++i) {
            assertTrue(stmt0.getMoreResults());

            assertNull(stmt0.getResultSet());
            assertEquals(1, stmt0.getUpdateCount());
        }

        assertFalse(stmt0.getMoreResults());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled("IGNITE-15108")
    public void testExecuteQueryMultipleMixed() throws Exception {
        conn.setSchema(null);

        Statement stmt0 = conn.createStatement();

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder("drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i) {
            if (i % 2 == 0)
                sql.append(" insert into test (ID, NAME) values (" + i + ", 'name_" + i + "'); ");
            else
                sql.append(" select * from test where id < " + i + "; ");
        }

        assertFalse(stmt0.execute(sql.toString()));

        // DROP TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        assertTrue(stmt0.getMoreResults(), "Result set doesn't have more results.");

        // CREATE TABLE statement
        assertNull(stmt0.getResultSet());
        assertEquals(0, stmt0.getUpdateCount());

        boolean notEmptyResult = false;

        for (int i = 0; i < stmtCnt; ++i) {
            assertTrue(stmt0.getMoreResults());

            if (i % 2 == 0) {
                assertNull(stmt0.getResultSet());
                assertEquals(1, stmt0.getUpdateCount());
            }
            else {
                assertEquals(-1, stmt0.getUpdateCount());

                ResultSet rs = stmt0.getResultSet();

                int rowsCnt = 0;

                while (rs.next())
                    rowsCnt++;

                assertTrue(rowsCnt <= (i + 1) / 2);

                if (rowsCnt == (i + 1) / 2)
                    notEmptyResult = true;

                assertTrue(notEmptyResult);

                assertFalse(stmt0.getMoreResults());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled("IGNITE-15108")
    public void testExecuteUpdate() throws Exception {
        final String sqlText = "update test set val=1 where _key=1";

        assertEquals(1, stmt.executeUpdate(sqlText));

        stmt.close();

        checkStatementClosed(() -> stmt.executeUpdate(sqlText));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteUpdateProducesResultSet() {
        final String sqlText = "select * from test";

        assertThrows(SQLException.class, () -> stmt.executeUpdate(sqlText),
            "Given statement type does not match that declared by JDBC driver"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClose() throws Exception {
        String sqlText = "select 1";

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());
        assertFalse(rs.isClosed());

        assertFalse(stmt.isClosed());

        stmt.close();
        stmt.close(); // Closing closed is ok

        assertTrue(stmt.isClosed());

        // Current result set must be closed
        assertTrue(rs.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetMaxFieldSizeUnsupported() throws Exception {
        assertEquals(0, stmt.getMaxFieldSize());

        assertThrows(SQLFeatureNotSupportedException.class, () -> stmt.setMaxFieldSize(100),
            "Field size limitation is not supported");

        assertEquals(0, stmt.getMaxFieldSize());

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(() -> stmt.getMaxFieldSize());

        // Call on a closed statement
        checkStatementClosed(() -> stmt.setMaxFieldSize(100));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testGetSetMaxRows() throws Exception {
        assertEquals(0, stmt.getMaxRows());

        assertThrows(SQLException.class, () -> stmt.setMaxRows(-1),
            "Invalid max rows value");

        assertEquals(0, stmt.getMaxRows());

        final int maxRows = 1;

        stmt.setMaxRows(maxRows);

        assertEquals(maxRows, stmt.getMaxRows());

        String sqlText = "select * from test";

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());
        assertFalse(rs.next()); //max rows reached

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(() -> stmt.getMaxRows());

        // Call on a closed statement
        checkStatementClosed(() -> stmt.setMaxRows(maxRows));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetQueryTimeout() throws Exception {
        assertEquals(0, stmt.getQueryTimeout());

        assertThrows(SQLException.class, () -> stmt.setQueryTimeout(-1),
            "Invalid timeout value");

        assertEquals(0, stmt.getQueryTimeout());

        final int timeout = 3;

        stmt.setQueryTimeout(timeout);

        assertEquals(timeout, stmt.getQueryTimeout());

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(() -> stmt.getQueryTimeout());

        // Call on a closed statement
        checkStatementClosed(() -> stmt.setQueryTimeout(timeout));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMaxFieldSize() throws Exception {
        assertTrue(stmt.getMaxFieldSize() >= 0);

        assertThrows(SQLException.class, () -> stmt.setMaxFieldSize(-1),            
            "Invalid field limit");

        checkNotSupported(() -> stmt.setMaxFieldSize(100));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryTimeout() throws Exception {
        assertEquals(0, stmt.getQueryTimeout(), "Default timeout invalid: " + stmt.getQueryTimeout());

        stmt.setQueryTimeout(10);

        assertEquals(10, stmt.getQueryTimeout());

        stmt.close();

        checkStatementClosed(() -> stmt.getQueryTimeout());

        checkStatementClosed(() -> stmt.setQueryTimeout(10));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWarningsOnClosedStatement() throws Exception {
        stmt.clearWarnings();

        assertNull(stmt.getWarnings());

        stmt.close();

        checkStatementClosed(() -> stmt.getWarnings());

        checkStatementClosed(() -> stmt.clearWarnings());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCursorName() throws Exception {
        checkNotSupported(() -> stmt.setCursorName("test"));

        stmt.close();

        checkStatementClosed(() -> stmt.setCursorName("test"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetMoreResults() throws Exception {
        assertFalse(stmt.getMoreResults());

        stmt.execute("select 1; ");

        ResultSet rs = stmt.getResultSet();

        assertFalse(stmt.getMoreResults());

        assertNull(stmt.getResultSet());

        assertTrue(rs.isClosed());

        stmt.close();

        checkStatementClosed(() -> stmt.getMoreResults());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetMoreResultsKeepCurrent() throws Exception {
        assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        stmt.execute("select 1; ");

        ResultSet rs = stmt.getResultSet();

        assertThrows(SQLFeatureNotSupportedException.class, () -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));

        stmt.close();

        checkStatementClosed(() -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetMoreResultsCloseAll() throws Exception {
        assertFalse(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        assertFalse(stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));

        stmt.execute("select 1; ");

        assertThrows(SQLFeatureNotSupportedException.class, () -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));

        stmt.close();

        checkStatementClosed(() -> stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
    }

    /**
     * TODO Enable when batch query is supported
     *
     * Verifies that empty batch can be performed.
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testBatchEmpty() throws Exception {
        assertTrue(conn.getMetaData().supportsBatchUpdates());

        stmt.addBatch("");
        stmt.clearBatch();

        // Just verify that no exception have been thrown.
        stmt.executeBatch();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFetchDirection() throws Exception {
        assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());

        assertThrows(
            SQLFeatureNotSupportedException.class,
            () -> stmt.setFetchDirection(ResultSet.FETCH_REVERSE),
            "Only forward direction is supported."
        );

        stmt.close();

        checkStatementClosed(() -> stmt.setFetchDirection(-1));

        checkStatementClosed(() -> stmt.getFetchDirection());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAutogenerated() throws Exception {
        assertThrows(
            SQLException.class,
            () -> stmt.executeUpdate("select 1", -1),
            "Invalid autoGeneratedKeys value"
        );

        assertThrows(
            SQLException.class,
            () -> stmt.execute("select 1", -1),
            "Invalid autoGeneratedKeys value"
        );

//        assertFalse(conn.getMetaData().supportsGetGeneratedKeys());

        checkNotSupported(() -> stmt.getGeneratedKeys());

        checkNotSupported(() -> stmt.executeUpdate("select 1", Statement.RETURN_GENERATED_KEYS));

        checkNotSupported(() -> stmt.executeUpdate("select 1", new int[] {1, 2}));

        checkNotSupported(() -> stmt.executeUpdate("select 1", new String[] {"a", "b"}));

        checkNotSupported(() -> stmt.execute("select 1", Statement.RETURN_GENERATED_KEYS));

        checkNotSupported(() -> stmt.execute("select 1", new int[] {1, 2}));

        checkNotSupported(() -> stmt.execute("select 1", new String[] {"a", "b"}));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStatementTypeMismatchSelectForCachedQuery() throws Exception {
        // Put query to cache.
        stmt.executeQuery("select 1;");

        assertThrows(
            SQLException.class,                    
            () -> stmt.executeUpdate("select 1;"),
            "Given statement type does not match that declared by JDBC driver"
        );

        assertNull(stmt.getResultSet(), "Not results expected. Last statement is executed with exception");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled("IGNITE-15108")
    public void testStatementTypeMismatchUpdate() throws Exception {
        assertThrows(
            SQLException.class,
            () -> stmt.executeQuery("update test set val=28 where _key=1"), 
            "Given statement type does not match that declared by JDBC driver"
        );
        
        ResultSet rs = stmt.executeQuery("select val from test where _key=1");

        boolean next = rs.next();

        assertTrue(next);

        assertEquals(
            1,
            rs.getInt(1),
            "The data must not be updated. " +
                "Because update statement is executed via 'executeQuery' method." +
                " Data [val=" + rs.getInt(1) + ']'
        );
    }
}
