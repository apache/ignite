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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Statement test.
 */
public class JdbcThinBatchSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age) values " +
        "(?, ?, ?, ?, ?)";

    /** Statement. */
    private Statement stmt;

    /** Prepared statement. */
    private PreparedStatement pstmt;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stmt = conn.createStatement();

        pstmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());

        assertNotNull(pstmt);
        assertFalse(pstmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();

        if (pstmt != null && !pstmt.isClosed())
            pstmt.close();

        assertTrue(pstmt.isClosed());
        assertTrue(stmt.isClosed());

        super.afterTest();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatch() throws SQLException {
        final int BATCH_SIZE = 10;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        int[] updCnts = stmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count", i + 1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchOnClosedStatement() throws SQLException {
        final Statement stmt2 = conn.createStatement();
        final PreparedStatement pstmt2 = conn.prepareStatement("");

        stmt2.close();
        pstmt2.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt2.addBatch("");

                return null;
            }
        }, SQLException.class, "Statement is closed.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt2.clearBatch();

                return null;
            }
        }, SQLException.class, "Statement is closed.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt2.executeBatch();

                return null;
            }
        }, SQLException.class, "Statement is closed.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                pstmt2.addBatch();

                return null;
            }
        }, SQLException.class, "Statement is closed.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                pstmt2.clearBatch();

                return null;
            }
        }, SQLException.class, "Statement is closed.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                pstmt2.executeBatch();

                return null;
            }
        }, SQLException.class, "Statement is closed.");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        for (int idx = 0, i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("select * from Person");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count", i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED,
                    updCnts[i]);

            if (!e.getMessage().contains("Given statement type does not match that declared by JDBC driver")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.PARSING_EXCEPTION, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.STMT_TYPE_MISMATCH, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchParseException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        for (int idx = 0, i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values (4444, 'fail', 1, 1, 1)");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count: " + i, i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED,
                    updCnts[i]);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMerge() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        int[] updCnts = stmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count", i + 1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMergeParseException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        for (int idx = 0, i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values (4444, 'FAIL', 1, 1, 1)");

        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count: " + i, i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED,
                    updCnts[i]);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchKeyDuplicatesException() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        int idx = 0;

        for (int i = 0; i < FAILED_IDX; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values ('p0', 0, 'Name0', 'Lastname0', 20)");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(++idx, 7));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count: " + i, i != FAILED_IDX ? i + 1 : Statement.EXECUTE_FAILED,
                    updCnts[i]);

            if (!e.getMessage().contains("Failed to INSERT some keys because they are already in cache [keys=[p0]")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.CONSTRAINT_VIOLATION, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.DUPLICATE_KEY, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testHeterogeneousBatch() throws SQLException {
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values ('p0', 0, 'Name0', 'Lastname0', 10)");
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values ('p1', 1, 'Name1', 'Lastname1', 20), ('p2', 2, 'Name2', 'Lastname2', 30)");
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p3', 3, 'Name3', 'Lastname3', 40)");
        stmt.addBatch("update Person set id = 5 where age >= 30");
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p0', 2, 'Name2', 'Lastname2', 50)");
        stmt.addBatch("delete from Person where age <= 40");

        int[] updCnts = stmt.executeBatch();

        assertEquals("Invalid update counts size", 6, updCnts.length);
        assertArrayEquals("Invalid update count", new int[] {1, 2, 1, 2, 1, 3}, updCnts);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testHeterogeneousBatchException() throws Exception {
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values ('p0', 0, 'Name0', 'Lastname0', 10)");
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) " +
            "values ('p1', 1, 'Name1', 'Lastname1', 20), ('p2', 2, 'Name2', 'Lastname2', 30)");
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p3', 3, 'Name3', 'Lastname3', 40)");
        stmt.addBatch("update Person set id = 'FAIL' where age >= 30"); // Fail.
        stmt.addBatch("merge into Person (_key, id, firstName, lastName, age) values ('p0', 2, 'Name2', 'Lastname2', 50)");
        stmt.addBatch("delete from Person where FAIL <= 40"); // Fail.

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid update counts size", 6, updCnts.length);
            assertArrayEquals("Invalid update count",
                new int[] {1, 2, 1, Statement.EXECUTE_FAILED, 1, Statement.EXECUTE_FAILED}, updCnts);
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchClear() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.clearBatch();

        int[] updates = stmt.executeBatch();

        assertEquals("Returned update counts array should have no elements for empty batch.",
            0, updates.length);
    }

    /**
     * Check that user can execute empty batch using Statement. Such behaviour is more user friendly than forbidding
     * empty batches.
     *
     * @throws SQLException on error.
     */
    @Test
    public void testEmptyBatchStreaming() throws SQLException {
        executeUpdateOn(conn, "SET STREAMING ON");

        int[] updates = stmt.executeBatch();

        assertEquals("Returned update counts array should have no elements for empty batch.",
            0, updates.length);

        executeUpdateOn(conn, "SET STREAMING OFF");

        assertEquals("Test table should be empty after empty batch is performed.", 0L, personsCount());
    }

    /**
     * Same as {@link #testEmptyBatchStreaming()} but for PreparedStatement.
     *
     * @throws SQLException on error.
     */
    @Test
    public void testEmptyBatchStreamingPrepared() throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("SET STREAMING ON");
        }

        int[] updates = pstmt.executeBatch();

        assertEquals("Returned update counts array should have no elements for empty batch.",
            0, updates.length);

        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate("SET STREAMING OFF");
        }

        assertEquals("Test table should be empty after empty batch is performed.", 0L, personsCount());
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchPrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        for (int i = 0; i < BATCH_SIZE; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count", 1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX);
        pstmt.setString(paramCnt++, "FAIL");
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX);

        pstmt.addBatch();

        paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            pstmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count", i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i]);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMergePrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        pstmt = conn.prepareStatement("merge into Person(_key, id, firstName, lastName, age) values " +
            "(?, ?, ?, ?, ?)");

        for (int i = 0; i < BATCH_SIZE; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count", 1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchMergeExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        pstmt = conn.prepareStatement("merge into Person(_key, id, firstName, lastName, age) values " +
            "(?, ?, ?, ?, ?)");

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX);
        pstmt.setString(paramCnt++, "FAIL");
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX);

        pstmt.addBatch();

        paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Name" + FAILED_IDX + 1);
        pstmt.setString(paramCnt++, "Lastname" + FAILED_IDX + 1);
        pstmt.setInt(paramCnt++, 20 + FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count", i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i]);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    /**
     * Populates table 'Person' with entities.
     *
     * @param size Number of entities.
     * @throws SQLException If failed.
     */
    private void populateTable(int size) throws SQLException {
        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(0, size));

        stmt.executeBatch();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchUpdatePrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("update Person set age = 100 where id = ?;");

        for (int i = 0; i < BATCH_SIZE; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count", 1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchUpdateExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("update Person set age = 100 where id = ?;");

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        pstmt.setString(1, "FAIL");

        pstmt.addBatch();

        pstmt.setInt(1, FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i) {
                assertEquals("Invalid update count[" + i + ']',
                    i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i]);
            }

            if (!e.getMessage().contains("Data conversion error converting \"FAIL\"")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.INTERNAL_ERROR, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.UNKNOWN, e.getErrorCode());

            //assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            //assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchDeletePrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("delete from Person where id = ?;");

        for (int i = 0; i < BATCH_SIZE; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        int[] updCnts = pstmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count", 1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchDeleteExceptionPrepared() throws Exception {
        final int BATCH_SIZE = 7;

        final int FAILED_IDX = 5;

        populateTable(BATCH_SIZE);

        pstmt = conn.prepareStatement("delete from Person where id = ?;");

        assert FAILED_IDX + 2 == BATCH_SIZE;

        for (int i = 0; i < FAILED_IDX; ++i) {
            pstmt.setInt(1, i);

            pstmt.addBatch();
        }

        pstmt.setString(1, "FAIL");

        pstmt.addBatch();

        pstmt.setInt(1, FAILED_IDX + 1);

        pstmt.addBatch();

        try {
            int[] res = pstmt.executeBatch();

            fail("BatchUpdateException must be thrown res=" + Arrays.toString(res));
        }
        catch (BatchUpdateException e) {
            checkThereAreNotUsedConnections();

            int[] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count", i != FAILED_IDX ? 1 : Statement.EXECUTE_FAILED, updCnts[i]);

            if (!e.getMessage().contains("Data conversion error converting \"FAIL\"")) {
                log.error("Invalid exception: ", e);

                fail();
            }

            assertEquals("Invalid SQL state.", SqlStateCode.INTERNAL_ERROR, e.getSQLState());
            assertEquals("Invalid error code.", IgniteQueryErrorCode.UNKNOWN, e.getErrorCode());

            //assertEquals("Invalid SQL state.", SqlStateCode.CONVERSION_FAILED, e.getSQLState());
            //assertEquals("Invalid error code.", IgniteQueryErrorCode.CONVERSION_FAILED, e.getErrorCode());
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatchClearPrepared() throws SQLException {
        final int BATCH_SIZE = 10;

        for (int persIdx = 0; persIdx < BATCH_SIZE; ++persIdx) {
            fillParamsWithPerson(pstmt, persIdx);

            pstmt.addBatch();
        }

        pstmt.clearBatch();

        int[] updates = pstmt.executeBatch();

        assertEquals("Returned update counts array should have no elements for empty batch.",
            0, updates.length);

        assertEquals("Test table should be empty after empty batch is performed.", 0L, personsCount());
    }

    /**
     * @param beginIndex Begin row index.
     * @param cnt Count of rows.
     * @return String contains values for 'cnt' rows.
     */
    private String generateValues(int beginIndex, int cnt) {
        StringBuilder sb = new StringBuilder();

        int lastIdx = beginIndex + cnt - 1;

        for (int i = beginIndex; i < lastIdx; ++i)
            sb.append(valuesRow(i)).append(',');

        sb.append(valuesRow(lastIdx));

        return sb.toString();
    }

    /**
     * @param idx Index of the row.
     * @return String with row values.
     */
    private String valuesRow(int idx) {
        return String.format("('p%d', %d, 'Name%d', 'Lastname%d', %d)", idx, idx, idx, idx, 20 + idx);
    }

    /**
     * Fills PreparedStatement's parameters with fields of some Person generated by index.
     *
     * @param stmt PreparedStatement to fill
     * @param personIdx number to generate Person's fields.
     * @throws SQLException on error.
     */
    private static void fillParamsWithPerson(PreparedStatement stmt, int personIdx) throws SQLException {
        int paramCnt = 1;

        stmt.setString(paramCnt++, "p" + personIdx);
        stmt.setInt(paramCnt++, personIdx);
        stmt.setString(paramCnt++, "Name" + personIdx);
        stmt.setString(paramCnt++, "Lastname" + personIdx);
        stmt.setInt(paramCnt++, 20 + personIdx);
    }

    /**
     * @return How many rows Person table contains.
     * @throws SQLException on error.
     */
    private long personsCount() throws SQLException {
        try (ResultSet cnt = stmt.executeQuery("SELECT COUNT(*) FROM PERSON;")) {
            cnt.next();

            return cnt.getLong(1);
        }
    }

    /**
     * Executes update query on specified connection.
     *
     * @param conn Connection to use.
     * @param updQry sql update query.
     * @throws SQLException on error.
     */
    private static void executeUpdateOn(Connection conn, String updQry) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(updQry);
        }
    }
}
