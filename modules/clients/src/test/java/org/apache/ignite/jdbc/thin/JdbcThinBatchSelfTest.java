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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;

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
    public void testBatch() throws SQLException {
        final int BATCH_SIZE = 10;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        int [] updCnts = stmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count",i + 1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
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
    public void testBatchException() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("select * from Person");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 1));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch(BatchUpdateException e) {
            int [] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count",i + 1, updCnts[i]);

            if (!e.getMessage().contains("Given statement type does not match that declared by JDBC driver")) {
                log.error("Invalid exception: ", e);

                fail();
            }
        }
    }

    /**
     * @throws SQLException If failed.
     */
    public void testBatchParseException() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values (4444, 'fail', 1, 1, 1)");

        stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
            + generateValues(100, 1));

        try {
            stmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        } catch(BatchUpdateException e) {
            int [] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count",i + 1, updCnts[i]);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }
        }
    }

    /**
     * @throws SQLException If failed.
     */
    public void testBatchClear() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int idx = 0, i = 0; i < BATCH_SIZE; ++i, idx += i) {
            stmt.addBatch("insert into Person (_key, id, firstName, lastName, age) values "
                + generateValues(idx, i + 1));
        }

        stmt.clearBatch();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeBatch();

                return null;
            }
        }, SQLException.class, "Batch is empty.");
    }

    /**
     * @throws SQLException If failed.
     */
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

        int [] updCnts = pstmt.executeBatch();

        assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

        for (int i = 0; i < BATCH_SIZE; ++i)
            assertEquals("Invalid update count",1, updCnts[i]);
    }

    /**
     * @throws SQLException If failed.
     */
    public void testBatchExceptionPrepared() throws SQLException {
        final int BATCH_SIZE = 7;

        for (int i = 0; i < BATCH_SIZE; ++i) {
            int paramCnt = 1;

            pstmt.setString(paramCnt++, "p" + i);
            pstmt.setInt(paramCnt++, i);
            pstmt.setString(paramCnt++, "Name" + i);
            pstmt.setString(paramCnt++, "Lastname" + i);
            pstmt.setInt(paramCnt++, 20 + i);

            pstmt.addBatch();
        }

        int paramCnt = 1;
        pstmt.setString(paramCnt++, "p" + 100);
        pstmt.setString(paramCnt++, "x");
        pstmt.setString(paramCnt++, "Name" + 100);
        pstmt.setString(paramCnt++, "Lastname" + 100);
        pstmt.setInt(paramCnt++, 20 + 100);

        pstmt.addBatch();

        try {
            pstmt.executeBatch();

            fail("BatchUpdateException must be thrown");
        }
        catch(BatchUpdateException e) {
            int [] updCnts = e.getUpdateCounts();

            assertEquals("Invalid update counts size", BATCH_SIZE, updCnts.length);

            for (int i = 0; i < BATCH_SIZE; ++i)
                assertEquals("Invalid update count",1, updCnts[i]);

            if (!e.getMessage().contains("Value conversion failed")) {
                log.error("Invalid exception: ", e);

                fail();
            }
        }
    }

    /**
     * @throws SQLException If failed.
     */
    public void testBatchClearPrepared() throws SQLException {
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

        pstmt.clearBatch();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                pstmt.executeBatch();

                return null;
            }
        }, SQLException.class, "Batch is empty.");
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
}
