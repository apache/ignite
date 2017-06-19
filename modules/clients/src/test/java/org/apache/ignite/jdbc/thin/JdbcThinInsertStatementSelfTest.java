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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Statement test.
 */
public class JdbcThinInsertStatementSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL = "insert into Person(_key, id, firstName, lastName, age) values " +
        "('p1', 1, 'John', 'White', 25), " +
        "('p2', 2, 'Joe', 'Black', 35), " +
        "('p3', 3, 'Mike', 'Green', 40)";

    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age) values " +
        "(?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)";

    /** Arguments for prepared statement. */
    private final Object [][] args = new Object[][] {
        {"p1", 1, "John", "White", 25},
        {"p3", 3, "Mike", "Green", 40},
        {"p2", 2, "Joe", "Black", 35}
    };

    /** Statement. */
    private Statement stmt;

    /** Prepared statement. */
    private PreparedStatement prepStmt;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stmt = conn.createStatement();

        prepStmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());

        assertNotNull(prepStmt);
        assertFalse(prepStmt.isClosed());

        int paramCnt = 1;

        for (Object[] arg : args) {
            prepStmt.setString(paramCnt++, (String)arg[0]);
            prepStmt.setInt(paramCnt++, (Integer)arg[1]);
            prepStmt.setString(paramCnt++, (String)arg[2]);
            prepStmt.setString(paramCnt++, (String)arg[3]);
            prepStmt.setInt(paramCnt++, (Integer)arg[4]);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();

        if (prepStmt != null && !prepStmt.isClosed())
            prepStmt.close();

        assertTrue(prepStmt.isClosed());
        assertTrue(stmt.isClosed());

        super.afterTest();
    }

    /**
     * @throws SQLException If failed.
     */
    private void checkTableContent() throws SQLException {
        try (Statement selStmt = conn.createStatement()) {
            assertTrue(selStmt.execute(SQL_SELECT));

            ResultSet rs = selStmt.getResultSet();

            assert rs != null;

            while (rs.next()) {
                int id = rs.getInt("id");

                switch (id) {
                    case 1:
                        assertEquals("p1", rs.getString("_key"));
                        assertEquals("John", rs.getString("firstName"));
                        assertEquals("White", rs.getString("lastName"));
                        assertEquals(25, rs.getInt("age"));
                        break;

                    case 2:
                        assertEquals("p2", rs.getString("_key"));
                        assertEquals("Joe", rs.getString("firstName"));
                        assertEquals("Black", rs.getString("lastName"));
                        assertEquals(35, rs.getInt("age"));
                        break;

                    case 3:
                        assertEquals("p3", rs.getString("_key"));
                        assertEquals("Mike", rs.getString("firstName"));
                        assertEquals("Green", rs.getString("lastName"));
                        assertEquals(40, rs.getInt("age"));
                        break;

                    case 4:
                        assertEquals("p4", rs.getString("_key"));
                        assertEquals("Leah", rs.getString("firstName"));
                        assertEquals("Grey", rs.getString("lastName"));
                        assertEquals(22, rs.getInt("age"));
                        break;

                    default:
                        assert false : "Invalid ID: " + id;
                }
            }
        }
    }

    /**
     * @throws SQLException If failed.
     */
    public void testExecuteUpdate() throws SQLException {
        assertEquals(3, stmt.executeUpdate(SQL));

        checkTableContent();
    }

    /**
     * @throws SQLException If failed.
     */
    public void testPreparedExecuteUpdate() throws SQLException {
        assertEquals(3, prepStmt.executeUpdate());

        checkTableContent();
    }

    /**
     * @throws SQLException If failed.
     */
    public void testExecute() throws SQLException {
        assertFalse(stmt.execute(SQL));

        checkTableContent();
    }

    /**
     * @throws SQLException If failed.
     */
    public void testPreparedExecute() throws SQLException {
        assertFalse(prepStmt.execute());

        checkTableContent();
    }

    /**
     * @throws SQLException If failed.
     */
    public void testDuplicateKeys() throws SQLException {
        jcache(0).put("p2", new Person(2, "Joe", "Black", 35));

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                return stmt.execute(SQL);
            }
        }, IgniteCheckedException.class,
            "Failed to INSERT some keys because they are already in cache [keys=[p2]]");

        assertEquals(3, jcache(0).withKeepBinary().getAll(new HashSet<>(Arrays.asList("p1", "p2", "p3"))).size());

        checkTableContent();
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

        for (int i = 0; i < 10; ++i)
            assertEquals("Invalid update count",i + 1, updCnts[i]);
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
