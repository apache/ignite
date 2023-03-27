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

package org.apache.ignite.internal.jdbc2;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Statement test.
 */
public class JdbcInsertStatementSelfTest extends JdbcAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL = "insert into Person(_key, id, firstName, lastName, age, data) values " +
        "('p1', 1, 'John', 'White', 25, RAWTOHEX('White')), " +
        "('p2', 2, 'Joe', 'Black', 35, RAWTOHEX('Black')), " +
        "('p3', 3, 'Mike', 'Green', 40, RAWTOHEX('Green'))";

    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age, data) values " +
        "(?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)";

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
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
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
                        assertEquals("White", str(getBytes(rs.getBlob("data"))));
                        break;

                    case 2:
                        assertEquals("p2", rs.getString("_key"));
                        assertEquals("Joe", rs.getString("firstName"));
                        assertEquals("Black", rs.getString("lastName"));
                        assertEquals(35, rs.getInt("age"));
                        assertEquals("Black", str(getBytes(rs.getBlob("data"))));
                        break;

                    case 3:
                        assertEquals("p3", rs.getString("_key"));
                        assertEquals("Mike", rs.getString("firstName"));
                        assertEquals("Green", rs.getString("lastName"));
                        assertEquals(40, rs.getInt("age"));
                        assertEquals("Green", str(getBytes(rs.getBlob("data"))));
                        break;

                    case 4:
                        assertEquals("p4", rs.getString("_key"));
                        assertEquals("Leah", rs.getString("firstName"));
                        assertEquals("Grey", rs.getString("lastName"));
                        assertEquals(22, rs.getInt("age"));
                        assertEquals("Grey", str(getBytes(rs.getBlob("data"))));
                        break;

                    default:
                        assert false : "Invalid ID: " + id;
                }
            }
        }

        grid(0).cache(DEFAULT_CACHE_NAME).clear();

        assertEquals(0, grid(0).cache(DEFAULT_CACHE_NAME).size(CachePeekMode.ALL));

        super.afterTest();

        if (stmt != null && !stmt.isClosed())
            stmt.close();

        if (prepStmt != null && !prepStmt.isClosed())
            prepStmt.close();

        conn.close();

        assertTrue(prepStmt.isClosed());
        assertTrue(stmt.isClosed());
        assertTrue(conn.isClosed());
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        int res = stmt.executeUpdate(SQL);

        assertEquals(3, res);
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testExecute() throws SQLException {
        boolean res = stmt.execute(SQL);

        assertEquals(false, res);
    }

    /**
     *
     */
    @Test
    public void testDuplicateKeys() {
        jcache(0).put("p2", new Person(2, "Joe", "Black", 35));

        Throwable reason = GridTestUtils.assertThrows(log, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                return stmt.execute(SQL);
            }
        }, SQLException.class, null);

        reason = reason.getCause();

        assertNotNull(reason);

        assertTrue(reason.getMessage().contains(
            "Failed to INSERT some keys because they are already in cache [keys=[p2]]"));

        assertEquals(3, jcache(0).withKeepBinary().getAll(new HashSet<>(Arrays.asList("p1", "p2", "p3"))).size());
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testBatch() throws SQLException {
        formBatch(1, 2);
        formBatch(3, 4);

        int[] res = prepStmt.executeBatch();

        assertTrue(Arrays.equals(new int[] {2, 2}, res));
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testSingleItemBatch() throws SQLException {
        formBatch(1, 2);

        int[] res = prepStmt.executeBatch();

        assertTrue(Arrays.equals(new int[] {2}, res));
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testSingleItemBatchError() throws SQLException {
        formBatch(1, 2);

        prepStmt.executeBatch();

        formBatch(1, 2); // Duplicate key

        BatchUpdateException reason = (BatchUpdateException)
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return prepStmt.executeBatch();
                }
            },
            BatchUpdateException.class,
            "Failed to INSERT some keys because they are already in cache");

        // Check update counts in the exception.
        assertTrue(F.isEmpty(reason.getUpdateCounts()));
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testErrorAmidstBatch() throws SQLException {
        formBatch(1, 2);
        formBatch(3, 1); // Duplicate key

        BatchUpdateException reason = (BatchUpdateException)
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return prepStmt.executeBatch();
                }
            },
            BatchUpdateException.class,
            "Failed to INSERT some keys because they are already in cache");

        // Check update counts in the exception.
        int[] counts = reason.getUpdateCounts();

        assertNotNull(counts);

        assertEquals(1, counts.length);
        assertEquals(2, counts[0]);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearBatch() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws SQLException {
                return prepStmt.executeBatch();
            }
        }, SQLException.class, "Batch is empty");

        formBatch(1, 2);

        prepStmt.clearBatch();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws SQLException {
                return prepStmt.executeBatch();
            }
        }, SQLException.class, "Batch is empty");
    }

    /**
     * Form batch on prepared statement.
     *
     * @param id1 id for first row.
     * @param id2 id for second row.
     * @throws SQLException if failed.
     */
    private void formBatch(int id1, int id2) throws SQLException {
        int[] ids = new int[] { id1, id2 };

        int arg = 0;
        for (int id: ids) {
            String key = "p" + id;

            switch (id) {
                case 1:
                    prepStmt.setString(arg + 1, key);
                    prepStmt.setInt(arg + 2, 1);
                    prepStmt.setString(arg + 3, "John");
                    prepStmt.setString(arg + 4, "White");
                    prepStmt.setInt(arg + 5, 25);
                    prepStmt.setBytes(arg + 6, getBytes("White"));

                    break;

                case 2:
                    prepStmt.setString(arg + 1, key);
                    prepStmt.setInt(arg + 2, 2);
                    prepStmt.setString(arg + 3, "Joe");
                    prepStmt.setString(arg + 4, "Black");
                    prepStmt.setInt(arg + 5, 35);
                    prepStmt.setBytes(arg + 6, getBytes("Black"));

                    break;

                case 3:
                    prepStmt.setString(arg + 1, key);
                    prepStmt.setInt(arg + 2, 3);
                    prepStmt.setString(arg + 3, "Mike");
                    prepStmt.setString(arg + 4, "Green");
                    prepStmt.setInt(arg + 5, 40);
                    prepStmt.setBytes(arg + 6, getBytes("Green"));

                    break;

                case 4:
                    prepStmt.setString(arg + 1, key);
                    prepStmt.setInt(arg + 2, 4);
                    prepStmt.setString(arg + 3, "Leah");
                    prepStmt.setString(arg + 4, "Grey");
                    prepStmt.setInt(arg + 5, 22);
                    prepStmt.setBytes(arg + 6, getBytes("Grey"));

                    break;

                default:
                    assert false;
            }

            arg += 6;
        }

        prepStmt.addBatch();
    }
}
