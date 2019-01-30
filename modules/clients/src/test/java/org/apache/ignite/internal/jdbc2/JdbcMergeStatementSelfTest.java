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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.apache.ignite.cache.CachePeekMode;
import org.junit.Test;

/**
 * MERGE statement test.
 */
public class JdbcMergeStatementSelfTest extends JdbcAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL = "merge into Person(_key, id, firstName, lastName, age, data) values " +
        "('p1', 1, 'John', 'White', 25, RAWTOHEX('White')), " +
        "('p2', 2, 'Joe', 'Black', 35, RAWTOHEX('Black')), " +
        "('p3', 3, 'Mike', 'Green', 40, RAWTOHEX('Green'))";

    /** SQL query. */
    protected static final String SQL_PREPARED = "merge into Person(_key, id, firstName, lastName, age, data) values " +
        "(?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)";

    /** Statement. */
    protected Statement stmt;

    /** Prepared statement. */
    protected PreparedStatement prepStmt;

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
     * @throws SQLException if failed.
     */
    @Test
    public void testBatch() throws SQLException {
        prepStmt.setString(1, "p1");
        prepStmt.setInt(2, 1);
        prepStmt.setString(3, "John");
        prepStmt.setString(4, "White");
        prepStmt.setInt(5, 25);
        prepStmt.setBytes(6, getBytes("White"));

        prepStmt.setString(7, "p2");
        prepStmt.setInt(8, 2);
        prepStmt.setString(9, "Joe");
        prepStmt.setString(10, "Black");
        prepStmt.setInt(11, 35);
        prepStmt.setBytes(12, getBytes("Black"));
        prepStmt.addBatch();

        prepStmt.setString(1, "p3");
        prepStmt.setInt(2, 3);
        prepStmt.setString(3, "Mike");
        prepStmt.setString(4, "Green");
        prepStmt.setInt(5, 40);
        prepStmt.setBytes(6, getBytes("Green"));

        prepStmt.setString(7, "p4");
        prepStmt.setInt(8, 4);
        prepStmt.setString(9, "Leah");
        prepStmt.setString(10, "Grey");
        prepStmt.setInt(11, 22);
        prepStmt.setBytes(12, getBytes("Grey"));

        prepStmt.addBatch();

        int[] res = prepStmt.executeBatch();

        assertTrue(Arrays.equals(new int[] {2, 2}, res));
    }
}
