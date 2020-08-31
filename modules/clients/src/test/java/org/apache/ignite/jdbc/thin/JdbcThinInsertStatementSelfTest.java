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
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

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
    private final Object[][] args = new Object[][] {
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
    @Test
    public void testExecuteUpdate() throws SQLException {
        assertEquals(3, stmt.executeUpdate(SQL));
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testPreparedExecuteUpdate() throws SQLException {
        assertEquals(3, prepStmt.executeUpdate());
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testExecute() throws SQLException {
        assertFalse(stmt.execute(SQL));
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testPreparedExecute() throws SQLException {
        assertFalse(prepStmt.execute());
    }

    /**
     *
     */
    @Test
    public void testDuplicateKeys() {
        jcache(0).put("p2", new Person(2, "Joe", "Black", 35));

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                return stmt.execute(SQL);
            }
        }, SQLException.class,
            "Failed to INSERT some keys because they are already in cache [keys=[p2]]");

        assertEquals(3, jcache(0).withKeepBinary().getAll(new HashSet<>(Arrays.asList("p1", "p2", "p3"))).size());
    }
}
