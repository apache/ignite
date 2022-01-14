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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Statement test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcInsertStatementSelfTest extends ItJdbcAbstractStatementSelfTest {
    /** SQL SELECT query for verification. */
    private static final String SQL_SELECT = "select sid, id, firstName, lastName, age from PUBLIC.PERSON";

    /** SQL query. */
    private static final String SQL = "insert into PUBLIC.PERSON(sid, id, firstName, lastName, age) values "
            + "('p1', 1, 'John', 'White', 25), "
            + "('p2', 2, 'Joe', 'Black', 35), "
            + "('p3', 3, 'Mike', 'Green', 40)";

    /** SQL query. */
    private static final String SQL_PREPARED = "insert into PUBLIC.PERSON(sid, id, firstName, lastName, age) values "
            + "(?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)";

    /** Arguments for prepared statement. */
    private final Object[][] args = new Object[][] {
            {"p1", 1, "John", "White", 25},
            {"p3", 3, "Mike", "Green", 40},
            {"p2", 2, "Joe", "Black", 35}
    };

    /** SQL query to populate cache. */
    private static final String DROP_SQL = "DELETE FROM PUBLIC.PERSON;";

    /** Prepared statement. */
    private PreparedStatement prepStmt;

    @BeforeEach
    @Override
    public void refillTable() throws Exception {
        stmt.execute(DROP_SQL);

        prepStmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(prepStmt);
        assertFalse(prepStmt.isClosed());

        int paramCnt = 1;

        for (Object[] arg : args) {
            prepStmt.setString(paramCnt++, (String) arg[0]);
            prepStmt.setInt(paramCnt++, (Integer) arg[1]);
            prepStmt.setString(paramCnt++, (String) arg[2]);
            prepStmt.setString(paramCnt++, (String) arg[3]);
            prepStmt.setInt(paramCnt++, (Integer) arg[4]);
        }
    }

    @AfterEach
    @Override public void afterTest() throws Exception {
        super.afterTest();

        if (prepStmt != null && !prepStmt.isClosed()) {
            prepStmt.close();

            assertTrue(prepStmt.isClosed());
        }
    }

    private void doCheck() throws Exception {
        assertTrue(stmt.execute(SQL_SELECT));

        ResultSet rs = stmt.getResultSet();

        assertNotNull(rs);

        while (rs.next()) {
            int id = rs.getInt("id");

            switch (id) {
                case 1:
                    assertEquals("p1", rs.getString("sid"));
                    assertEquals("John", rs.getString("firstName"));
                    assertEquals("White", rs.getString("lastName"));
                    assertEquals(25, rs.getInt("age"));
                    break;

                case 2:
                    assertEquals("p2", rs.getString("sid"));
                    assertEquals("Joe", rs.getString("firstName"));
                    assertEquals("Black", rs.getString("lastName"));
                    assertEquals(35, rs.getInt("age"));
                    break;

                case 3:
                    assertEquals("p3", rs.getString("sid"));
                    assertEquals("Mike", rs.getString("firstName"));
                    assertEquals("Green", rs.getString("lastName"));
                    assertEquals(40, rs.getInt("age"));
                    break;

                case 4:
                    assertEquals("p4", rs.getString("sid"));
                    assertEquals("Leah", rs.getString("firstName"));
                    assertEquals("Grey", rs.getString("lastName"));
                    assertEquals(22, rs.getInt("age"));
                    break;

                default:
                    fail("Invalid ID: " + id);
            }
        }
    }

    /**
     * Execute update test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteUpdate() throws Exception {
        assertEquals(3, stmt.executeUpdate(SQL));

        doCheck();
    }

    /**
     * Prepared update execute test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPreparedExecuteUpdate() throws Exception {
        assertEquals(3, prepStmt.executeUpdate());

        doCheck();
    }

    /**
     * Test execute.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecute() throws Exception {
        assertFalse(stmt.execute(SQL));

        doCheck();
    }

    /**
     * Test prepared execute.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPreparedExecute() throws Exception {
        assertFalse(prepStmt.execute());

        doCheck();
    }

    /**
     * Test duplicated keys.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDuplicateKeys() throws Exception {
        String sql = "insert into PUBLIC.PERSON(sid, id, firstName, lastName, age) values('p1', 1, 'John', 'White', 25)";

        assertFalse(stmt.execute(sql));

        assertThrows(SQLException.class, () -> stmt.execute(SQL), "Failed to INSERT some keys because they are already in cache.");

        stmt.execute("select count(*) from PUBLIC.PERSON;");

        ResultSet resultSet = stmt.getResultSet();

        assertTrue(resultSet.next());

        assertEquals(3, resultSet.getInt(1));

        doCheck();
    }
}
