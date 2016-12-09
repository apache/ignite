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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Statement test.
 */
public class JdbcInsertStatementSelfTest extends JdbcAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL = "insert into Person(_key, id, firstName, lastName, age) values " +
        "('p1', 1, 'John', 'White', 25), " +
        "('p2', 2, 'Joe', 'Black', 35), " +
        "('p3', 3, 'Mike', 'Green', 40)";

    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age) values " +
        "(?, ?, ?, ?, ?), (?, ?, ?, ?, ?)";

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
    public void testExecuteUpdate() throws SQLException {
        int res = stmt.executeUpdate(SQL);

        assertEquals(3, res);
    }

    /**
     * @throws SQLException If failed.
     */
    public void testExecute() throws SQLException {
        boolean res = stmt.execute(SQL);

        assertEquals(false, res);
    }

    /**
     *
     */
    public void testDuplicateKeys() {
        jcache(0).put("p2", new Person(2, "Joe", "Black", 35));

        Throwable reason = GridTestUtils.assertThrows(log, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                return stmt.execute(SQL);
            }
        }, SQLException.class, null);

        assertNotNull(reason.getCause());

        reason = reason.getCause().getCause();

        assertNotNull(reason);

        assertEquals(IgniteException.class, reason.getClass());

        assertEquals("Failed to INSERT some keys because they are already in cache [keys=[p2]]", reason.getMessage());

        assertEquals(3, jcache(0).withKeepBinary().getAll(new HashSet<>(Arrays.asList("p1", "p2", "p3"))).size());
    }
}
