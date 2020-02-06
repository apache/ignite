/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class JdbcQueryTest extends GridCommonAbstractTest {
    /** URL. */
    private final String url = "jdbc:ignite:thin://127.0.0.1?useExperimentalQueryEngine=true";

    /** Nodes count. */
    private final int nodesCnt = 3;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testSimpleQuery() throws SQLException {
        stmt.execute("CREATE TABLE Person(\"id\" INT, PRIMARY KEY(\"id\"), \"name\" VARCHAR)");

        doSleep(1000);

        stmt.executeUpdate("INSERT INTO Person VALUES (10, 'Name')");
        try (ResultSet rs = stmt.executeQuery("select p.*, (1+1) as synthetic from Person p")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals("Name", rs.getString(2));
            assertEquals(2, rs.getInt(3));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(nodesCnt);
        conn = DriverManager.getConnection(url);
        conn.setSchema("PUBLIC");
        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();
    }
}
