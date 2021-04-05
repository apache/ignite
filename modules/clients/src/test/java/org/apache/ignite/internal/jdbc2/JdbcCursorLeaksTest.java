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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Test for cursors leak on JDBC v2.
 */
@RunWith(Parameterized.class)
public class JdbcCursorLeaksTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEYS = 100;

    /** */
    @Parameterized.Parameter
    public boolean remote;

    /** */
    @Parameterized.Parameter(1)
    public boolean multipleStatement;

    /** */
    @Parameterized.Parameter(2)
    public boolean distributedJoin;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "remote={0}, multipleStatement={1}, distributedJoin={2}")
    public static Collection parameters() {
        Set<Object[]> paramsSet = new LinkedHashSet<>();

        for (int i = 0; i < 8; ++i) {
            Object[] params = new Object[3];

            params[0] = (i & 1) != 0;
            params[1] = (i & 2) != 0;
            params[2] = (i & 4) != 0;

            paramsSet.add(params);
        }

        return paramsSet;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);

        sql("CREATE TABLE A(ID INT PRIMARY KEY, JID INT)");
        sql("CREATE TABLE B(ID INT PRIMARY KEY, JID INT)");

        for (int i = 0; i < KEYS; ++i) {
            sql("INSERT INTO A VALUES (?, ?)", i, i);
            sql("INSERT INTO B VALUES (?, ?)", i, i + 1);
        }
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testSingleQuery() throws Exception {
        checkQuery("SELECT 1");
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testMultipleStatement0() throws Exception {
        // Skip the test when multiple statement not allowed
        if (!multipleStatement)
            return;

        checkQuery("SELECT 1; SELECT 2");
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testMultipleStatement1() throws Exception {
        // Skip the test when multiple statement not allowed
        if (!multipleStatement)
            return;

        checkQuery("SELECT 1; SELECT 2; SELECT 3");
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testJoin() throws Exception {
        checkQuery("SELECT * FROM A JOIN B on A.JID=B.JID");
    }

    /**
     * @param sql Query string.
     * @throws Exception Orn error.
     */
    private void checkQuery(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(url())) {
            for (int i = 0; i < 10; i++) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sql);
                }
            }

            checkThereAreNoRunningQueries(1000);
        }
    }

    /**
     * @param timeout Timeout to finish running queries.
     */
    private void checkThereAreNoRunningQueries(int timeout) {
        for (Ignite ign : G.allGrids())
            checkThereAreNoRunningQueries((IgniteEx)ign, timeout);
    }

    /**
     * @param ign Noe to check running queries.
     * @param timeout Timeout to finish running queries.
     */
    private void checkThereAreNoRunningQueries(IgniteEx ign, int timeout) {
        long t0 = U.currentTimeMillis();

        while (true) {
            List<List<?>> res = ign.context().query().querySqlFields(
                new SqlFieldsQuery("SELECT * FROM SYS.SQL_QUERIES"), false).getAll();

            if (res.size() == 1)
                return;

            if (U.currentTimeMillis() - t0 > timeout)
                fail("Timeout. There are unexpected running queries [node=" + ign.name() + ", queries= " + res + ']');
        }
    }

    /**
     * @return JDBCv2 URL connection string.
     */
    private String url() {
        StringBuilder params = new StringBuilder();

        params.append("multipleStatementsAllowed=").append(multipleStatement);
        params.append(":");
        params.append("distributedJoin=").append(distributedJoin);
        params.append(":");

        if (remote)
            params.append("nodeId=").append(grid(0).cluster().localNode().id());

        return CFG_URL_PREFIX + params + "@modules/clients/src/test/config/jdbc-config.xml";
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }
}
