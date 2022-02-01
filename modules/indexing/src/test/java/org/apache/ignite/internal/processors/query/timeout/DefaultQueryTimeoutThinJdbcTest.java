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

package org.apache.ignite.internal.processors.query.timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.jdbc.thin.JdbcThinStatement;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

/**
 *
 */
public class DefaultQueryTimeoutThinJdbcTest extends AbstractDefaultQueryTimeoutTest {
    /** {@inheritDoc} */
    @Override protected void executeQuery(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            conn.createStatement().executeQuery(sql);
        }
    }

    /** {@inheritDoc} */
    @Override protected void executeQuery(String sql, int timeout) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            JdbcThinStatement stmt = (JdbcThinStatement)conn.createStatement();

            stmt.timeout(timeout);

            stmt.executeQuery(sql);
        }
    }

    /**
     * Check cases when JDBC query timeout value (sets in second) more then Integer range in milliseconds.
     * Steps:
     * - start server node;
     * - execute queries with huge timeout in seconds: (timeout_value * 1000 more than Integer.MAX_VALUE);
     * - the query must be executed successful.
     */
    @Test
    public void testExplicitTimeoutMoreThenMaxValue() throws Exception {
        startGrid(0);

        setDefaultQueryTimeout(500);

        TimedQueryHelper helper = new TimedQueryHelper(1000, DEFAULT_CACHE_NAME);
        helper.createCache(grid(0));

        String sql = helper.buildTimedQuery();

        int[] hugeTimeouts = new int[] {Integer.MAX_VALUE / 1000 + 1, Integer.MAX_VALUE};

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            for (int timeout: hugeTimeouts) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(timeout);

                    ResultSet rs = stmt.executeQuery(sql);

                    int cnt = 0;

                    while (rs.next())
                        cnt++;

                    assertTrue(cnt > 0);
                }
            }
        }
    }


    /** {@inheritDoc} */
    @Override protected void assertQueryCancelled(Callable<?> c) {
        try {
            c.call();

            fail("Exception is expected");
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, "The query was cancelled while executing", SQLException.class));
        }
    }
}
