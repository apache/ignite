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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinAbstractSelfTest extends AbstractIndexingCommonTest {
    /** Signals that tests should start in Partition Awareness mode. */
    public static boolean partitionAwareness;

    /**
     * @param r Runnable to check support.
     */
    protected void checkNotSupported(final RunnableX r) {
        GridTestUtils.assertThrowsWithCause(r, SQLFeatureNotSupportedException.class);
    }

    /**
     * @param r Runnable to check on closed connection.
     */
    protected void checkConnectionClosed(final RunnableX r) {
        GridTestUtils.assertThrowsAnyCause(log,
            () -> {
                r.run();

                return null;
            }, SQLException.class, "Connection is closed");
    }

    /**
     * @param r Runnable to check on closed statement.
     */
    protected void checkStatementClosed(final RunnableX r) {
        GridTestUtils.assertThrowsAnyCause(log,
            () -> {
                r.run();

                return null;
            }, SQLException.class, "Statement is closed");
    }

    /**
     * @param r Runnable to check on closed result set.
     */
    protected void checkResultSetClosed(final RunnableX r) {
        GridTestUtils.assertThrowsAnyCause(log,
            () -> {
                r.run();

                return null;
            }, SQLException.class, "Result set is closed");
    }

    /**
     * @param node Node to connect to.
     * @param params Connection parameters.
     * @return Thin JDBC connection to specified node.
     */
    protected Connection connect(IgniteEx node, String params) throws SQLException {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class) {
                cliLsnrRec = rec;

                break;
            }
        }

        assertNotNull(cliLsnrRec);

        String connStr = "jdbc:ignite:thin://127.0.0.1:" + cliLsnrRec.port();

        if (!F.isEmpty(params))
            connStr += "/?" + params;

        return DriverManager.getConnection(connStr);
    }

    /**
     * @param sql Statement.
     * @param args Arguments.
     * @return Result set.
     * @throws RuntimeException if failed.
     */
    protected List<List<?>> execute(Connection conn, String sql, Object... args) throws SQLException {
        try (PreparedStatement s = conn.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++)
                s.setObject(i + 1, args[i]);

            if (s.execute()) {
                List<List<?>> res = new ArrayList<>();

                try (ResultSet rs = s.getResultSet()) {
                    ResultSetMetaData meta = rs.getMetaData();

                    int cnt = meta.getColumnCount();

                    while (rs.next()) {
                        List<Object> row = new ArrayList<>(cnt);

                        for (int i = 1; i <= cnt; i++)
                            row.add(rs.getObject(i));

                        res.add(row);
                    }
                }

                return res;
            }
            else
                return Collections.emptyList();
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean keepSerializedObjects() {
        return true;
    }
}
