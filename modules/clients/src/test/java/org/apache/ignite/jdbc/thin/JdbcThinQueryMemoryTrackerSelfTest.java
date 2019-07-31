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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.oom.QueryMemoryTrackerSelfTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Query memory manager for local queries.
 */
public class JdbcThinQueryMemoryTrackerSelfTest extends QueryMemoryTrackerSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return false;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testGlobalQuota() throws Exception {
        maxMem = 8 * MB;

        final List<ResultSet> results = Collections.synchronizedList(new ArrayList<>());
        final List<Statement> statements = Collections.synchronizedList(new ArrayList<>());
        final List<IgniteInternalFuture> futs = new ArrayList<>();

        try {
            for (int i = 0; i < 10; i++) {
                futs.add(GridTestUtils.runAsync(() -> {
                        Connection conn = createConnection(true);
                        Statement stmt = conn.createStatement();

                        statements.add(stmt);

                        ResultSet rs = stmt.executeQuery("select * from T as T0, T as T1 where T0.id < 2 " +
                            "UNION select * from T as T2, T as T3 where T2.id >= 1 AND T2.id < 2");

                        results.add(rs);

                        rs.next();

                        return null;
                    }
                ));
            }

            SQLException ex = (SQLException)GridTestUtils.assertThrows(GridAbstractTest.log, () -> {
                SQLException sqlEx = null;

                for (IgniteInternalFuture f : futs) {
                    try {
                        f.get(5_000);
                    }
                    catch (IgniteCheckedException e) {
                        if (e.hasCause(SQLException.class))
                            sqlEx = e.getCause(SQLException.class);
                    }
                }

                if (sqlEx != null)
                    throw sqlEx;

                return null;
            }, SQLException.class, "SQL query run out of memory: Global quota exceeded.");

            assertEquals(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, ex.getErrorCode());
            assertEquals(IgniteQueryErrorCode.codeToSqlState(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY), ex.getSQLState());
        }
        finally {
            for (ResultSet rs : results)
                IgniteUtils.closeQuiet(rs);

            for (Statement stmt : statements) {
                IgniteUtils.closeQuiet(stmt.getConnection());
                IgniteUtils.closeQuiet(stmt);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> execQuery(String sql, boolean lazy) throws Exception {
        try (Connection conn = createConnection(lazy)) {
            try (Statement stmt = conn.createStatement()) {
                assert stmt != null && !stmt.isClosed();

                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next())
                        ;
                }
            }
        }
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override protected void checkQueryExpectOOM(String sql, boolean lazy) {
        SQLException ex = (SQLException)GridTestUtils.assertThrows(log, () -> {
            execQuery(sql, lazy);

            return null;
        }, SQLException.class, "SQL query run out of memory: Query quota exceeded.");

        assertEquals(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, ex.getErrorCode());
        assertEquals(IgniteQueryErrorCode.codeToSqlState(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY), ex.getSQLState());
    }

    /**
     * Initialize SQL connection.
     *
     * @param lazy Lazy flag.
     * @throws SQLException If failed.
     */
    protected Connection createConnection(boolean lazy) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800..10802?" +
            "queryMaxMemory=" + (maxMem) + "&lazy=" + lazy);

        conn.setSchema("\"PUBLIC\"");

        return conn;
    }
}