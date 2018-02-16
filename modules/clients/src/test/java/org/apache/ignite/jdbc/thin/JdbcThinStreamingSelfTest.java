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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Tests for streaming via thin driver.
 */
public class JdbcThinStreamingSelfTest extends JdbcStreamingSelfTest {
    /** */
    private int batchSize = 17;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        batchSize = 17;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try (Connection c = createOrdinaryConnection()) {
            execute(c, "DROP TABLE PUBLIC.T IF EXISTS");
        }

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected Connection createStreamedConnection(boolean allowOverwrite, long flushFreq) throws Exception {
        return JdbcThinAbstractSelfTest.connect(grid(0), "streaming=true&streamingFlushFrequency="
            + flushFreq + "&" + "streamingAllowOverwrite=" + allowOverwrite + "&streamingPerNodeBufferSize=1000&"
            + "streamingBatchSize=" + batchSize);
    }

    /** {@inheritDoc} */
    @Override protected Connection createOrdinaryConnection() throws SQLException {
        return JdbcThinAbstractSelfTest.connect(grid(0), null);
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedBatchedInsert() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            put(i, nameForId(i * 100));

        try (Connection conn = createStreamedConnection(false)) {
            assertStreamingOn();

            try (PreparedStatement stmt = conn.prepareStatement("insert into Person(\"id\", \"name\") values (?, ?), " +
                "(?, ?)")) {
                for (int i = 1; i <= 100; i+=2) {
                    stmt.setInt(1, i);
                    stmt.setString(2, nameForId(i));
                    stmt.setInt(3, i + 1);
                    stmt.setString(4, nameForId(i + 1));

                    stmt.addBatch();
                }

                stmt.executeBatch();
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        for (int i = 1; i <= 100; i++) {
            if (i % 10 != 0)
                assertEquals(nameForId(i), nameForIdInCache(i));
            else // All that divides by 10 evenly should point to numbers 100 times greater - see above
                assertEquals(nameForId(i * 100), nameForIdInCache(i));
        }
    }

    /**
     * @throws SQLException if failed.
     */
    public void testSimultaneousStreaming() throws Exception {
        try (Connection anotherConn = createOrdinaryConnection()) {
            execute(anotherConn, "CREATE TABLE PUBLIC.T(x int primary key, y int) WITH " +
                "\"cache_name=T,wrap_value=false\"");
        }

        // Timeout to let connection close be handled on server side.
        U.sleep(500);

        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingOn();

            PreparedStatement firstStmt = conn.prepareStatement("insert into Person(\"id\", \"name\") values (?, ?)");

            PreparedStatement secondStmt = conn.prepareStatement("insert into PUBLIC.T(x, y) values (?, ?)");

            try {
                for (int i = 1; i <= 10; i++) {
                    firstStmt.setInt(1, i);
                    firstStmt.setString(2, nameForId(i));

                    firstStmt.executeUpdate();
                }

                for (int i = 51; i <= 67; i++) {
                    secondStmt.setInt(1, i);
                    secondStmt.setInt(2, i);

                    secondStmt.executeUpdate();
                }

                for (int i = 11; i <= 50; i++) {
                    firstStmt.setInt(1, i);
                    firstStmt.setString(2, nameForId(i));

                    firstStmt.executeUpdate();
                }

                for (int i = 68; i <= 100; i++) {
                    secondStmt.setInt(1, i);
                    secondStmt.setInt(2, i);

                    secondStmt.executeUpdate();
                }

                assertCacheEmpty();

                SqlClientContext cliCtx = sqlClientContext();

                HashMap<String, IgniteDataStreamer<?, ?>> streamers = U.field(cliCtx, "streamers");

                assertEquals(2, streamers.size());

                assertEqualsCollections(new HashSet<>(Arrays.asList("person", "T")), streamers.keySet());
            }
            finally {
                U.closeQuiet(firstStmt);

                U.closeQuiet(secondStmt);
            }
        }

        // Let's wait a little so that all data arrives to destination - we can't intercept streamers' flush
        // on connection close in any way.
        U.sleep(1000);

        // Now let's check it's all there.
        for (int i = 1; i <= 50; i++)
            assertEquals(nameForId(i), nameForIdInCache(i));

        for (int i = 51; i <= 100; i++)
            assertEquals(i, grid(0).cache("T").get(i));
    }

    /**
     *
     */
    public void testStreamingWithMixedStatementTypes() throws Exception {
        String prepStmtStr = "insert into Person(\"id\", \"name\") values (?, ?)";

        String stmtStr = "insert into Person(\"id\", \"name\") values (%d, '%s')";

        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingOn();

            PreparedStatement firstStmt = conn.prepareStatement(prepStmtStr);

            Statement secondStmt = conn.createStatement();

            try {
                for (int i = 1; i <= 100; i++) {
                    boolean usePrep = Math.random() > 0.5;

                    boolean useBatch = Math.random() > 0.5;

                    if (usePrep) {
                        firstStmt.setInt(1, i);
                        firstStmt.setString(2, nameForId(i));

                        if (useBatch)
                            firstStmt.addBatch();
                        else
                            firstStmt.execute();
                    }
                    else {
                        String sql = String.format(stmtStr, i, nameForId(i));

                        if (useBatch)
                            secondStmt.addBatch(sql);
                        else
                            secondStmt.execute(sql);
                    }
                }
            }
            finally {
                U.closeQuiet(firstStmt);

                U.closeQuiet(secondStmt);
            }
        }

        // Let's wait a little so that all data arrives to destination - we can't intercept streamers' flush
        // on connection close in any way.
        U.sleep(1000);

        // Now let's check it's all there.
        for (int i = 1; i <= 100; i++)
            assertEquals(nameForId(i), nameForIdInCache(i));
    }

    /**
     * Check that there's nothing in cache.
     */
    private void assertCacheEmpty() {
        assertEquals(0, grid(0).cache(DEFAULT_CACHE_NAME).size(CachePeekMode.ALL));
    }

    /**
     * @param conn Connection.
     * @param sql Statement.
     * @throws SQLException if failed.
     */
    private static void execute(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /**
     * @return Active SQL client context.
     */
    private SqlClientContext sqlClientContext() {
        Set<SqlClientContext> ctxs = U.field(grid(0).context().query(), "cliCtxs");

        assertFalse(F.isEmpty(ctxs));

        assertEquals(1, ctxs.size());

        return ctxs.iterator().next();
    }

    /**
     * Check that streaming state on target node is as expected.
     */
    private void assertStreamingOn() {
        SqlClientContext cliCtx = sqlClientContext();

        assertTrue(cliCtx.isStream());
    }

    /** {@inheritDoc} */
    @Override protected void assertStatementForbidden(String sql) {
        batchSize = 1;

        super.assertStatementForbidden(sql);
    }
}