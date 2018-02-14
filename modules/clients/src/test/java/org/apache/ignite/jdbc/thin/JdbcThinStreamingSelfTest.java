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
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try (Connection c = createOrdinaryConnection()) {
            execute(c, "DROP TABLE PUBLIC.T IF EXISTS");
        }

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected Connection createStreamedConnection(boolean allowOverwrite, long flushFreq) throws Exception {
        Connection res = JdbcThinAbstractSelfTest.connect(grid(0), "streaming=true&streamingFlushFrequency="
            + flushFreq + "&" + "streamingAllowOverwrite=" + allowOverwrite + "&streamingPerNodeBufferSize=1000&"
            + "streamingBatchSize=17");

        res.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        return res;
    }

    /**
     * @return Connection without streaming initially turned on.
     * @throws SQLException if failed.
     */
    private Connection createOrdinaryConnection() throws SQLException {
        return JdbcThinAbstractSelfTest.connect(grid(0), null);
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedBatchedInsert() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i * 100);

        try (Connection conn = createStreamedConnection(false)) {
            assertStreamingState(true);

            try (PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?), " +
                "(?, ?)")) {
                for (int i = 1; i <= 100; i+=2) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, i);
                    stmt.setInt(3, i + 1);
                    stmt.setInt(4, i + 1);

                    stmt.addBatch();
                }

                stmt.executeBatch();
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        for (int i = 1; i <= 100; i++) {
            if (i % 10 != 0)
                assertEquals(i, grid(0).cache(DEFAULT_CACHE_NAME).get(i));
            else // All that divides by 10 evenly should point to numbers 100 times greater - see above
                assertEquals(i * 100, grid(0).cache(DEFAULT_CACHE_NAME).get(i));
        }
    }

    /**
     * @throws SQLException if failed.
     */
    public void testStreamingOffToOn() throws SQLException {
        try (Connection conn = createOrdinaryConnection()) {
            assertStreamingState(false);

            execute(conn, "SET STREAMING 1");

            assertStreamingState(true);
        }
    }

    /**
     * @throws SQLException if failed.
     */
    public void testStreamingOnToOff() throws Exception {
        try (Connection conn = createStreamedConnection(false)) {
            assertStreamingState(true);

            execute(conn, "SET STREAMING off");

            assertStreamingState(false);
        }
    }

    /**
     * @throws SQLException if failed.
     */
    public void testFlush() throws Exception {
        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingState(true);

            try (PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, i);

                    stmt.executeUpdate();
                }
            }

            assertCacheEmpty();

            execute(conn, "flush streamer");

            // Now let's check it's all there.
            for (int i = 1; i <= 100; i++)
                assertEquals(i, grid(0).cache(DEFAULT_CACHE_NAME).get(i));

        }
    }

    /**
     * @throws SQLException if failed.
     */
    public void testFlushByDdl() throws Exception {
        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingState(true);

            try (PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, i);

                    stmt.executeUpdate();
                }
            }

            assertCacheEmpty();

            // DDL statement issued from a different connection must flush stream on first connection.
            try (Connection anotherConn = createOrdinaryConnection()) {
                execute(anotherConn, "CREATE TABLE PUBLIC.T(x int primary key, y int)");
            }

            // Now let's check it's all there.
            for (int i = 1; i <= 100; i++)
                assertEquals(i, grid(0).cache(DEFAULT_CACHE_NAME).get(i));

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
            assertStreamingState(true);

            PreparedStatement firstStmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?)");

            PreparedStatement secondStmt = conn.prepareStatement("insert into PUBLIC.T(x, y) values (?, ?)");

            try {
                for (int i = 1; i <= 50; i++) {
                    firstStmt.setInt(1, i);
                    firstStmt.setInt(2, i);

                    firstStmt.executeUpdate();
                }

                for (int i = 51; i <= 100; i++) {
                    secondStmt.setInt(1, i);
                    secondStmt.setInt(2, i);

                    secondStmt.executeUpdate();
                }

                assertCacheEmpty();

                SqlClientContext cliCtx = sqlClientContext();

                HashMap<String, IgniteDataStreamer<?, ?>> streamers = U.field(cliCtx, "streamers");

                assertEquals(2, streamers.size());

                assertEqualsCollections(new HashSet<>(Arrays.asList("default", "T")), streamers.keySet());
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
            assertEquals(i, grid(0).cache(DEFAULT_CACHE_NAME).get(i));

        for (int i = 51; i <= 100; i++)
            assertEquals(i, grid(0).cache("T").get(i));
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
     * @param on Expected streaming state.
     */
    private void assertStreamingState(boolean on) {
        SqlClientContext cliCtx = sqlClientContext();

        assertEquals(on, cliCtx.isStream());
    }
}