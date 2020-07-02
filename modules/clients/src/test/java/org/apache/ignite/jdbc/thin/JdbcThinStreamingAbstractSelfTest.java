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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for streaming via thin driver.
 */
public abstract class JdbcThinStreamingAbstractSelfTest extends JdbcStreamingSelfTest {
    /** */
    protected int batchSize = 17;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        GridQueryProcessor.idxCls = IndexingWithContext.class;

        super.beforeTestsStarted();

        batchSize = 17;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Init IndexingWithContext.cliCtx
        try (Connection c = createOrdinaryConnection()) {
            execute(c, "SELECT 1");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try (Connection c = createOrdinaryConnection()) {
            execute(c, "DROP TABLE PUBLIC.T IF EXISTS");
        }

        IndexingWithContext.cliCtx = null;

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected Connection createStreamedConnection(boolean allowOverwrite, long flushFreq) throws Exception {
        Connection c = connect(grid(0), null);

        execute(c, "SET STREAMING 1 BATCH_SIZE " + batchSize + " ALLOW_OVERWRITE " + (allowOverwrite ? 1 : 0) +
            " PER_NODE_BUFFER_SIZE 1000 FLUSH_FREQUENCY " + flushFreq);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected Connection createOrdinaryConnection() throws SQLException {
        return connect(grid(0), null);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStreamedBatchedInsert() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            put(i, nameForId(i * 100));

        try (Connection conn = createStreamedConnection(false)) {
            assertStreamingState(true);

            try (PreparedStatement stmt = conn.prepareStatement("insert into Person(\"id\", \"name\") values (?, ?), " +
                "(?, ?)")) {
                for (int i = 1; i <= 100; i += 2) {
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
     * @throws Exception if failed.
     */
    @Test
    public void testStreamedBatchedInsertFunctionSuppliedValues() throws Exception {
        doStreamedInsertFunctionSuppliedValues(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStreamedInsertFunctionSuppliedValues() throws Exception {
        doStreamedInsertFunctionSuppliedValues(false);
    }

    /**
     * Inserts data using built-in function for column value.
     *
     * @param batch Batch mode flag.
     * @throws Exception if failed.
     */
    private void doStreamedInsertFunctionSuppliedValues(boolean batch) throws Exception {
        try (Connection conn = createStreamedConnection(false)) {
            assertStreamingState(true);

            try (PreparedStatement stmt = conn.prepareStatement(
                "insert into Person(\"id\", \"name\") values (?, RANDOM_UUID())")) {
                for (int i = 1; i <= 10; i++) {
                    stmt.setInt(1, i);

                    if (batch)
                        stmt.addBatch();
                    else
                        stmt.execute();
                }

                if (batch)
                    stmt.executeBatch();
            }
        }

        U.sleep(500);

        for (int i = 1; i <= 10; i++)
            UUID.fromString(nameForIdInCache(i));
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testSimultaneousStreaming() throws Exception {
        try (Connection anotherConn = createOrdinaryConnection()) {
            execute(anotherConn, "CREATE TABLE PUBLIC.T(x int primary key, y int) WITH " +
                "\"cache_name=T,wrap_value=false\"");
        }

        // Timeout to let connection close be handled on server side.
        U.sleep(500);

        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingState(true);

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

                final HashMap<String, IgniteDataStreamer<?, ?>> streamers = U.field(cliCtx, "streamers");

                // Wait when node process requests (because client send batch requests async).
                GridTestUtils.waitForCondition(() -> streamers.size() == 2, 1000);

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

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11795")
    @Test
    @Override public void testStreamedInsertFailsOnReadOnlyMode() throws Exception {
        super.testStreamedInsertFailsOnReadOnlyMode();
    }

    /**
     *
     */
    @Test
    public void testStreamingWithMixedStatementTypes() throws Exception {
        String prepStmtStr = "insert into Person(\"id\", \"name\") values (?, ?)";

        String stmtStr = "insert into Person(\"id\", \"name\") values (%d, '%s')";

        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingState(true);

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
     * @throws SQLException if failed.
     */
    @Test
    public void testStreamingOffToOn() throws Exception {
        try (Connection conn = createOrdinaryConnection()) {
            assertStreamingState(false);

            execute(conn, "SET STREAMING 1");

            assertStreamingState(true);
        }
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testStreamingOffToOff() throws Exception {
        try (Connection conn = createOrdinaryConnection()) {
            assertStreamingState(false);

            execute(conn, "SET STREAMING 0");

            assertStreamingState(false);
        }
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
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
    @Test
    public void testFlush() throws Exception {
        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingState(true);

            try (PreparedStatement stmt = conn.prepareStatement("insert into Person(\"id\", \"name\") values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, nameForId(i));

                    stmt.executeUpdate();
                }
            }

            assertCacheEmpty();

            execute(conn, "set streaming 0");

            assertStreamingState(false);

            U.sleep(500);

            // Now let's check it's all there.
            for (int i = 1; i <= 100; i++)
                assertEquals(nameForId(i), nameForIdInCache(i));
        }
    }

    /**
     * Ensure custom object can be serialized in streaming mode
     *      - start grid
     *      - create table such one of the columns was user's object
     *      - enable streaming and fill the table
     *      - disable streaming and query random row such it should be presented in the table
     *      - verify returned object
     *
     * @throws Exception
     */
    @Test
    public void testCustomObject() throws Exception {
        try (Connection conn = createOrdinaryConnection()) {
            execute(conn, "CREATE TABLE t2(id INT PRIMARY KEY, val OTHER)");
        }

        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingState(true);

            int testInd = 1 + new Random().nextInt(1000);

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t2 values (?, ?)")) {
                for (int i = 1; i <= 1000; i++) {
                    stmt.setInt(1, i);
                    stmt.setObject(2, i == testInd ? new Foo(testInd) : null);

                    stmt.executeUpdate();
                }
            }

            assertCacheEmpty();

            execute(conn, "set streaming 0");

            assertStreamingState(false);

            U.sleep(500);

            try (PreparedStatement stmt = conn.prepareStatement("SELECT val FROM t2 WHERE id = ?")) {
                stmt.setInt(1, testInd);

                ResultSet rs = stmt.executeQuery();

                Assert.assertTrue("Result should not be empty", rs.next());

                Foo foo = rs.getObject(1, Foo.class);

                Assert.assertEquals("Stored value not equals the expected one", testInd, foo.val);
            }
        }
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testStreamingReEnabled() throws Exception {
        try (Connection conn = createStreamedConnection(false, 10000)) {
            assertStreamingState(true);

            try (PreparedStatement stmt = conn.prepareStatement("insert into Person(\"id\", \"name\") values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, nameForId(i));

                    stmt.executeUpdate();
                }
            }

            assertCacheEmpty();

            execute(conn, "set streaming 1 batch_size 111 allow_overwrite 0 per_node_buffer_size 512 " +
                "per_node_parallel_operations 4 flush_frequency 5000");

            U.sleep(500);

            assertEquals((Integer)111, U.field((Object)U.field(conn, "streamState"), "streamBatchSize"));

            SqlClientContext cliCtx = sqlClientContext();

            assertTrue(cliCtx.isStream());

            assertFalse(U.field(cliCtx, "streamAllowOverwrite"));

            assertEquals((Integer)512, U.field(cliCtx, "streamNodeBufSize"));

            assertEquals((Long)5000L, U.field(cliCtx, "streamFlushTimeout"));

            assertEquals((Integer)4, U.field(cliCtx, "streamNodeParOps"));

            // Now let's check it's all there - SET STREAMING 1 repeated call must also have caused flush.
            for (int i = 1; i <= 100; i++)
                assertEquals(nameForId(i), nameForIdInCache(i));
        }
    }

    /**
     *
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testNonStreamedBatch() {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Connection conn = createOrdinaryConnection()) {
                    try (Statement s = conn.createStatement()) {
                        for (int i = 1; i <= 10; i++)
                            s.addBatch(String.format("insert into Person(\"id\", \"name\")values (%d, '%s')", i,
                                nameForId(i)));

                        execute(conn, "SET STREAMING 1");

                        s.addBatch(String.format("insert into Person(\"id\", \"name\")values (%d, '%s')", 11,
                            nameForId(11)));
                    }
                }

                return null;
            }
        }, SQLException.class, "Statement has non-empty batch (call executeBatch() or clearBatch() before " +
            "enabling streaming).");
    }

    /**
     *
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testStreamingStatementInTheMiddleOfNonPreparedBatch() {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Connection conn = createOrdinaryConnection()) {
                    try (Statement s = conn.createStatement()) {
                        s.addBatch(String.format("insert into Person(\"id\", \"name\")values (%d, '%s')", 1,
                            nameForId(1)));

                        s.addBatch("SET STREAMING 1 FLUSH_FREQUENCY 10000");
                    }
                }

                return null;
            }
        }, SQLException.class, "Streaming control commands must be executed explicitly");
    }

    /**
     *
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testBatchingSetStreamingStatement() {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Connection conn = createOrdinaryConnection()) {
                    try (PreparedStatement s = conn.prepareStatement("SET STREAMING 1 FLUSH_FREQUENCY 10000")) {
                        s.addBatch();
                    }
                }

                return null;
            }
        }, SQLException.class, "Streaming control commands must be executed explicitly");
    }

    /**
     * Check that there's nothing in cache.
     */
    protected void assertCacheEmpty() {
        assertEquals(0, cache().size(CachePeekMode.ALL));
    }

    /**
     * @param conn Connection.
     * @param sql Statement.
     * @throws SQLException if failed.
     */
    protected static void execute(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /**
     * @return Active SQL client context.
     */
    private SqlClientContext sqlClientContext() {
        assertNotNull(IndexingWithContext.cliCtx);

        return IndexingWithContext.cliCtx;
    }

    /**
     * Check that streaming state on target node is as expected.
     *
     * @param on Expected streaming state.
     */
    protected void assertStreamingState(boolean on) throws Exception {
        SqlClientContext cliCtx = sqlClientContext();

        GridTestUtils.waitForCondition(() -> cliCtx.isStream() == on, 1000);

        assertEquals(on, cliCtx.isStream());
    }

    /** {@inheritDoc} */
    @Override protected void assertStatementForbidden(String sql) {
        batchSize = 1;

        super.assertStatementForbidden(sql);
    }

    /**
     *
     */
    static final class IndexingWithContext extends IgniteH2Indexing {
        /** Client context. */
        static SqlClientContext cliCtx;

        /** {@inheritDoc} */
        @Override public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
            SqlClientContext cliCtx) throws IgniteCheckedException {
            IndexingWithContext.cliCtx = cliCtx;

            return super.streamBatchedUpdateQuery(schemaName, qry, params, cliCtx);
        }

        /** {@inheritDoc} */
        @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(
            String schemaName,
            SqlFieldsQuery qry,
            @Nullable SqlClientContext cliCtx,
            boolean keepBinary,
            boolean failOnMultipleStmts,
            GridQueryCancel cancel
        ) {
            IndexingWithContext.cliCtx = cliCtx;

            return super.querySqlFields(
                schemaName,
                qry,
                cliCtx,
                keepBinary,
                failOnMultipleStmts,
                cancel
            );
        }
    }

    /**
     * Dummy class to use as custom object field.
     */
    static class Foo {
        /** */
        int val;

        /** */
        public Foo(int val) {
            this.val = val;
        }
    }
}
