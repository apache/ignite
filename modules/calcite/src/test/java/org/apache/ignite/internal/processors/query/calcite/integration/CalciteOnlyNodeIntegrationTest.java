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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.processors.query.calcite.GridCommonAbstractWrapperTest;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Checks behaviour of a node that has the Calcite engine on the classpath but <b>not</b> the H2 engine
 * ({@code ignite-indexing}).
 * This is the default node layout after the H2 module is moved to {@code libs/optional} (IGNITE-29117).
 */
@SuppressWarnings("deprecation")
public class CalciteOnlyNodeIntegrationTest extends GridCommonAbstractWrapperTest {
    /** */
    private static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1";

    /**
     * Fragment that must be present in the error message of the query types that are implemented by the H2 engine only
     * ({@link SqlQuery}, {@link TextQuery}), so that a user knows which module to add.
     */
    private static final String H2_ONLY_FEATURE_MSG = "ignite-indexing";

    /** Error message of the H2-only SQL commands ({@code SET STREAMING}, {@code COPY}): Calcite cannot parse them. */
    private static final String PARSE_ERR_MSG = "Failed to parse query";

    /** {@link NodeTask} operation: is the H2 engine visible to the node. */
    private static final String OP_H2_IN_CLASSPATH = "h2InClassPath";

    /** {@link NodeTask} operation: {@code SELECT QUERY_ENGINE()} via the cache API. */
    private static final String OP_QUERY_ENGINE = "queryEngine";

    /** {@link NodeTask} operation: deprecated {@link SqlQuery} via the cache API, returns the matching keys. */
    private static final String OP_SQL_QUERY = "sqlQuery";

    /** {@link NodeTask} operation: {@link TextQuery} via the cache API, returns the matching keys. */
    private static final String OP_TEXT_QUERY = "textQuery";

    /** Prefix of a {@link NodeTask} result when the operation has failed; the messages of the cause chain follow. */
    private static final String ERR_PREFIX = "error: ";

    /** {@inheritDoc} */
    @BeforeAll
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startCalciteOnlyNode();

        assertEquals("H2 engine must not be visible to the Calcite-only node", "false", nodeOp(OP_H2_IN_CLASSPATH));

        try (IgniteClient cli = client()) {
            ClientCache<Integer, String> cache = cli.cache(DEFAULT_CACHE_NAME);

            cache.put(1, "v1");
            cache.put(2, "v2");
        }
    }

    /** {@inheritDoc} */
    @AfterAll
    @Override protected void afterTestsStopped() throws Exception {
        IgniteProcessProxy.killAll();

        super.afterTestsStopped();
    }

    /**
     * Starts a node in a separate JVM with {@code ignite-indexing}, H2 and Lucene removed from the classpath.
     * No engine is configured explicitly: the node must pick the only available one.
     */
    private void startCalciteOnlyNode() throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration("calcite-only"))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(1)))
            .setCacheConfiguration(
                new CacheConfiguration<Integer, String>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, String.class));

        CountDownLatch started = new CountDownLatch(1);

        ListeningTestLogger lsnrLog = new ListeningTestLogger(log);

        lsnrLog.registerListener(new CallbackExecutorLogListener(".*Topology snapshot \\[ver=1,.*", started::countDown));

        new IgniteProcessProxy(cfg, lsnrLog, null, false) {
            @Override protected Collection<String> filteredJvmArgs() throws Exception {
                Collection<String> args = super.filteredJvmArgs();

                args.add("-cp");
                args.add(Stream.of(System.getProperty("java.class.path"), System.getProperty("surefire.test.class.path"))
                    .filter(Objects::nonNull)
                    .flatMap(s -> Arrays.stream(s.split(File.pathSeparator)))
                    .filter(e -> !isH2Entry(e))
                    .collect(Collectors.joining(File.pathSeparator)));

                return args;
            }
        };

        assertTrue("Calcite-only node has not started", started.await(getTestTimeout(), TimeUnit.MILLISECONDS));
    }

    /** Baseline: {@link SqlFieldsQuery}, DDL and DML work on a Calcite-only node, Calcite is the default engine. */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        assertEquals(CalciteQueryEngineConfiguration.ENGINE_NAME, nodeOp(OP_QUERY_ENGINE));

        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE t(id INT PRIMARY KEY, val VARCHAR) WITH \"template=replicated\"");
            stmt.executeUpdate("INSERT INTO t VALUES (1, 'a')");

            try (ResultSet rs = stmt.executeQuery("SELECT val FROM t WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
            }

            stmt.executeUpdate("DROP TABLE t");
        }
    }

    /** */
    @Test
    public void testSqlQueryNotSupported() throws Exception {
        String msg = nodeOp(OP_SQL_QUERY);

        assertTrue(msg, msg.startsWith(ERR_PREFIX) && msg.contains(H2_ONLY_FEATURE_MSG));
    }

    /** Deprecated {@link SqlQuery} sent by the Java thin client fails on the server node with the same error. */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testThinClientSqlQuery() {
        try (IgniteClient cli = client()) {
            assertThrows(
                log,
                () -> cli.cache(DEFAULT_CACHE_NAME).query(new SqlQuery<Integer, String>(String.class, "_val = ?").setArgs("v2")).getAll(),
                ClientException.class,
                H2_ONLY_FEATURE_MSG
            );
        }
    }

    /** {@link TextQuery} is implemented by the H2 engine only: the error must name the missing module. */
    @Test
    public void testTextQuery() throws Exception {
        String res = nodeOp(OP_TEXT_QUERY);

        assertTrue(res, res.startsWith(ERR_PREFIX) && res.contains(H2_ONLY_FEATURE_MSG));
    }

    /**
     * {@code SET STREAMING} is implemented by the H2 engine only and is rejected by the Calcite parser. A rejected
     * {@code SET STREAMING ON} must not leave the thin JDBC connection in a state where {@code close()} hangs.
     */
    @Test
    public void testSetStreaming() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);

        try (Statement stmt = conn.createStatement()) {
            assertThrowsAnyCause(
                log,
                () -> stmt.executeUpdate("SET STREAMING ON"),
                SQLException.class,
                PARSE_ERR_MSG
            );
        }
        finally {
            try {
                GridTestUtils.runAsync(conn::close).get(10_000);
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                fail("Connection close hangs after a rejected SET STREAMING ON");
            }
        }
    }

    /** {@code COPY} (bulk load) is implemented by the H2 engine only and is rejected by the Calcite parser. */
    @Test
    public void testCopy() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            assertThrowsAnyCause(
                log,
                () -> stmt.executeUpdate("COPY FROM '/nonexistent.csv' INTO \"test\".String(_key, _val) FORMAT CSV"),
                SQLException.class,
                PARSE_ERR_MSG
            );
        }
    }

    /** The only available engine can be selected by name via the JDBC property without an explicit configuration. */
    @Test
    public void testJdbcQueryEngineProperty() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL + "?queryEngine=" + CalciteQueryEngineConfiguration.ENGINE_NAME);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT QUERY_ENGINE()")) {
            assertTrue(rs.next());
            assertEquals(CalciteQueryEngineConfiguration.ENGINE_NAME, rs.getString(1));
        }
    }

    /** The only available engine can be selected by the query hint without an explicit configuration. */
    @Test
    public void testQueryEngineHint() throws Exception {
        String qry = "SELECT /*+ QUERY_ENGINE('" + CalciteQueryEngineConfiguration.ENGINE_NAME + "') */ QUERY_ENGINE()";

        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(qry)) {
            assertTrue(rs.next());
            assertEquals(CalciteQueryEngineConfiguration.ENGINE_NAME, rs.getString(1));
        }
    }

    /** @return {@code True} if the classpath entry belongs to the H2 engine. */
    private static boolean isH2Entry(String entry) {
        String name = new File(entry).getName();

        return name.startsWith("ignite-indexing")
            || name.startsWith("h2-")
            || name.startsWith("lucene-")
            || entry.replace('\\', '/').contains("/modules/indexing/target/");
    }

    /** */
    private static IgniteClient client() {
        return Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER));
    }

    /**
     * Runs a cache API operation inside the Calcite-only node.
     *
     * @param op One of the {@code OP_*} constants.
     * @return Operation result, or {@link #ERR_PREFIX} followed by the error messages if the operation has failed.
     */
    private static String nodeOp(String op) throws Exception {
        try (IgniteClient cli = client()) {
            return cli.compute().execute(NodeTask.class.getName(), op);
        }
    }

    /** Runs a cache API operation inside the Calcite-only node, see {@link #nodeOp(String)}. */
    public static class NodeTask extends ComputeTaskSplitAdapter<String, String> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String op) {
            return Collections.singleton(new NodeJob(op));
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) {
            return results.get(0).getData();
        }
    }

    /** */
    private static class NodeJob extends ComputeJobAdapter {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final String op;

        /** */
        private NodeJob(String op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            try {
                IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

                return switch (op) {
                    case OP_H2_IN_CLASSPATH ->
                        String.valueOf(IgniteComponentType.INDEXING.inClassPath());

                    case OP_QUERY_ENGINE ->
                        String.valueOf(cache.query(new SqlFieldsQuery("SELECT QUERY_ENGINE()")).getAll().get(0).get(0));

                    case OP_SQL_QUERY ->
                        cache.query(new SqlQuery<Integer, String>(String.class, "_val = ?").setArgs("v1")).getAll();

                    case OP_TEXT_QUERY ->
                        cache.query(new TextQuery<Integer, String>(String.class, "v1")).getAll();

                    default ->
                        throw new IllegalArgumentException("Unknown operation: " + op);
                };
            }
            catch (Throwable e) {
                StringBuilder sb = new StringBuilder(ERR_PREFIX);

                for (Throwable t = e; t != null; t = t.getCause())
                    sb.append(t.getMessage()).append(t.getCause() == null ? "" : " <- ");

                return sb.toString();
            }
        }
    }
}
