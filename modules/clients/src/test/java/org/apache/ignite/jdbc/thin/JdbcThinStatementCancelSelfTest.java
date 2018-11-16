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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * Statement cancel test.
 */
public class JdbcThinStatementCancelSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** Max table rows. */
    private static final int MAX_ROWS = 100;

    /** Subdirectory with CSV files */
    private static final String CSV_FILE_SUBDIR = "/modules/clients/src/test/resources/";

    /** A CSV file with one record. */
    private static final String BULKLOAD_20_000_LINE_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload20_000.csv")).getAbsolutePath();

    /** Default table name. */
    private static final String TBL_NAME = "Person";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setSqlFunctionClasses(JdbcThinStatementTimeoutSelfTest.class);
        cache.setIndexedTypes(Integer.class, Integer.class, Long.class, Long.class, String.class,
            JdbcThinAbstractDmlStatementSelfTest.Person.class);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);

        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);
        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put((long)i, (long)i);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

        conn.setSchema('"' + DEFAULT_CACHE_NAME + '"');

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

    /**
     *
     */
    public void testExpectSQLExceptionOnCancelingStmtWithoutQuery() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.cancel();

                return null;
            }
        }, SQLException.class, "There is no request to cancel.");


        stmt.close();
    }

    /**
     *
     */
    public void testExpectSQLExceptionOnCancelingStmtAgainstClosedStmt() throws Exception {
        stmt.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.cancel();

                return null;
            }
        }, SQLException.class, "Statement is closed.");
    }

    /**
     *
     */
    public void testExpectSQLExceptionOnResultSetNextAfterCancelingStmt() throws Exception {
        stmt.setFetchSize(10);

        ResultSet rs = stmt.executeQuery("SELECT * from Integer");

        rs.next();

        stmt.cancel();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                rs.next();

                return null;
            }
        }, SQLException.class, "The query was cancelled while executing.");

        stmt.close();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testExpectSQLExceptionAndAFAPControlRetrievalAfterCancellationLongRunningQuery() throws Exception {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(1000);

                    stmt.cancel();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);

                    fail("Unexpected exception");
                }
            }
        });

        IgniteInternalFuture<Object> res = GridTestUtils.runAsync(() -> {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Execute long running query
                    stmt.executeQuery("select sleep_func(3)");

                    return null;
                }
            }, SQLException.class, "The query was cancelled while executing.");
        });

        //ensure that the client receives the control before the initial request is executed
        res.get(2, TimeUnit.SECONDS);

        stmt.close();
    }

    /**
     *
     */
    public void testExpectQueryAfterCancellationPreviousQueryWorksFine() throws Exception {
        stmt.setFetchSize(10);

        ResultSet rs = stmt.executeQuery("SELECT * from Integer");

        rs.next();

        stmt.cancel();

        ResultSet rs2 = stmt.executeQuery("SELECT * from Integer order by _val");

        rs2.next();

        assertEquals(0, rs2.getInt(1));

        stmt.close();
    }

    /**
     *
     */
    public void testExpectResultSet1CancellationDoesNotEffectResultSet2() throws Exception {
        Statement anotherStmt = conn.createStatement();

        ResultSet rs1 = stmt.executeQuery("SELECT * FROM Integer WHERE _key % 2 = 0");

        ResultSet rs2 = anotherStmt.executeQuery("SELECT * FROM Integer  WHERE _key % 2 <> 0");

        stmt.cancel();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                rs1.next();

                return null;
            }
        }, SQLException.class, "The query was cancelled while executing.");

        rs2.next();
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public void testExpectSQLExceptionAndAFAPControlRetrievalAfterCancelingLongRunningQueryBasedOnJoins() throws Exception {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(1000);

                    stmt.cancel();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);

                    fail("Unexpected exception");
                }
            }
        });

        IgniteInternalFuture<Object> res = GridTestUtils.runAsync(() -> {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Execute long running query
                    stmt.executeQuery("SELECT * FROM Integer I1 JOIN Integer I2 JOIN Integer I3 JOIN" +
                        " Integer I4 JOIN Integer I5");

                    return null;
                }
            }, SQLException.class, "The query was cancelled while executing.");
        });

        //ensure that the client receives the control before the initial request is executed
        res.get(2, TimeUnit.SECONDS);

        stmt.close();
    }

    /**
     *
     */
    public void testExpectSQLExceptionAndAFAPControlRetrievalAfterCancelingLongRunningFileUpload() throws Exception {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(200);

                    stmt.cancel();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);

                    fail("Unexpected exception");
                }
            }
        });

        IgniteInternalFuture<Object> res = GridTestUtils.runAsync(() -> {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeUpdate(
                        "copy from '" + BULKLOAD_20_000_LINE_CSV_FILE + "' into " + TBL_NAME +
                            " (_key, age, firstName, lastName)" +
                            " format csv");

                    return null;
                }
            }, SQLException.class, "The query was cancelled while executing.");
        });

        stmt.close();
    }

    @SuppressWarnings("unused")
    @QuerySqlFunction
    public static int sleep_func(int v) {
        try {
            Thread.sleep(v * 1000);
        }
        catch (InterruptedException ignored) {
            // No-op
        }
        return v;
    }
}