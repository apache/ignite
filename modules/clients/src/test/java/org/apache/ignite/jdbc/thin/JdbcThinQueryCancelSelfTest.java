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
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Statement test.
 */
public class JdbcThinQueryCancelSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** Max table rows. */
    private static final int MAX_ROWS = 100;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration<Integer, Integer> cc = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cc.setCopyOnRead(true);
        cc.setIndexedTypes(Integer.class, Integer.class);
        cc.setSqlFunctionClasses(TestSQLFunctions.class);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        for (int i = 0; i < MAX_ROWS; ++i)
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

        conn.setSchema(DEFAULT_CACHE_NAME);

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
     * @throws Exception If failed.
     */
    public void testCancelQuery() throws Exception {
        Statement stmt2 = conn.createStatement();

        try {
            stmt2.setFetchSize(1);

            // Open the second cursor
            ResultSet rs = stmt2.executeQuery("SELECT * from Integer");

            assert rs.next();

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

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // Execute long running query
                    stmt.executeQuery("select * from (select _val, sleep(500) as s from Integer limit 50)");

                    return null;
                }
            }, IgniteCheckedException.class, "The query was cancelled while executing");

            assert rs.next() : "The other cursor mustn't be closed";
        } finally {
            stmt2.close();
        }
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /**
         * Sleep function to simulate long running queries.
         *
         * @param x Time to sleep.
         * @return Return specified argument.
         */
        @QuerySqlFunction
        public static long sleep(long x) {
            if (x >= 0)
                try {
                    Thread.sleep(x);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

            return x;
        }
    }
}