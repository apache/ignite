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
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
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

    /** Max cross joins. */
    private static final int MAX_CROSS_JOINS = 10;

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

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);

        try(Connection conn = DriverManager.getConnection(URL)) {
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE TEST (ID INT primary key, NAME VARCHAR(50))");

            for (int i = 0; i < MAX_ROWS; i++)
                stmt.execute("INSERT INTO TEST (ID, NAME) values (" + i + ", 'name_" + i + "')");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

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
        final StringBuilder sql = new StringBuilder("SELECT * from TEST as t0");

        for (int i = 1; i < MAX_CROSS_JOINS; i++)
            sql.append(", TEST as t" + i);

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
                stmt.executeQuery(sql.toString());

                return null;
            }
        }, IgniteCheckedException.class, "The query was cancelled while executing");
    }
}