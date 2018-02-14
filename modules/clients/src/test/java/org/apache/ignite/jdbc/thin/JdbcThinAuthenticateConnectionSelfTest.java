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
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests for authenticated an non authenticated connection.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinAuthenticateConnectionSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setAuthenticationEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(2);

        AuthorizationContext.context(grid(0).context().authentication().authenticate("ignite", "ignite"));

        grid(0).context().authentication().addUser("another_user", "passwd");

        AuthorizationContext.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnection() throws Exception {
        checkConnection("jdbc:ignite:thin://127.0.0.1", "ignite", "ignite");
        checkConnection("jdbc:ignite:thin://127.0.0.1", "another_user", "passwd");
        checkConnection("jdbc:ignite:thin://127.0.0.1?user=ignite&password=ignite", null, null);
        checkConnection("jdbc:ignite:thin://127.0.0.1?user=another_user&password=passwd", null, null);
    }

    /**
     */
    public void testInvalidUserPassword() {
        String err = "Unauthenticated sessions are prohibited";
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", null, null, err);

        err = "The user name or password is incorrect";
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "ignite", null, err);
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "another_user", null, err);
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "ignite", "", err);
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "ignite", "password", err);
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "another_user", "ignite", err);
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "another_user", "password", err);
        checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "another_user", "password", err);
    }

    /**
     * @throws SQLException On failed.
     */
    public void testUserSqlOnAuthorized() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1", "ignite", "ignite")) {
            conn.createStatement().execute("CREATE USER test WITH PASSWORD 'test'");

            checkConnection("jdbc:ignite:thin://127.0.0.1", "TEST", "test");

            conn.createStatement().execute("ALTER USER test WITH PASSWORD 'newpasswd'");

            checkConnection("jdbc:ignite:thin://127.0.0.1", "TEST", "newpasswd");

            conn.createStatement().execute("DROP USER test");

            checkInvalidUserPassword("jdbc:ignite:thin://127.0.0.1", "TEST", "newpasswd",
                "The user name or password is incorrect");
        }
    }

    /**
     * @throws SQLException On error.
     */
    public void testUserSqlWithNotIgniteUser() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1", "another_user", "passwd")) {
            String err = "User management operations are not allowed for user";

            checkUnauthorizedOperation(conn, "CREATE USER test WITH PASSWORD 'test'", err);
            checkUnauthorizedOperation(conn, "ALTER USER test WITH PASSWORD 'newpasswd'", err);
            checkUnauthorizedOperation(conn, "DROP USER test", err);
            checkUnauthorizedOperation(conn, "DROP USER \"another_user\"", err);

            conn.createStatement().execute("ALTER USER \"another_user\" WITH PASSWORD 'newpasswd'");

            checkConnection("jdbc:ignite:thin://127.0.0.1", "another_user", "newpasswd");
        }
    }

    /**
     * @param url Connection URL.
     * @param user User name.
     * @param passwd User password.
     * @throws SQLException On failed.
     */
    private void checkConnection(String url, String user, String passwd) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, passwd)) {
            conn.createStatement().execute("SELECT 1");
        }
    }

    /**
     * @param url Connection URL.
     * @param user User name.
     * @param passwd User pasword.
     * @param err Error message pattern.
     */
    private void checkInvalidUserPassword(final String url, final String user, final String passwd, String err) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                checkConnection(url, user, passwd);

                return null;
            }
        }, SQLException.class, err);
    }

    /**
     * @param conn JDBC connection.
     * @param sql SQL query.
     * @param err Error message pattern.
     */
    private void checkUnauthorizedOperation(final Connection conn, final String sql, String err) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                conn.createStatement().execute(sql);

                return null;
            }
        }, SQLException.class, err);
    }
}