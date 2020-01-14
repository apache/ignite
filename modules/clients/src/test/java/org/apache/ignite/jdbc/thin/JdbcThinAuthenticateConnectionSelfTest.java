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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for authenticated an non authenticated JDBC thin connection.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinAuthenticateConnectionSelfTest extends JdbcThinAbstractSelfTest {
    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        startGrids(2);

        grid(0).cluster().active(true);

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
    @Test
    public void testConnection() throws Exception {
        checkConnection(URL, "ignite", "ignite");
        checkConnection(URL, "another_user", "passwd");
        checkConnection(URL + "?user=ignite&password=ignite", null, null);
        checkConnection(URL + "?user=another_user&password=passwd", null, null);
    }

    /**
     */
    @Test
    public void testInvalidUserPassword() {
        String err = "Unauthenticated sessions are prohibited";
        checkInvalidUserPassword(URL, null, null, err);

        err = "The user name or password is incorrect";
        checkInvalidUserPassword(URL, "ignite", null, err);
        checkInvalidUserPassword(URL, "another_user", null, err);
        checkInvalidUserPassword(URL, "ignite", "", err);
        checkInvalidUserPassword(URL, "ignite", "password", err);
        checkInvalidUserPassword(URL, "another_user", "ignite", err);
        checkInvalidUserPassword(URL, "another_user", "password", err);
        checkInvalidUserPassword(URL, "another_user", "password", err);
    }

    /**
     * @throws SQLException On failed.
     */
    @Test
    public void testUserSqlOnAuthorized() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL, "ignite", "ignite")) {
            conn.createStatement().execute("CREATE USER test WITH PASSWORD 'test'");

            checkConnection(URL, "TEST", "test");

            conn.createStatement().execute("ALTER USER test WITH PASSWORD 'newpasswd'");

            checkConnection(URL, "TEST", "newpasswd");

            conn.createStatement().execute("DROP USER test");

            checkInvalidUserPassword(URL, "TEST", "newpasswd",
                "The user name or password is incorrect");
        }
    }

    /**
     * @throws SQLException On error.
     */
    @Test
    public void testUserSqlWithNotIgniteUser() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL, "another_user", "passwd")) {
            String err = "User management operations are not allowed for user";

            checkUnauthorizedOperation(conn, "CREATE USER test WITH PASSWORD 'test'", err);
            checkUnauthorizedOperation(conn, "ALTER USER test WITH PASSWORD 'newpasswd'", err);
            checkUnauthorizedOperation(conn, "DROP USER test", err);
            checkUnauthorizedOperation(conn, "DROP USER \"another_user\"", err);

            conn.createStatement().execute("ALTER USER \"another_user\" WITH PASSWORD 'newpasswd'");

            checkConnection(URL, "another_user", "newpasswd");
        }
    }

    /**
     * @throws SQLException On error.
     */
    @Test
    public void testQuotedUsername() throws SQLException {
        // Spaces
        checkUserPassword(" test", "    ");
        checkUserPassword(" test ", " test ");
        checkUserPassword("test ", " ");
        checkUserPassword(" ", " ");
        checkUserPassword("user", "&/:=?");
        checkUserPassword("user", "&");
        checkUserPassword("user", "/");
        checkUserPassword("user", ":");
        checkUserPassword("user", "=");
        checkUserPassword("user", "?");

        // Nationals "user / password" in Russian.
        checkUserPassword("\\u044E\\u0437\\u0435\\u0440", "\\u043F\\u0430\\u0440\\u043E\\u043B\\u044C");

        // Nationals "user / password" in Chinese.
        checkUserPassword("\\u7528\\u6236", "\\u5BC6\\u78BC");
    }

    /**
     * @param user User name.
     * @param passwd User's password.
     * @throws SQLException On error.
     */
    private void checkUserPassword(String user, String passwd) throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL, "ignite", "ignite")) {
            conn.createStatement().execute(String.format("CREATE USER \"%s\" WITH PASSWORD '%s'", user, passwd));

            checkConnection(URL, user, passwd);
            checkConnection(URL + String.format("?user={%s}&password={%s}", user, passwd), null, null);

            conn.createStatement().execute(String.format("DROP USER \"%s\"", user));
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
     * @param passwd User password.
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
