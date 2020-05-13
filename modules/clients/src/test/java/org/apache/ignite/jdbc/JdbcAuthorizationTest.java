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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.Config;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityBasicPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.junit.Test;

import static java.sql.DriverManager.getConnection;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Tests permissions required for execution of the basic SQL requests through JDBC client.
 */
public class JdbcAuthorizationTest extends AbstractSecurityTest {
    /** Name of the schema for DML operations testing. */
    private static final String TEST_DML_SCHEMA = "test_schema";

    /** JDBC URL prefix. */
    private static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

    /** Name of the table for DML operations testing. */
    private static final String TEST_DML_TABLE = TEST_DML_SCHEMA + '.' + Integer.class.getSimpleName();

    /** Name of the table for bulk-load operation testing. */
    private static final String TEST_BULKLOAD_CACHE = "test-bulkload-cache";

    /** Name of the cache for the CREATE TABLE query security permissions testing. */
    private static final String TEST_CREATE_TABLE_CACHE = "test-create-table-cache";

    /** Name of the cache for the DROP TABLE query security permissions testing. */
    private static final String TEST_DROP_TABLE_CACHE = "test-drop-table-cache";

    /** Name of the user with the {@link SecurityPermission#CACHE_CREATE} system permission granted. */
    private static final String CACHE_CREATE_SYS_PERM_USER = "cache-create-sys-perm-user";

    /** Name of the user with the {@link SecurityPermission#CACHE_DESTROY} system permission granted. */
    private static final String CACHE_DESTROY_SYS_PERMS_USER = "cache-destroy-sys-perm-user";

    /** Name of the user with {@link SecurityPermission#CACHE_CREATE} granted for {@link #TEST_CREATE_TABLE_CACHE}.*/
    private static final String CACHE_CREATE_CACHE_PERMS_USER = "cache-create-cache-perm-user";

    /** Name of the user with {@link SecurityPermission#CACHE_DESTROY} granted for {@link #TEST_DROP_TABLE_CACHE}. */
    private static final String CACHE_DESTROY_CACHE_PERMS_USER = "cache-destroy-cache-perm-user";

    /** Name of the user with the {@link SecurityPermission#CACHE_READ} granted for the default cache. */
    private static final String CACHE_READ_USER = "cache-read-user";

    /** Name of the user with the {@link SecurityPermission#CACHE_PUT} permission granted for the default cache. */
    private static final String CACHE_PUT_USER = "cache-put-user";

    /** Name of the user with the {@link SecurityPermission#CACHE_REMOVE} permission granted for the default cache. */
    private static final String CACHE_REMOVE_USER = "cache-remove-user";

    /** Name of the user with no permission granted. */
    private static final String EMPTY_PERMS_USER = "empty-perms-user";

    /** CSV file for bulk-load testing. */
    private static final String BULKLOAD_CSV_FILE = Objects.requireNonNull(resolveIgnitePath(
        "/modules/clients/src/test/resources/bulkload2.csv")).getAbsolutePath();

    /** Counter that is used to obtain unique keys. */
    private static final AtomicInteger KEY_CNTR = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite srv = startSecurityGrid(0,
            new TestSecurityData(EMPTY_PERMS_USER, new SecurityBasicPermissionSet()),
            new TestSecurityData(CACHE_CREATE_SYS_PERM_USER, systemPermissions(CACHE_CREATE)),
            new TestSecurityData(CACHE_DESTROY_SYS_PERMS_USER, systemPermissions(CACHE_DESTROY)),
            new TestSecurityData(CACHE_CREATE_CACHE_PERMS_USER, cachePermissions(TEST_CREATE_TABLE_CACHE, CACHE_CREATE)),
            new TestSecurityData(CACHE_DESTROY_CACHE_PERMS_USER, cachePermissions(TEST_DROP_TABLE_CACHE, CACHE_DESTROY)),
            new TestSecurityData(CACHE_READ_USER, cachePermissions(DEFAULT_CACHE_NAME, CACHE_READ)),
            new TestSecurityData(CACHE_PUT_USER, create().defaultAllowAll(false)
                .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT)
                .appendCachePermissions(TEST_BULKLOAD_CACHE, CACHE_PUT)
                .build()),
            new TestSecurityData(CACHE_REMOVE_USER, cachePermissions(DEFAULT_CACHE_NAME, CACHE_REMOVE)));

        startSecurityGrid(1);

        srv.cluster().state(ACTIVE);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, Integer.class);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setSqlSchema(TEST_DML_SCHEMA);

        srv.createCache(ccfg);
    }

    /**
     * Tests INSERT query permissions check.
     */
    @Test
    public void testInsert() throws Exception {
        int key = KEY_CNTR.getAndIncrement();

        String sql = "INSERT INTO " + TEST_DML_TABLE + "(_key, _val) VALUES (" + key + ", 0);";

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);
        assertAuthorizationFailed(sql, CACHE_READ_USER);
        assertAuthorizationFailed(sql, CACHE_REMOVE_USER);

        execute(sql, CACHE_PUT_USER);
    }

    /**
     * Tests COPY FROM query permissions check.
     */
    @Test
    public void testCopyFrom() throws Exception {
        String table = "test_table_copy_from";

        execute("CREATE TABLE " + table + "(id LONG PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, age LONG)" +
                " WITH \"TEMPLATE=REPLICATED, CACHE_NAME=" + TEST_BULKLOAD_CACHE + "\";",
            CACHE_CREATE_SYS_PERM_USER);

        String sql = "COPY FROM '" + BULKLOAD_CSV_FILE + "' INTO " + table +
            "(id, age, first_name, last_name) FORMAT csv";

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);
        assertAuthorizationFailed(sql, CACHE_READ_USER);
        assertAuthorizationFailed(sql, CACHE_REMOVE_USER);

        execute(sql, CACHE_PUT_USER);
    }

    /**
     * Tests SELECT query permissions check.
     */
    @Test
    public void testSelect() throws Exception {
        int key = insertKey();

        String sql = "SELECT _val FROM " + TEST_DML_TABLE + " WHERE _key=" + key + ";";

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);
        assertAuthorizationFailed(sql, CACHE_REMOVE_USER);
        assertAuthorizationFailed(sql, CACHE_PUT_USER);

        execute(sql, CACHE_READ_USER);
    }

    /**
     * Tests UPDATE query permissions check.
     */
    @Test
    public void testUpdate() throws Exception {
        int key = insertKey();

        String sql = "UPDATE " + TEST_DML_TABLE + " SET _val=1 WHERE _key=" + key + ';';

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);
        assertAuthorizationFailed(sql, CACHE_REMOVE_USER);
        assertAuthorizationFailed(sql, CACHE_READ_USER);

        execute(sql, CACHE_PUT_USER);
    }

    /**
     * Tests DELETE query permissions check.
     */
    @Test
    public void testDelete() throws Exception {
        int key = insertKey();

        String sql = "DELETE FROM " + TEST_DML_TABLE + " WHERE _key=" + key + ';';

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);
        assertAuthorizationFailed(sql, CACHE_PUT_USER);
        assertAuthorizationFailed(sql, CACHE_READ_USER);

        execute(sql, CACHE_REMOVE_USER);
    }

    /**
     * Tests MERGE query permissions check.
     */
    @Test
    public void testMerge() throws Exception {
        int key = insertKey();

        String sql = "MERGE INTO " + TEST_DML_TABLE + "(_key, _val) VALUES (" + key + ", 0);";

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);
        assertAuthorizationFailed(sql, CACHE_REMOVE_USER);
        assertAuthorizationFailed(sql, CACHE_READ_USER);

        execute(sql, CACHE_PUT_USER);
    }

    /**
     * Tests CREATE TABLE query system permissions check.
     */
    @Test
    public void testCreateTableSystemPermissions() throws Exception {
        String sql = "CREATE TABLE sys_perm_table_create(id LONG PRIMARY KEY, val VARCHAR) WITH \"TEMPLATE=REPLICATED\";";

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);

        execute(sql, CACHE_CREATE_SYS_PERM_USER);
    }

    /**
     * Tests CREATE TABLE query cache permissions check.
     */
    @Test
    public void testCreateTableCachePermissions() throws Exception {
        String sql = "CREATE TABLE cache_perm_table_create(id LONG PRIMARY KEY, val VARCHAR) WITH " +
            "\"TEMPLATE=REPLICATED, CACHE_NAME=" + TEST_CREATE_TABLE_CACHE + "\";";

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);

        execute(sql, CACHE_CREATE_CACHE_PERMS_USER);
    }

    /**
     * Tests DROP TABLE query system permissions check.
     */
    @Test
    public void testDropTableSystemPermissions() throws Exception {
        String table = "test_sys_perm_table_drop";

        execute("CREATE TABLE " + table + "(id LONG PRIMARY KEY, val VARCHAR)  WITH \"TEMPLATE=REPLICATED\";",
            CACHE_CREATE_SYS_PERM_USER);

        String sql = "DROP TABLE " + table + ';';

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);

        execute(sql, CACHE_DESTROY_SYS_PERMS_USER);
    }

    /**
     * Tests DROP TABLE query cache permissions check.
     */
    @Test
    public void testDropTableCachePermissions() throws Exception {
        String table = "test_cache_perm_table_drop";

        execute("CREATE TABLE " + table + "(id LONG PRIMARY KEY, val VARCHAR)" +
                " WITH \"TEMPLATE=REPLICATED, CACHE_NAME=" + TEST_DROP_TABLE_CACHE + "\";",
            CACHE_CREATE_SYS_PERM_USER);

        String sql = "DROP TABLE " + table + ';';

        assertAuthorizationFailed(sql, EMPTY_PERMS_USER);

        execute(sql, CACHE_DESTROY_CACHE_PERMS_USER);
    }

    /**
     * Inserts random key into the test table.
     *
     * @return Inserted key.
     */
    private int insertKey() throws Exception {
        int key = KEY_CNTR.getAndIncrement();

        execute("INSERT INTO " + TEST_DML_TABLE + "(_key, _val) VALUES (" + key + ", 0);", CACHE_PUT_USER);

        return key;
    }

    /**
     * Asserts that authorization of SQL query on behalf of the user with the specified login.
     *
     * @param sql SQL query to execute.
     * @param login Login of the user.
     */
    private void assertAuthorizationFailed(String sql, String login) {
        assertThrowsAnyCause(
            log,
            () -> {
                execute(sql, login);

                return null;
            },
            SQLException.class,
            "Authorization failed");
    }

    /**
     * Executes an SQL query on behalf of a user with the specified login.
     *
     * @param sql SQL query to execute.
     * @param login Login of the user.
     */
    private void execute(String sql, String login) throws Exception {
        try (Connection conn = getConnection(JDBC_URL_PREFIX + Config.SERVER, login, "")) {
            PreparedStatement stmt = conn.prepareStatement(sql);

            stmt.execute();
        }
    }

    /**
     * @param idx Index of the node.
     * @param clients Security data of the clients.
     */
    private IgniteEx startSecurityGrid(int idx, TestSecurityData... clients) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        return startGrid(getConfiguration(
            login,
            new TestSecurityPluginProvider(
                login,
                "",
                systemPermissions(CACHE_CREATE, JOIN_AS_SERVER),
                null,
                false,
                clients
            ))
        );
    }

    /**
     * @return {@link SecurityPermissionSet} containing specified system permissions.
     */
    private SecurityPermissionSet systemPermissions(SecurityPermission... perms) {
        return create().defaultAllowAll(false).appendSystemPermissions(perms).build();
    }

    /**
     * @return {@link SecurityPermissionSet} containing specified cache permissions.
     */
    private SecurityPermissionSet cachePermissions(String cache, SecurityPermission... perms) {
        return create().defaultAllowAll(false).appendCachePermissions(cache, perms).build();
    }
}
