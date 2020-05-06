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
import java.util.UUID;
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
import static org.apache.ignite.jdbc.JdbcAuthorizationTest.StatementProvider.of;
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

    /** Name of the table for DML operations testing. */
    private static final String TEST_DML_TABLE = Integer.class.getSimpleName();

    /** Name of the cache for the CREATE TABLE query security permissions testing. */
    private static final String TEST_CREATE_TABLE_CACHE = "test-create-table-cache";

    /** Name of the cache for the DROP TABLE query security permissions testing. */
    private static final String TEST_DROP_TABLE_CACHE = "test-drop-table-cache";

    /** Empty permissions set. */
    private static final SecurityPermissionSet EMPTY_PERMS = new SecurityBasicPermissionSet();

    /** JDBC URL prefix. */
    private static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite srv = startSecurityGrid(0,
            holderOf(EMPTY_PERMS),
            holderOf(systemPermissions(CACHE_CREATE)),
            holderOf(systemPermissions(CACHE_DESTROY)),
            holderOf(cachePermissions(TEST_CREATE_TABLE_CACHE, CACHE_CREATE)),
            holderOf(cachePermissions(TEST_DROP_TABLE_CACHE, CACHE_DESTROY)),
            holderOf(cachePermissions(DEFAULT_CACHE_NAME, CACHE_READ)),
            holderOf(cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT)),
            holderOf(cachePermissions(DEFAULT_CACHE_NAME, CACHE_REMOVE)),
            holderOf(cachePermissions(DEFAULT_CACHE_NAME, CACHE_DESTROY)));

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
        int key = UUID.randomUUID().hashCode();

        checkPermissionsRequired(
            of("INSERT INTO " + TEST_DML_SCHEMA + '.' + TEST_DML_TABLE + "(_key, _val) VALUES (" + key + ", 0);"),
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));
    }

    /**
     * Tests BULK INSERT query permissions check.
     */
    @Test
    public void testBulkInsert() throws Exception {
        checkPermissionsRequired(
            conn -> {
                PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO " + TEST_DML_SCHEMA + '.' + TEST_DML_TABLE + "(_key, _val) VALUES (?, ?);");

                for (int i = 1; i < 10; i++) {
                    stmt.setInt(1, UUID.randomUUID().hashCode());
                    stmt.setInt(2, 0);
                }

                return stmt;
            },
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));
    }

    /**
     * Tests SELECT query permissions check.
     */
    @Test
    public void testSelect() throws Exception {
        int key = insertKey();

        checkPermissionsRequired(
            of("SELECT _val FROM " + TEST_DML_SCHEMA + '.' + TEST_DML_TABLE + " WHERE _key=" + key + ";"),
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_READ));
    }

    /**
     * Tests UPDATE query permissions check.
     */
    @Test
    public void testUpdate() throws Exception {
        int key = insertKey();

        checkPermissionsRequired(
            of("UPDATE " + TEST_DML_SCHEMA + '.' + TEST_DML_TABLE + " SET _val=1 WHERE _key=" + key + ';'),
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));
    }

    /**
     * Tests DELETE query permissions check.
     */
    @Test
    public void testDelete() throws Exception {
        int key = insertKey();

        checkPermissionsRequired(
            of("DELETE FROM " + TEST_DML_SCHEMA + '.' + TEST_DML_TABLE + " WHERE _key=" + key + ';'),
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_REMOVE));
    }

    /**
     * Tests MERGE query permissions check.
     */
    @Test
    public void testMerge() throws Exception {
        int key = insertKey();

        checkPermissionsRequired(
            of("MERGE INTO " + TEST_DML_SCHEMA + '.' + TEST_DML_TABLE + "(_key, _val) VALUES (" + key + ", 0);"),
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));
    }

    /**
     * Tests CREATE TABLE query permissions check.
     */
    @Test
    public void testCreateTable() throws Exception {
        checkPermissionsRequired(
            of("CREATE TABLE sys_perm_table_create(id LONG PRIMARY KEY, val varchar) WITH \"TEMPLATE=REPLICATED\";"),
            systemPermissions(CACHE_CREATE));

        checkPermissionsRequired(
            of("CREATE TABLE cache_perm_table_create(id LONG PRIMARY KEY, val varchar) WITH " +
                "\"TEMPLATE=REPLICATED, CACHE_NAME=" + TEST_CREATE_TABLE_CACHE + "\";"),
            cachePermissions(TEST_CREATE_TABLE_CACHE, CACHE_CREATE));
    }

    /**
     * Tests DROP TABLE query permissions check.
     */
    @Test
    public void testDropTable() throws Exception {
        String sysPermsTable = createTable("test_sys_perm_table_drop");

        checkPermissionsRequired(of("DROP TABLE " + sysPermsTable + ';'), systemPermissions(CACHE_DESTROY));

        String cachePermsTable = "test_cache_perm_table_drop";

        executeWithPermissions(
            of("CREATE TABLE " + cachePermsTable + "(id LONG PRIMARY KEY, str_col varchar, long_col LONG)" +
                " WITH \"TEMPLATE=REPLICATED, CACHE_NAME=" + TEST_DROP_TABLE_CACHE + "\";"),
            systemPermissions(CACHE_CREATE)
        );

        checkPermissionsRequired(
            of("DROP TABLE " + cachePermsTable + ';'),
            cachePermissions(TEST_DROP_TABLE_CACHE, CACHE_DESTROY));
    }

    /**
     * Tests ALTER TABLE query permissions check.
     */
    @Test
    public void testAlterTableAddColumn() throws Exception {
        String table = createTable("test_table_add_column");

        executeWithPermissions(of("ALTER TABLE " + table + " ADD test_add_column LONG;"), EMPTY_PERMS);
    }

    /**
     * Tests ALTER TABLE DROP COLUMN query permissions check.
     */
    @Test
    public void testAlterTableDropColumn() throws Exception {
        String table = createTable("test_table_drop_column");

        executeWithPermissions(of("ALTER TABLE " + table + " DROP COLUMN long_col;"), EMPTY_PERMS);
    }

    /**
     * Tests CREATE INDEX and DROP INDEX queries permissions check.
     */
    @Test
    public void testCreateAndDropIndex() throws Exception {
        String table = createTable("test_table_manage_index");

        executeWithPermissions(of("CREATE INDEX test_idx ON " + table + "(id ASC);"), EMPTY_PERMS);

        executeWithPermissions(of("DROP INDEX test_idx ON " + table + ';'), EMPTY_PERMS);
    }

    /**
     * Inserts random key into the test table.
     *
     * @return Inserted key.
     */
    private int insertKey() throws Exception {
        int key = UUID.randomUUID().hashCode();

        executeWithPermissions(
            of("INSERT INTO " + TEST_DML_SCHEMA + '.' + TEST_DML_TABLE + "(_key, _val) VALUES (" + key + ", 0);"),
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));

        return key;
    }

    /**
     * Creates table with the specified name.
     *
     * @param name Name of the table to be created.
     * @return Name of created table.
     */
    private String createTable(String name) throws Exception {
        executeWithPermissions(
            of("CREATE TABLE " + name + "(id LONG PRIMARY KEY, str_col varchar, long_col LONG)" +
                " WITH \"TEMPLATE=REPLICATED\";"),
            systemPermissions(CACHE_CREATE)
        );

        return name;
    }

    /**
     * Checks that execution of specified sql statement requires specified security permissions be granted.
     *
     * @param stmt Provider of the SQL statement.
     * @param perms Minimum bunch of permissions for successful authorization.
     */
    private void checkPermissionsRequired(StatementProvider stmt, SecurityPermissionSet perms) throws Exception {
        assertAuthorizationFailed(stmt, EMPTY_PERMS);

        executeWithPermissions(stmt, perms);
    }

    /**
     * Asserts that authorization of SQL query on behalf of the user with the specified permissions fails.
     *
     * @param stmt Provider of the SQL statement.
     * @param perms Executive user permissions.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertAuthorizationFailed(StatementProvider stmt, SecurityPermissionSet perms) {
        assertThrowsAnyCause(
            log,
            () -> {
                executeWithPermissions(stmt, perms);

                return null;
            },
            SQLException.class,
            "Authorization failed");
    }

    /**
     * Executes an SQL statement on behalf of a user with the specified security permissions.
     *
     * @param stmt Provider of the SQL statement.
     * @param perms Required permissions.
     */
    private void executeWithPermissions(StatementProvider stmt, SecurityPermissionSet perms) throws Exception {
        try (Connection conn = getConnection(JDBC_URL_PREFIX + Config.SERVER, toLogin(perms), "")) {
            stmt.statement(conn).execute();
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
     * @return User security data with specified permissons granted.
     */
    private TestSecurityData holderOf(SecurityPermissionSet perms) {
        return new TestSecurityData(toLogin(perms), perms);
    }

    /**
     * @return Login of the user with specified permissions granted.
     */
    private String toLogin(SecurityPermissionSet perms) {
        return String.valueOf(perms.hashCode());
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

    /**
     * Provider of SQL statement.
     */
    static interface StatementProvider {
        /**
         * @param conn Connection to the cluster.
         * @return SQL statement.
         */
        public PreparedStatement statement(Connection conn) throws Exception;

        /**
         * @param sql SQL query.
         * @return Provider of statement for the specified SQL query.
         */
        public static StatementProvider of(String sql) {
            return conn -> conn.prepareStatement(sql);
        }
    }
}
