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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.Config;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityBasicPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Tests permissions required for execution of the basic SQL requests through JDBC client.
 */
public class JdbcAuthorizationTest extends AbstractSecurityTest {
    /** Name of the schema for DML operations testing. */
    private static final String TEST_SCHEMA = "test_schema";

    /** Name of the table for DML operations testing. */
    private static final String TABLE_NAME = Integer.class.getSimpleName();

    /** Empty permissions set. */
    private static final SecurityPermissionSet EMPTY_PERMS = new SecurityBasicPermissionSet();

    /** JDBC URL prefix. */
    public static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

    /** Index of the node that accepted the query request. */
    protected static final int LOCAL_NODE_IDX = 0;

    /** Index of the node which is remote to query executor. */
    protected static final int REMOTE_NODE_IDX = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite srv = startGrid(LOCAL_NODE_IDX);

        startGrid(REMOTE_NODE_IDX);

        srv.cluster().state(ACTIVE);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, Integer.class);
        ccfg.setSqlSchema(TEST_SCHEMA);

        srv.createCache(ccfg);
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        return startGrid(
            getTestIgniteInstanceName(idx),
            systemPermissions(CACHE_CREATE, JOIN_AS_SERVER),
            false);
    }

    /**
     * Tests INSERT query permissions check on local and remote node against the executor.
     */
    @Test
    public void testInsert() throws Exception {
        checkInsert(LOCAL_NODE_IDX);
        checkInsert(REMOTE_NODE_IDX);
    }

    /**
     * Tests SELECT query permissions check on the local and remote nodes against the executor.
     */
    @Test
    public void testSelect() throws Exception {
        checkSelect(LOCAL_NODE_IDX);
        checkSelect(REMOTE_NODE_IDX);
    }

    /**
     * Tests UPDATE query permissions check on the local and remote nodes against the executor.
     */
    @Test
    public void testUpdate() throws Exception {
        checkUpdate(LOCAL_NODE_IDX);
        checkUpdate(REMOTE_NODE_IDX);
    }

    /**
     * Tests DELETE query permissions check on the local and remote nodes against the executor.
     */
    @Test
    public void testDelete() throws Exception {
        checkDelete(LOCAL_NODE_IDX);
        checkDelete(REMOTE_NODE_IDX);
    }

    /**
     * Tests MERGE query permissions check on the local and remote nodes against the executor.
     */
    @Test
    public void testMerge() throws Exception {
        checkMerge(LOCAL_NODE_IDX);
        checkMerge(REMOTE_NODE_IDX);
    }

    /**
     * Tests CREATE TABLE query permissions check,
     */
    @Test
    public void testCreateTable() throws Exception {
        checkPermissionsRequired(
            "CREATE TABLE test_table_create(id LONG PRIMARY KEY, val varchar) WITH \"TEMPLATE=REPLICATED\";",
            systemPermissions(CACHE_CREATE));
    }

    /**
     * Tests DROP TABLE query permissions check,
     */
    @Test
    public void testDropTable() throws Exception {
        String table = createTable("test_table_drop");

        checkPermissionsRequired("DROP TABLE " + table + ';', systemPermissions(CACHE_DESTROY));
    }

    /**
     * Tests ALTER TABLE query permissions check.
     */
    @Test
    public void testAlterTableAddColumn() throws Exception {
        String table = createTable("test_table_add_column");

        executeWithPermissions("ALTER TABLE " + table + " ADD test_add_column LONG;", EMPTY_PERMS);
    }

    /**
     * Tests ALTER TABLE DROP COLUMN query permissions check.
     */
    @Test
    public void testAlterTableDropColumn() throws Exception {
        String table = createTable("test_table_drop_column");

        executeWithPermissions("ALTER TABLE " + table + " DROP COLUMN long_col;", EMPTY_PERMS);
    }

    /**
     * Tests CREATE INDEX and DROP INDEX queries permissions check.
     */
    @Test
    public void testCreateAndDropIndex() throws Exception {
        String table = createTable("test_table_manage_index");

        executeWithPermissions("CREATE INDEX test_idx ON " + table + "(id ASC);", EMPTY_PERMS);

        executeWithPermissions("DROP INDEX test_idx ON " + table + ';', EMPTY_PERMS);
    }

    /**
     * Checks the permissions required to execute INSERT SQL query.
     *
     * @param nodeIdx Index of the node on which the query will be executed.
     */
    private void checkInsert(int nodeIdx) throws Exception {
        int key = keyForNode(KeyType.INSERT_KEY, nodeIdx);

        checkPermissionsRequired(
            "INSERT INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0)",
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));
    }

    /**
     * Checks the permissions required to execute SELECT SQL statement.
     *
     * @param nodeIdx Index of the node on which the query will be executed.
     */
    private void checkSelect(int nodeIdx) throws Exception {
        int key = initializeKeyForNode(KeyType.SELECT_KEY, nodeIdx);

        checkPermissionsRequired(
            "SELECT _val FROM " + TEST_SCHEMA + '.' + TABLE_NAME + " WHERE _key=" + key + ';',
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_READ));
    }

    /**
     * Checks the permissions required to execute UPDATE SQL statement.
     *
     * @param nodeIdx Index of the node on which the query will be executed.
     */
    private void checkUpdate(int nodeIdx) throws Exception {
        int key = initializeKeyForNode(KeyType.UPDATE_KEY, nodeIdx);

        checkPermissionsRequired(
            "UPDATE " + TEST_SCHEMA + '.' + TABLE_NAME + " SET _val = 1 WHERE _key=" + key + ';',
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));
    }

    /**
     * Checks the permissions required to execute DELETE SQL statement.
     *
     * @param nodeIdx Index of the node on which the query will be executed.
     */
    private void checkDelete(int nodeIdx) throws Exception {
        int key = initializeKeyForNode(KeyType.DELETE_KEY, nodeIdx);

        checkPermissionsRequired(
            "DELETE FROM " + TEST_SCHEMA + '.' + TABLE_NAME + " WHERE _key=" + key + ';',
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_REMOVE));
    }

    /**
     * Checks the permissions required to execute MERGE SQL statement.
     *
     * @param nodeIdx Index of the node on which the query will be executed.
     */
    private void checkMerge(int nodeIdx) throws Exception {
        int key = initializeKeyForNode(KeyType.MERGE_KEY, nodeIdx);

        checkPermissionsRequired(
            "MERGE INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0);",
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));
    }

    /**
     * Creates table with the specified name.
     *
     * @param name Name of the table to be created.
     * @return Name of created table.
     */
    private String createTable(String name) throws Exception{
        executeWithPermissions(
            "CREATE TABLE " + name + "(id LONG PRIMARY KEY, str_col varchar, long_col LONG)" +
                " WITH \"TEMPLATE=REPLICATED\";",
            systemPermissions(CACHE_CREATE)
        );

        return name;
    }

    /**
     * Generates key which will belong to node with specified index and inserts it in the test table.
     *
     * @param keyType Type of key for dynamic generation.
     * @param nodeIdx Index of node to which key will belong.
     * @return Initialized key.
     */
    private int initializeKeyForNode(KeyType keyType, int nodeIdx) throws Exception {
        int key = keyForNode(keyType, nodeIdx);

        executeWithPermissions(
            "INSERT INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0)",
            cachePermissions(DEFAULT_CACHE_NAME, CACHE_PUT));

        return key;
    }

    /**
     * Generates key which belongs to specified node.
     *
     * @param keyType Type of the key to be generated.
     * @param nodeIdx Key storage node index.
     */
    private int keyForNode(KeyType keyType, int nodeIdx) {
        int res;

        int keyIdx = 0;

        AtomicInteger cnt = new AtomicInteger(0);

        do {
            res = keyForNode(
                grid(LOCAL_NODE_IDX).affinity(DEFAULT_CACHE_NAME),
                cnt,
                grid(nodeIdx).localNode());
        }
        while (keyIdx++ != keyType.ordinal());

        return res;
    }

    /**
     * Checks that execution of specified sql query requires that all of specified security permissions be granted.
     *
     * @param sql SQL query to execute.
     * @param requiredPerms Minimum bunch of permissions for successful authorization.
     */
    private void checkPermissionsRequired(String sql, SecurityPermissionSet requiredPerms) throws Exception {
        Map<String, Collection<SecurityPermission>> cachePerms = requiredPerms.cachePermissions();

        cachePerms.forEach((cache, perms) -> {
            for (SecurityPermission perm : perms)
                assertAuthorizationFailed(sql, excludeCachePermission(requiredPerms, cache, perm));
        });

        Collection<SecurityPermission> sysPerms = requiredPerms.systemPermissions();

        if (sysPerms != null) {
            for (SecurityPermission perm : sysPerms)
                assertAuthorizationFailed(sql, excludeSystemPermission(requiredPerms, perm));
        }

        executeWithPermissions(sql, requiredPerms);
    }

    /**
     * Asserts that authorization of SQL query on behalf of the user with the specified permissions fails.
     *
     * @param sql SQL query to execute.
     * @param perms Executive user permissions.
     */
    @SuppressWarnings("ThrowableNotThrown")
    protected void assertAuthorizationFailed(String sql, SecurityPermissionSet perms) {
        assertThrowsAnyCause(
            log,
            () -> {
                executeWithPermissions(sql, perms);

                return null;
            },
            SQLException.class,
            "Authorization failed");
    }

    /**
     * Excludes cache permission from specified {@link SecurityPermissionSet}.
     *
     * @param src Source {@link SecurityPermissionSet} from which cache permission will be excluded.
     * @param cache Name of cache from which {@code perm} will be excluded.
     * @param perm Cache permission to exclude.
     * @return New instance of {@link SecurityPermissionSet} with {@code perm} excluded for {@code cache}.
     */
    private SecurityPermissionSet excludeCachePermission(
        SecurityPermissionSet src,
        String cache,
        SecurityPermission perm
    ) {
        Map<String, Collection<SecurityPermission>> cachesPerms = new HashMap<>(src.cachePermissions());

        Collection<SecurityPermission> perms = new HashSet<>(cachesPerms.get(cache));

        perms.remove(perm);

        cachesPerms.put(cache, perms);

        SecurityBasicPermissionSet res = new SecurityBasicPermissionSet();

        res.setSystemPermissions(src.systemPermissions());
        res.setCachePermissions(cachesPerms);

        return res;
    }

    /**
     * Excludes system permission from specified {@link SecurityPermissionSet}.
     *
     * @param src Source {@link SecurityPermissionSet} from which system permission will be excluded.
     * @param perm System permission to exclude.
     * @return New instance of {@link SecurityPermissionSet} with {@code perm} excluded.
     */
    private SecurityPermissionSet excludeSystemPermission(SecurityPermissionSet src, SecurityPermission perm) {
        Collection<SecurityPermission> sysPerms = new HashSet<>(src.systemPermissions());

        sysPerms.remove(perm);

        SecurityBasicPermissionSet res = new SecurityBasicPermissionSet();

        res.setSystemPermissions(sysPerms);
        res.setCachePermissions(src.cachePermissions());

        return res;
    }

    /**
     * Executes an SQL query on behalf of a user with the specified security permissions.
     *
     * @param sql Query to execute.
     * @param perms Required permissions.
     */
    protected void executeWithPermissions(String sql, SecurityPermissionSet perms) throws Exception {
        String login = "jdbc-client";

        registerUser(login, perms);

        try {
            try (Connection conn = getConnection(login)) {
                Statement stmt = conn.createStatement();

                stmt.execute(sql);
            }
        }
        finally {
            removeUser(login);
        }
    }

    /**
     * Establishes JDBC connection on behalf of user with the specified login.
     *
     * @param login User login.
     * @return Connection to server node.
     */
    protected Connection getConnection(String login) throws SQLException {
        return DriverManager.getConnection(JDBC_URL_PREFIX + Config.SERVER, login, "");
    }

    /**
     * The type of the SQL query key for its dynamic generation with binding to a specific node.
     */
    private enum KeyType {
        /** Key for INSERT operation testing. */
        INSERT_KEY,

        /** Key for SELECT operation testing. */
        SELECT_KEY,

        /** Key for UPDATE operation testing. */
        UPDATE_KEY,

        /** Key for DELETE operation testing. */
        DELETE_KEY,

        /** Key for MERGE operation testing. */
        MERGE_KEY
    }
}
