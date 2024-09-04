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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.SQL_VIEW_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.SQL_VIEW_DROP;

/**
 * Test authorization of different operations.
 */
@RunWith(Parameterized.class)
public class AuthorizationIntegrationTest extends AbstractSecurityTest {
    /** */
    private static final String LOGIN = "client";

    /** */
    private static final String PWD = "pwd";

    /** */
    private static final String ALLOWED_CACHE = "allowed_cache";

    /** */
    private static final String ALLOWED_READ_CACHE = "allowed_read_cache";

    /** */
    private static final String FORBIDDEN_CACHE = "forbidden_cache";

    /** */
    private static final AtomicInteger putCnt = new AtomicInteger();

    /** */
    private static final AtomicInteger removeCnt = new AtomicInteger();

    /** */
    @Parameterized.Parameter
    public boolean allowDdl;

    /** */
    @Parameterized.Parameters(name = "allowDdl = {0}")
    public static Iterable<Object> parameters() {
        return Arrays.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setIncludeEventTypes(EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED)
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected TestSecurityData[] securityData() {
        return new TestSecurityData[] {
            new TestSecurityData(LOGIN, PWD, clientPermissions(), null)
        };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteEx grid0 = startGridAllowAll("srv1");
        IgniteEx grid1 = startGridAllowAll("srv2");

        grid0.getOrCreateCache(new CacheConfiguration<>(ALLOWED_CACHE).setIndexedTypes(Integer.class, Integer.class));
        grid0.getOrCreateCache(new CacheConfiguration<>(ALLOWED_READ_CACHE).setIndexedTypes(Integer.class, Integer.class));
        grid0.getOrCreateCache(new CacheConfiguration<>(FORBIDDEN_CACHE).setIndexedTypes(Integer.class, Integer.class));

        IgnitePredicate<CacheEvent> lsnrPut = evt -> {
            // Ensure event is triggered with the correct security context.
            ensureSubjId(grid0, evt.subjectId());

            putCnt.incrementAndGet();

            return true;
        };

        IgnitePredicate<CacheEvent> lsnrRemove = evt -> {
            // Ensure event is triggered with the correct security context.
            ensureSubjId(grid0, evt.subjectId());

            removeCnt.incrementAndGet();

            return true;
        };

        grid0.events().localListen(lsnrPut, EVT_CACHE_OBJECT_PUT);
        grid1.events().localListen(lsnrPut, EVT_CACHE_OBJECT_PUT);
        grid0.events().localListen(lsnrRemove, EVT_CACHE_OBJECT_REMOVED);
        grid1.events().localListen(lsnrRemove, EVT_CACHE_OBJECT_REMOVED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testClientNode() throws Exception {
        try (IgniteEx clientNode = startGrid(getConfiguration("client",
                new TestSecurityPluginProvider(LOGIN, PWD, clientPermissions(), null,
                    globalAuth, securityData())).setClientMode(true))
        ) {
            check(
                sql -> clientNode.cache(ALLOWED_CACHE).query(sqlFieldsQuery(sql)).getAll(),
                SecurityException.class,
                "Authorization failed"
            );
        }
    }

    /** */
    @Test
    public void testThinClient() throws Exception {
        try (IgniteClient client = Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER).setUserName(LOGIN).setUserPassword(PWD))
        ) {
            check(
                sql -> client.cache(ALLOWED_CACHE).query(sqlFieldsQuery(sql)).getAll(),
                ClientAuthorizationException.class,
                "User is not authorized"
            );
        }
    }

    /** */
    @Test
    public void testJdbc() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?user=" + LOGIN +
            "&password=" + PWD)
        ) {
            try (Statement stmt = conn.createStatement()) {
                check(stmt::execute, SQLException.class, "Authorization failed");
            }
        }
    }

    /** */
    private SecurityPermissionSet clientPermissions() {
        SecurityPermissionSetBuilder permBuilder = SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendCachePermissions(ALLOWED_CACHE, CACHE_PUT, CACHE_READ, CACHE_REMOVE)
            .appendCachePermissions(ALLOWED_READ_CACHE, CACHE_READ)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS);

        if (allowDdl)
            permBuilder.appendSystemPermissions(SQL_VIEW_CREATE, SQL_VIEW_DROP, CACHE_CREATE, CACHE_DESTROY);

        return permBuilder.build();
    }

    /** */
    private void check(SqlExecutor sqlExecutor, Class<? extends Exception> errCls, String errMsg) throws Exception {
        putCnt.set(0);
        removeCnt.set(0);
        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            sqlExecutor.execute(insertSql(ALLOWED_CACHE, i));

        sqlExecutor.execute(selectSql(ALLOWED_CACHE));
        sqlExecutor.execute(deleteSql(ALLOWED_CACHE));

        assertEquals(cnt, putCnt.get());
        assertEquals(cnt, removeCnt.get());

        for (int i = 0; i < cnt; i++)
            assertThrows(sqlExecutor, insertSql(FORBIDDEN_CACHE, i), errCls, errMsg);

        assertThrows(sqlExecutor, selectSql(FORBIDDEN_CACHE), errCls, errMsg);
        assertThrows(sqlExecutor, deleteSql(FORBIDDEN_CACHE), errCls, errMsg);

        for (int i = 0; i < cnt; i++)
            assertThrows(sqlExecutor, insertSql(ALLOWED_READ_CACHE, i), errCls, errMsg);

        sqlExecutor.execute(selectSql(ALLOWED_READ_CACHE));
        assertThrows(sqlExecutor, deleteSql(ALLOWED_READ_CACHE), errCls, errMsg);

        if (allowDdl) {
            sqlExecutor.execute("CREATE TABLE test(id INT, val VARCHAR)");
            sqlExecutor.execute("CREATE VIEW test_view AS SELECT * FROM test");
            sqlExecutor.execute("DROP VIEW test_view");
            sqlExecutor.execute("DROP TABLE test");
        }
        else {
            assertThrows(sqlExecutor, "CREATE TABLE test(id INT, val VARCHAR)", errCls, errMsg);
            assertThrows(sqlExecutor, "CREATE VIEW test_view AS SELECT * FROM test", errCls, errMsg);

            executeOnServer("CREATE TABLE test(id INT, val VARCHAR)");
            executeOnServer("CREATE VIEW test_view AS SELECT * FROM test");

            assertThrows(sqlExecutor, "DROP VIEW test_view", errCls, errMsg);
            assertThrows(sqlExecutor, "DROP TABLE test", errCls, errMsg);
        }
    }

    /** */
    private void executeOnServer(String sql) {
        grid("srv1").cache(ALLOWED_CACHE).query(sqlFieldsQuery(sql)).getAll();
    }

    /** */
    private SqlFieldsQuery sqlFieldsQuery(String sql) {
        return new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA);
    }

    /** Ensure security context subject relates to client.  */
    private void ensureSubjId(IgniteEx ignite, UUID subjId) {
        try {
            assertEquals(LOGIN, ignite.context().security().authenticatedSubject(subjId).login());
        }
        catch (IgniteCheckedException e) {
            throw new AssertionError("Unexpected exception", e);
        }
    }

    /** */
    private void assertThrows(SqlExecutor sqlExecutor, String sql, Class<? extends Exception> errCls, String errMsg) {
        GridTestUtils.assertThrowsAnyCause(log, () -> {
            sqlExecutor.execute(sql);

            return null;
        }, errCls, errMsg);
    }

    /** */
    private String insertSql(String cacheName, int key) {
        return "INSERT INTO \"" + cacheName + "\".Integer (_KEY, _VAL) VALUES (" + key + ", " + key + ')';
    }

    /** */
    private String selectSql(String cacheName) {
        return "SELECT _KEY, _VAL FROM \"" + cacheName + "\".Integer";
    }

    /** */
    private String deleteSql(String cacheName) {
        return "DELETE FROM \"" + cacheName + "\".Integer";
    }

    /** Functional interface (throwable consumer) for SQL execution by different clients. */
    private interface SqlExecutor {
        /** */
        void execute(String sql) throws Exception;
    }
}
