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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.SessionContextProviderResource;
import org.apache.ignite.session.SessionContextProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assume.assumeFalse;

/** */
@RunWith(Parameterized.class)
public class JdbcSetClientInfoCacheInterceptorTest extends GridCommonAbstractTest {
    /** */
    private static final String SESSION_ID = "sessionId";

    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** */
    @Parameterized.Parameter
    public boolean runInTx;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode cacheMode;

    /** */
    @Parameterized.Parameters(name = "runInTx={0}, mode={1}")
    public static Collection<Object[]> data() {
        return GridTestUtils.cartesianProduct(
            F.asList(false, true),
            F.asList(CacheAtomicityMode.TRANSACTIONAL, CacheAtomicityMode.ATOMIC));
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)));

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(runInTx);

        QueryEntity entity = new QueryEntity()
            .setTableName("MYTABLE")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("sessionId", String.class.getName(), null)
            .setKeyFieldName("id")
            .setValueFieldName("sessionId");

        cfg.setCacheConfiguration(new CacheConfiguration<Integer, String>()
            .setAtomicityMode(cacheMode)
            .setName(DEFAULT_CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(List.of(entity))
            .setInterceptor(new SessionContextCacheInterceptor()));

        return cfg;
    }

    /** */
    @Override protected void beforeTest() {
        assumeFalse(runInTx && cacheMode == CacheAtomicityMode.ATOMIC);
    }

    /** */
    @Test
    public void testInterceptInsert() throws Exception {
        try (Ignite ignore = startGrid(); Connection conn = DriverManager.getConnection(URL)) {
            String sessionid = "42";

            conn.setClientInfo(SESSION_ID, sessionid);

            try (Statement s = conn.createStatement()) {
                assertEquals(1, s.executeUpdate(
                    "insert into PUBLIC.MYTABLE(id, sessionId) values (0, 'must be changed to sessionId in CacheInterceptor');"));
            }

            checkRecordExist(conn, sessionid);
        }
    }

    /** */
    @Test
    public void testInterceptUpdate() throws Exception {
        try (Ignite ignore = startGrid(); Connection conn = DriverManager.getConnection(URL)) {
            try (Statement s = conn.createStatement()) {
                assertEquals(1, s.executeUpdate("insert into PUBLIC.MYTABLE(id, sessionId) values (0, '1');"));
            }

            String sessionid = "42";

            conn.setClientInfo(SESSION_ID, sessionid);

            try (Statement s = conn.createStatement()) {
                assertEquals(1, s.executeUpdate(
                    "update PUBLIC.MYTABLE set sessionId = 'must be changed to sessionId in CacheInterceptor' where id = 0;"));
            }

            checkRecordExist(conn, sessionid);
        }
    }

    /** */
    @Test
    public void testInterceptDelete() throws Exception {
        try (Ignite ignore = startGrid(); Connection conn = DriverManager.getConnection(URL)) {
            try (Statement s = conn.createStatement()) {
                assertEquals(1, s.executeUpdate(
                    "insert into PUBLIC.MYTABLE(id, sessionId) values (0, 'survives deletion due to CacheInterceptor');"));
            }

            conn.setClientInfo(SESSION_ID, "42");

            try (Statement s = conn.createStatement()) {
                // Record will be kept in table because of SessionContextCacheInterceptor#onBeforeRemove cancels the remove.
                s.executeUpdate("delete from PUBLIC.MYTABLE where id = 0;");
            }

            checkRecordExist(conn, "survives deletion due to CacheInterceptor");
        }
    }

    /** */
    private void checkRecordExist(Connection conn, String expVal) throws Exception {
        try (Statement s = conn.createStatement()) {
            assertTrue(s.execute("select id, sessionId from PUBLIC.MYTABLE;"));

            ResultSet rs = s.getResultSet();
            assertTrue(rs.next());

            assertEquals(0, rs.getInt("id"));
            assertEquals(expVal, rs.getString("sessionId"));
        }
    }

    /** */
    public static class SessionContextCacheInterceptor extends CacheInterceptorAdapter<Integer, String> {
        /** */
        @SessionContextProviderResource
        private SessionContextProvider sessionCtxProv;

        /** Replaces {@code newVal} explicitly inserted by user with {@code SESSION_ID} attribute value. */
        @Override public @Nullable String onBeforePut(Cache.Entry<Integer, String> entry, String newVal) {
            String val = sessionCtxProv.getSessionContext().getAttribute(SESSION_ID);

            return val == null ? newVal : val;
        }

        /** Cancels removing entry in case {@code SESSION_ID} attribute is set. */
        @Override public @Nullable IgniteBiTuple<Boolean, String> onBeforeRemove(Cache.Entry<Integer, String> entry) {
            String val = sessionCtxProv.getSessionContext().getAttribute(SESSION_ID);

            return new IgniteBiTuple<>(val != null, entry.getValue());
        }
    }

    /** */
    private List<List<?>> query(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
