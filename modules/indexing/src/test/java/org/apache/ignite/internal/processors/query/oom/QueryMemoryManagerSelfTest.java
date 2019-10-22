/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.oom;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.H2LocalResultFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.LocalResult;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static org.apache.ignite.internal.util.IgniteUtils.MB;

public class QueryMemoryManagerSelfTest extends GridCommonAbstractTest {
    /** Node client mode flag. */
    protected boolean client;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.clearProperty(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE);
        System.clearProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_QUERY_MEMORY_LIMIT);
        System.clearProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE);
        System.clearProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY);

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE);
        System.clearProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_QUERY_MEMORY_LIMIT);
        System.clearProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE);
        System.clearProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY);

        stopAllGrids();
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testDefaults() throws Exception {
        final long maxMem = Runtime.getRuntime().maxMemory();

        System.setProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY, TestH2LocalResultFactory.class.getName());
        System.setProperty(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, String.valueOf(maxMem));
        System.clearProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_QUERY_MEMORY_LIMIT);
        System.clearProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE);

        startGrid(0);
        client = true;
        startGrid(1);

        createSchema();

        populateData();

        final String sql = "select * from T as T0, T as T1 where T0.id < 1 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 2 AND T2.id < 3";

        try (FieldsQueryCursor<List<?>> cursor = query(sql, false)) {
            cursor.getAll();
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testWrongSqlMemoryPoolSize() throws Exception {
        final long maxMem = Runtime.getRuntime().maxMemory();

        System.setProperty(IgniteSystemProperties.IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE, String.valueOf(maxMem));

        GridTestUtils.assertThrows(log, () -> {
            startGrid(0);
        }, IgniteException.class, "Sql memory pool size can't be more than heap memory max size");
    }

    /**
     *
     */
    private void populateData() {
        for (int i = 0; i < 1000; ++i)
            execSql("insert into T VALUES (?, ?, ?)", i, i, UUID.randomUUID().toString());

        for (int i = 0; i < 10_000; ++i)
            execSql("insert into K VALUES (?, ?, ?, ?, ?)", i, i, i % 100, i % 100, UUID.randomUUID().toString());
    }

    /**
     *
     */
    private void createSchema() {
        execSql("create table T (id int primary key, ref_key int, name varchar)");
        execSql("create table K (id int primary key, indexed int, grp int, grp_indexed int, name varchar)");
        execSql("create index K_IDX on K(indexed)");
        execSql("create index K_GRP_IDX on K(grp_indexed)");
    }

    /**
     * @param sql SQL query
     * @param args Query parameters.
     */
    private void execSql(String sql, Object... args) {
        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /**
     * @param sql SQL query
     * @return Results set.
     */
    FieldsQueryCursor<List<?>> query(String sql, boolean lazy) {
        return grid(1).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null)
                .setMaxMemory(MB)
                .setLazy(lazy)
                .setEnforceJoinOrder(true)
                .setPageSize(100), false);
    }

    /**
     * Local result factory for test.
     */
    public static class TestH2LocalResultFactory extends H2LocalResultFactory {
        /** {@inheritDoc} */
        @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt) {
            assertNull(ses.queryMemoryTracker());

            return super.create(ses, expressions, visibleColCnt);
        }

        /** {@inheritDoc} */
        @Override public LocalResult create() {
            throw new NotImplementedException();
        }
    }
}
