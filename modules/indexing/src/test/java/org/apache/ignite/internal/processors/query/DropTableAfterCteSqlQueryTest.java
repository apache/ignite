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

package org.apache.ignite.internal.processors.query;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Checks SQL query with the WITH clause (AKA Common Table Expressions (CTEs))
 * doesn't break the H2 SQL Engine.
 */
@RunWith(Parameterized.class)
public class DropTableAfterCteSqlQueryTest extends AbstractIndexingCommonTest {
    /** */
    private static final String SCHEMA = "PUBLIC";

    /** */
    private static final String VALID_SQL =
        "WITH cte AS (SELECT id FROM T2) " +
        "SELECT * FROM T1 JOIN cte ON cte.id = T1.id";

    /** */
    private static final String INVALID_SQL =
        "WITH cte AS (SELECT id FROM T2) " +
        "SELECT * FROM absent_table_to_emulate_statement_parsing_failure";

    /** Node. */
    private IgniteEx node;

    /** */
    final AtomicBoolean failed = new AtomicBoolean(false);

    /** */
    final CountDownLatch completed = new CountDownLatch(1);

    /** */
    @Parameterized.Parameter(value = 0)
    public Boolean validSql;

    /** */
    @Parameterized.Parameter(value = 1)
    public String sql;

    /** */
    @Parameterized.Parameters(name = "VALID_SQL={0}")
    public static Collection<Object[]> params() {
        return F.asList(
            new Object[] {true, VALID_SQL},
            new Object[] {false, INVALID_SQL});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new TestFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = startGrid(getConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Check that CTE query doesn't prevent the table drop and doesn't crash the
     * ignite node on table recreate if used via the Ignite API.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testCteQueryDoesNotCrashNode() throws Exception {
        node.createCache(new CacheConfiguration<>("T1")
            .setQueryEntities(List.of(
                new QueryEntity("java.lang.Integer", "T1")
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("data", String.class.getName(), null)
                    .setKeyFieldName("id")
            ))
            .setSqlSchema(SCHEMA));

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>("T2")
            .setQueryEntities(List.of(
                new QueryEntity("java.lang.Integer", "T2")
                    .addQueryField("id", Integer.class.getName(), null)
                    .addQueryField("data", String.class.getName(), null)
                    .setIndexes(List.of(
                        new QueryIndex("id", false, "id_idx")))
                    .setKeyFieldName("id")
            ))
            .setSqlSchema(SCHEMA);

        IgniteCache<Object, Object> cache = node.createCache(cfg);

        addData();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (validSql) {
            assertEquals(1, cache.query(qry).getAll().size());
        }
        else
            assertThrowsWithCause(() -> cache.query(qry).getAll(), IgniteSQLException.class);

        node.destroyCache("T2");

        IgniteInternalFuture<Object> fut = runAsync(() -> {
            node.createCache(cfg);

            completed.countDown();
        });

        completed.await();

        fut.cancel();

        assertFalse("Ignite node should not crash", failed.get());
    }

    /**
     * Check that CTE query doesn't prevent the table drop and doesn't prevent
     * the table recreate if used via the SQL API.
     */
    @Test
    public void testCteQueryDoesNotPreventTableRecreate() {
        sql("CREATE TABLE T1 (id INTEGER PRIMARY KEY, data VARCHAR)");

        sql("CREATE TABLE T2 (id INTEGER PRIMARY KEY, data VARCHAR)");
        sql("CREATE INDEX IDX ON T2 (id)");

        addData();

        if (validSql) {
            assertEquals(1, sql(sql).size());
        }
        else
            assertThrowsWithCause(() -> sql(sql), IgniteSQLException.class);

        sql("DROP TABLE T2");

        sql("CREATE TABLE T2 (id INTEGER PRIMARY KEY, data VARCHAR)");
        sql("CREATE INDEX IDX ON T2 (id)");
    }

    /**
     * Test failure handler.
     */
    private class TestFailureHandler implements FailureHandler {
        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            failed.set(true);

            completed.countDown();

            return true;
        }
    }

    /** */
    private void addData() {
        sql("INSERT INTO T1 (id, data) VALUES (1, 'one')");
        sql("INSERT INTO T1 (id, data) VALUES (2, 'two')");

        sql("INSERT INTO T2 (id, data) VALUES (2, 'two')");
        sql("INSERT INTO T2 (id, data) VALUES (3, 'three')");
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    private List<List<?>> sql(String sql) {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema(SCHEMA);

        try (FieldsQueryCursor<List<?>> cursor = qryProc.querySqlFields(qry, true)) {
            return cursor.getAll();
        }
    }
}
