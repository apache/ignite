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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.result.LazyResult;
import org.h2.result.ResultInterface;

/**
 * Tests for local query execution in lazy mode.
 */
public class LocalQueryLazyTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** Queries count. */
    private static final int QRY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();

        IgniteCache c = grid().createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("TEST")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test local query execution.
     */
    public void test() {
        Iterator[] iters = new Iterator[QRY_CNT];

        for (int i = 0; i < QRY_CNT; ++i) {
            iters[i] = sql("SELECT * FROM test").iterator();

            ResultInterface res = GridTestUtils.getFieldValueHierarchy(iters[i], "iter", "iter", "res");

            assertTrue("Unexpected result type " + res.getClass(), res instanceof LazyResult);
        }

        // Scan and close iterator in reverse order.
        for (int i = QRY_CNT - 1; i >= 0; --i) {
            while (iters[i].hasNext())
                iters[i].next();
        }
    }

    /**
     * Test use valid query context for local lazy queries.
     * @throws Exception On error.
     */
    public void testDuplicates() throws Exception {
        IgniteEx g0 = grid();
        IgniteEx g1 = startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        int r0 = sql(g0, "SELECT * FROM test").getAll().size();
        int r1 = sql(g1, "SELECT * FROM test").getAll().size();

        // Check that primary partitions are scanned on each node.
        assertTrue(r0 < KEY_CNT);
        assertTrue(r1 < KEY_CNT);
        assertEquals(KEY_CNT, r0 + r1);
    }

    /** */
    public void testParallelIteratorWithReducePhase() throws Exception {
        IgniteEx g1 = startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        Iterator<List<?>> cursorIter0 = sql(g1, "SELECT * FROM test limit 3").iterator();
        Iterator<List<?>> cursorIter1 = distributedSql(g1, "SELECT * FROM test limit 3").iterator();

        int r0 = 0;
        int r1 = 0;

        for (int i =0; i< KEY_CNT; i++) {
            if (cursorIter0.hasNext()) {
                r0++;

                cursorIter0.next();
            }

            if (cursorIter1.hasNext()) {
                r1++;

                cursorIter1.next();
            }
        }

        assertEquals(3, r0);
        assertEquals(3, r1);
    }

    /** */
    public void testParallelIterator() throws Exception {
        IgniteEx g1 = startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        Iterator<List<?>> cursorIter0 = sql(g1, "SELECT * FROM test").iterator();
        Iterator<List<?>> cursorIter1 = distributedSql(g1, "SELECT * FROM test").iterator();

        int r0 = 0;
        int r1 = 0;

        for (int i =0; i < KEY_CNT; i++) {
            if (cursorIter0.hasNext()) {
                r0++;

                cursorIter0.next();
            }

            if (cursorIter1.hasNext()) {
                r1++;

                cursorIter1.next();
            }
        }

        assertTrue(r0 < KEY_CNT);
        assertEquals(KEY_CNT, r1);
    }

    /** */
    public void testParallelLocalQueries() throws Exception {
        startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        Iterator<List<?>> it = grid().context().query().querySqlFields(new SqlFieldsQuery("SELECT * FROM test")
            .setLocal(true)
            .setLazy(true)
            .setSchema("TEST")
            .setPageSize(1), false).iterator();

        AtomicInteger part = new AtomicInteger();

        it.forEachRemaining((r) -> {
            List<?> innerRes = grid().context().query().querySqlFields(new SqlFieldsQuery("SELECT * FROM test")
                .setLocal(true)
                .setLazy(true)
                .setSchema("TEST")
                .setPartitions(part.getAndIncrement()), false).getAll();

            assertEquals(1, innerRes.size());
        });

        assertTrue(part.get() < KEY_CNT);
    }

    /** */
    public void testRetryLocalQueryByDDL() throws Exception {
        startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        final Iterator<List<?>> it = grid().context().query().querySqlFields(new SqlFieldsQuery("SELECT * FROM test")
            .setLocal(true)
            .setLazy(true)
            .setSchema("TEST")
            .setPageSize(1), false).iterator();

        it.next();

        distributedSql(grid(), "CREATE INDEX IDX_VAL ON TEST(val)");

        GridTestUtils.assertThrows(log, () -> {
            it.next();

            return null;
        }, QueryRetryException.class, "Table was modified concurrently (please retry the query)");
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        return sql(grid(), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object ... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLocal(true)
            .setLazy(true)
            .setSchema("TEST")
            .setArgs(args), false);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> distributedSql(IgniteEx ign, String sql, Object ... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setSchema("TEST")
            .setArgs(args), false);
    }
}
