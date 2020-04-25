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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.result.LazyResult;
import org.h2.result.ResultInterface;
import org.junit.Test;

/**
 * Tests for local query execution in lazy mode.
 */
public class LocalQueryLazyTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** Queries count. */
    private static final int QRY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();

        IgniteCache<Long, Long> c = grid().createCache(new CacheConfiguration<Long, Long>()
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
    @Test
    public void testLocalLazyQuery() {
        Iterator[] iters = new Iterator[QRY_CNT];

        for (int i = 0; i < QRY_CNT; ++i) {
            iters[i] = sql("SELECT * FROM test").iterator();

            ResultInterface res = GridTestUtils.getFieldValueHierarchy(iters[i], "iter", "delegateIt", "iter", "res");

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
    @Test
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

    /**
     * Test must not hang on CREATE INDEX. Table lock must be not locked by opened lazy iterator of local query
     * (was locked before fix).
     * @throws Exception On error.
     */
    @Test
    public void testTableUnlockOnNotFinishedQuery() throws Exception {
        IgniteEx g0 = grid();
        IgniteEx g1 = startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        try (FieldsQueryCursor cur = sql(g0,"SELECT * FROM test")) {
            Iterator<List<?>> it = cur.iterator();

            it.next();

            sqlDistrubuted(g1, "CREATE INDEX IDX_VAL ON TEST(VAL)");
        }
    }

    /** */
    @Test
    public void testDropTableWithOpenCursor() throws Exception {
        startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        sqlDistrubuted(grid(), "CREATE TABLE TBL0 (id INT PRIMARY KEY, name VARCHAR)");

        for (int i = 0; i < 100; ++i)
            sqlDistrubuted(grid(), "INSERT INTO TBL0 (id, name) VALUES (?, ?)", i, "val0_" + i);

        final Iterator<List<?>> it = grid().context().query().querySqlFields(new SqlFieldsQuery("SELECT * FROM TBL0")
            .setLocal(true)
            .setLazy(true)
            .setSchema("TEST")
            .setPageSize(1), false).iterator();

        it.next();

        sqlDistrubuted(grid(), "DROP TABLE TBL0");

        GridTestUtils.assertThrows(log, () -> {
            it.next();

            return null;
        }, QueryRetryException.class, "Table was modified concurrently (please retry the query)");
    }

    /** */
    @Test
    public void testDeactivateWithOpenCursor() throws Exception {
        startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        final Iterator<List<?>> it = grid().context().query().querySqlFields(new SqlFieldsQuery("SELECT * FROM test")
            .setLocal(true)
            .setLazy(true)
            .setSchema("TEST")
            .setPageSize(1), false).iterator();

        it.next();

        grid(0).cluster().active(false);

        assertFalse(grid().cluster().active());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return sql(grid(), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object... args) {
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
    private FieldsQueryCursor<List<?>> sqlDistrubuted(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setSchema("TEST")
            .setArgs(args), false);
    }
}
