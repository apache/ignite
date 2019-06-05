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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.H2LocalResultFactory;
import org.apache.ignite.internal.processors.query.h2.H2ManagedLocalResult;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.LocalResult;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Query memory manager tests.
 */
public abstract class AbstractQueryMemoryTrackerSelfTest extends GridCommonAbstractTest {
    /** Row count. */
    static final int SMALL_TABLE_SIZE = 1000;

    /** Row count. */
    static final int BIG_TABLE_SIZE = 10000;

    /** Query local results. */
    static final List<H2ManagedLocalResult> localResults = new ArrayList<>();

    /** Query memory limit. */
    long maxMem;

    /** Node client mode flag. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY, TestH2LocalResultFactory.class.getName());

        startGrid(0);

        if (!isLocal()) {
            client = true;

            startGrid(1);
        }

        populateData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_H2_LOCAL_RESULT_FACTORY);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        maxMem = 1024 * 1024;

        localResults.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientMode(client);
    }

    /**
     *
     */
    private void populateData() {
        execSql("create table T (id int primary key, ref_key int, name varchar)");
        execSql("create table K (id int primary key, indexed int, grp int, grp_indexed int, name varchar)");
        execSql("create index K_IDX on K(indexed)");
        execSql("create index K_GRP_IDX on K(grp_indexed)");

        for (int i = 0; i < SMALL_TABLE_SIZE; ++i)
            execSql("insert into T VALUES (?, ?, ?)", i, i, UUID.randomUUID().toString());

        for (int i = 0; i < BIG_TABLE_SIZE; ++i)
            execSql("insert into K VALUES (?, ?, ?, ?, ?)", i, i, i % 100, i % 100, UUID.randomUUID().toString());
    }

    /** Check simple query on small data set. */
    @Test
    public void testSimpleQuerySmallResult() {
        execQuery("select * from T", false);

        assertEquals(1, localResults.size());
        assertEquals(SMALL_TABLE_SIZE, localResults.get(0).getRowCount());
    }

    /** Check simple lazy query on large data set. */
    @Test
    public void testLazyQueryLargeResult() {
        execQuery("select * from K", true);

        assertEquals(0, localResults.size()); // No local result required.
    }

    /** Check simple query failure on large data set. */
    @Test
    public void testSimpleQueryLargeResult() {
        checkQueryExpectOOM("select * from K", false);

        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check simple query on large data set with small limit. */
    @Test
    public void testQueryWithLimit() {
        execQuery("select * from K LIMIT 500", false);

        assertEquals(1, localResults.size());
        assertEquals(500, localResults.get(0).getRowCount());
    }

    /** Check lazy query on large data set with large limit. */
    @Test
    public void testLazyQueryWithHighLimit() {
        execQuery("select * from K LIMIT 8000", true);

        assertEquals(0, localResults.size()); // No local result required.
    }

    /** Check simple query on large data set with small limit. */
    @Test
    public void testQueryWithHighLimit() {
        checkQueryExpectOOM("select * from K LIMIT 8000", false);

        assertEquals(1, localResults.size());
        assertTrue(8000 > localResults.get(0).getRowCount());
    }

    /** Check lazy query with ORDER BY indexed col. */
    @Test
    public void testLazyQueryWithSortByIndexedCol() {
        execQuery("select * from K ORDER BY K.indexed", true);

        // No local result needed.
        assertEquals(0, localResults.size());
    }

    /** Check query failure with ORDER BY indexed col. */
    @Test
    public void testQueryWithSortByIndexedCol() {
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);
        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check lazy query failure with ORDER BY non-indexed col. */
    @Test
    public void testLazyQueryWithSort() {
        checkQueryExpectOOM("select * from K ORDER BY K.grp", true);
        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check query failure with ORDER BY non-indexed col. */
    @Test
    public void testQueryWithSort() {
        // Order by non-indexed field.
        checkQueryExpectOOM("select * from K ORDER BY K.grp", false);
        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check UNION operation with large sub-selects. */
    @Test
    public void testUnionSimple() {
        maxMem = 8L * 1024 * 1024;

        execQuery("select * from T as T0, T as T1 where T0.id < 2 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 1 AND T2.id < 2", true);

        assertEquals(3, localResults.size());
        assertTrue(maxMem > localResults.get(1).memoryAllocated() + localResults.get(2).memoryAllocated());
        assertEquals(2000, localResults.get(1).getRowCount());
        assertEquals(1000, localResults.get(2).getRowCount());
        assertEquals(2000, localResults.get(0).getRowCount());
    }

    /** Check UNION operation with large sub-selects. */
    @Test
    public void testUnionLargeDataSets() {
        // None of sub-selects fits to memory.
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 4 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 2 AND T2.id < 6", true);

        assertEquals(2, localResults.size());
        assertTrue(maxMem < localResults.get(1).memoryAllocated());
        assertTrue(4000 > localResults.get(0).getRowCount());
        assertTrue(4000 > localResults.get(1).getRowCount());
    }

    /** Check large UNION operation with small enough sub-selects, but large result set. */
    @Test
    public void testUnionOfSmallDataSetsWithLargeResult() {
        maxMem = 3 * 1024 * 1024;

        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 2 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id > 2 AND T2.id < 4", false);

        assertEquals(3, localResults.size());
        assertTrue(maxMem > localResults.get(1).memoryAllocated() + localResults.get(2).memoryAllocated());
        assertEquals(2000, localResults.get(1).getRowCount());
        assertEquals(1000, localResults.get(2).getRowCount());
        assertTrue(3000 > localResults.get(0).getRowCount());
    }

    /** Check simple Joins. */
    @Test
    public void testSimpleJoins() {
        execQuery("select * from T as T0, T as T1 where T0.id < 2", false);
        execQuery("select * from T as T0, T as T1 where T0.id >= 2 AND T0.id < 4", false);
        execQuery("select * from T as T0, T as T1", true);
    }

    /** Check simple Joins. */
    @Test
    public void testSimpleJoinsHugeResult() {
        // Query with single huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1", false);

        assertEquals(1, localResults.size());
        assertTrue(maxMem < localResults.get(0).memoryAllocated());

    }

    /** Check simple Joins. */
    @Test
    public void testLazyQueryWithJoinAndSort() {
        // Query with huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1 ORDER BY T1.id", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem < localResults.get(0).memoryAllocated());
    }

    /** Check GROUP BY operation on large data set with small result set. */
    @Test
    public void testQueryWithGroupsSmallResult() {
        execQuery("select K.grp, avg(K.id), min(K.id), sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.

        assertEquals(1, localResults.size());
        assertEquals(100, localResults.get(0).getRowCount());
    }

    /** Check GROUP BY operation on indexed col. */
    @Test
    public void testQueryWithGroupByIndexedCol() {
        execQuery("select K.indexed, sum(K.grp) from K GROUP BY K.indexed", true);

        assertEquals(0, localResults.size());
    }

    /** Check GROUP BY operation on indexed col. */
    @Test
    public void testQueryWithGroupByPrimaryKey() {
        //TODO: GG-19071: make next test pass without hint.
        execQuery("select K.indexed, sum(K.id) from K USE INDEX (K_IDX) GROUP BY K.indexed", true);

        assertEquals(0, localResults.size());
    }

    /** Check GROUP BY operation on indexed col. */
    @Test
    public void testQueryWithGroupThenSort() {
        // Tiny local result with sorting.
        execQuery("select K.grp_indexed, sum(K.id) as s from K GROUP BY K.grp_indexed ORDER BY s", false);

        assertEquals(1, localResults.size());
        assertEquals(100, localResults.get(0).getRowCount());
    }

    /** Check lazy query with GROUP BY non-indexed col failure due to too many groups. */
    @Test
    public void testQueryWithGroupBy() {
        // Too many groups causes OOM.
        checkQueryExpectOOM("select K.name, count(K.id), sum(K.grp) from K GROUP BY K.name", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryAllocated());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
        //TODO: GG-18840: Add global limit check.
    }

    /** Check query with GROUP BY non-indexed col and with DISTINCT aggregates. */
    @Test
    public void testQueryWithGroupByNonIndexedColAndDistinctAggregates() {
        checkQueryExpectOOM("select K.grp, count(DISTINCT k.name) from K GROUP BY K.grp", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryAllocated());
        assertTrue(100 > localResults.get(0).getRowCount());
    }

    /** Check lazy query with GROUP BY indexed col and with and DISTINCT aggregates. */
    @Test
    public void testLazyQueryWithGroupByIndexedColAndDistinctAggregates() {
        execQuery("select K.grp_indexed, count(DISTINCT k.name) from K  USE INDEX (K_GRP_IDX) GROUP BY K.grp_indexed", true);

        assertEquals(0, localResults.size());
    }

    /** Check lazy query with GROUP BY indexed col (small result), then sort. */
    @Test
    public void testLazyQueryWithGroupByThenSort() {
        maxMem = 512 * 1024;

        checkQueryExpectOOM("select K.indexed, sum(K.grp) as a from K " +
            "GROUP BY K.indexed ORDER BY a DESC", true);

        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check query with DISTINCT and GROUP BY indexed col (small result). */
    @Test
    public void testQueryWithDistinctAndGroupBy() {
        checkQueryExpectOOM("select DISTINCT K.name from K GROUP BY K.id", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertTrue(maxMem < localResults.get(0).memoryAllocated());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check simple query with DISTINCT constraint. */
    @Test
    public void testQueryWithDistinct() {
        // Distinct on indexed column with small cardinality.
        execQuery("select DISTINCT K.grp_indexed from K", false);

        // Distinct on non-indexed column with small cardinality.
        execQuery("select DISTINCT K.grp from K", false);

        // Distinct on indexed column with unique values.
        checkQueryExpectOOM("select DISTINCT K.id from K", true);
    }

    /**
     * @param sql SQL query
     * @return Results set.
     */
    protected List<List<?>> execQuery(String sql, boolean lazy) {
        boolean localQry = isLocal();

        return grid(client ? 1 : 0).context().query().querySqlFields(
            new SqlFieldsQueryEx(sql, null)
                .setLocal(localQry)
                .setMaxMemory(maxMem)
                .setLazy(lazy)
                .setPageSize(100), false).getAll();
    }

    /**
     * @return Local query flag.
     */
    protected abstract boolean isLocal();

    /**
     * @param sql SQL query
     * @param args Query parameters.
     */
    protected void execSql(String sql, Object... args) {
        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }

    /**
     * @param sql SQL query.
     * @param lazy Lazy flag.
     */
    protected void checkQueryExpectOOM(String sql, boolean lazy) {
        GridTestUtils.assertThrows(log, () -> {
            execQuery(sql, lazy);

            return null;
        }, CacheException.class, "IgniteOutOfMemoryException: SQL query out of memory");
    }

    /**
     * Local result factory for test.
     */
    public static class TestH2LocalResultFactory extends H2LocalResultFactory {
        /** {@inheritDoc} */
        @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt) {
            LocalResult res = super.create(ses, expressions, visibleColCnt);

            if (res instanceof H2ManagedLocalResult)
                localResults.add((H2ManagedLocalResult)res);

            return res;
        }

        /** {@inheritDoc} */
        @Override public LocalResult create() {
            throw new NotImplementedException();
        }
    }
}