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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Query memory manager for local queries.
 */
public class LocalQueryMemoryTrackerWithQueryParallelismSelfTest extends AbstractQueryMemoryTrackerSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    protected void createSchema() {
        execSql("create table T (id int primary key, ref_key int, name varchar) WITH \"PARALLELISM=4\"");
        execSql("create table K (id int primary key, indexed int, grp int, grp_indexed int, name varchar) WITH \"PARALLELISM=4\"");
        execSql("create index K_IDX on K(indexed)");
        execSql("create index K_GRP_IDX on K(grp_indexed)");
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSimpleQuerySmallResult() throws Exception {
        execQuery("select * from T", false);

        long rowCount = localResults.stream().mapToLong(r -> r.getRowCount()).sum();

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertEquals(SMALL_TABLE_SIZE, rowCount);
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithSort() {
        maxMem = 2 * MB;
        // Order by non-indexed field.
        checkQueryExpectOOM("select * from K ORDER BY K.grp", false);

        assertEquals(5, localResults.size());
        // Map
        assertEquals(BIG_TABLE_SIZE, localResults.stream().limit(4).mapToLong(r -> r.getRowCount()).sum());
        // Reduce
        assertTrue(BIG_TABLE_SIZE > localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testGlobalQuota() throws Exception {
        final List<QueryCursor> cursors = new ArrayList<>();

        IgniteH2Indexing h2 = (IgniteH2Indexing)grid(0).context().query().getIndexing();

        assertEquals(10L * MB, h2.memoryManager().maxMemory());

        try {
            CacheException ex = (CacheException)GridTestUtils.assertThrows(log, () -> {
                for (int i = 0; i < 100; i++) {
                    QueryCursor<List<?>> cur = query("select T.name, avg(T.id), sum(T.ref_key) from T GROUP BY T.name",
                        true);

                    cursors.add(cur);

                    Iterator<List<?>> iter = cur.iterator();
                    iter.next();
                }

                return null;
            }, CacheException.class, "SQL query run out of memory: Global quota exceeded.");

            assertEquals(18, cursors.size());

            assertTrue(h2.memoryManager().maxMemory() < h2.memoryManager().memoryReserved() + MB);
        }
        finally {
            for (QueryCursor c : cursors)
                IgniteUtils.closeQuiet(c);
        }
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testUnionOfSmallDataSetsWithLargeResult() {
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 2 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id > 2 AND T2.id < 4", false);

        assertEquals(11, localResults.size());

        long rowCount = localResults.stream().mapToLong(r -> r.getRowCount()).sum();

        assertTrue(3000 > rowCount);

        Map<H2MemoryTracker, Long> collect = localResults.stream().collect(
            Collectors.toMap(r -> r.getMemoryTracker(), r -> r.memoryReserved(), Long::sum));
        assertTrue(collect.values().stream().anyMatch(s -> s + 1000 > maxMem));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithJoinAndSort() {
        // Query with huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1 ORDER BY T1.id", true);

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertTrue(localResults.stream().anyMatch(r -> r.memoryReserved() + 500 > maxMem));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testUnionLargeDataSets() {
        // None of sub-selects fits to memory.
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 4 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 2 AND T2.id < 6", true);

        assertEquals(3, localResults.size());
        // Reduce
        assertEquals(0, localResults.get(0).getRowCount());
        // Map
        assertTrue(4000 > localResults.get(1).getRowCount() + localResults.get(2).getRowCount());

    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithLimit() throws Exception {
        execQuery("select * from K LIMIT 500", false);

        assertEquals(5, localResults.size());
        // Reduce
        assertTrue(localResults.stream().allMatch(r -> r.getRowCount() == 500));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithDistinctAndGroupBy() throws Exception {
        checkQueryExpectOOM("select DISTINCT K.name from K GROUP BY K.id", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertEquals(0, localResults.get(0).memoryReserved());
        assertEquals(0, localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSimpleJoinsHugeResult() {
        // Query with single huge local result.
        checkQueryExpectOOM("select * from T as T0, T as T1", false);

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertTrue(localResults.stream().anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithSort() {
        checkQueryExpectOOM("select * from K ORDER BY K.grp", true);

        assertEquals(5, localResults.size());
        assertFalse(localResults.stream().limit(4).anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        assertTrue(maxMem < localResults.get(4).memoryReserved() + 1000);
        // Map
        assertEquals(BIG_TABLE_SIZE, localResults.stream().limit(4).mapToLong(r -> r.getRowCount()).sum());
        // Reduce
        assertTrue(BIG_TABLE_SIZE > localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithSortByIndexedCol() throws Exception {
        // OOM on reducer.
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", true);

        // Reduce only.
        assertEquals(1, localResults.size());
        assertTrue(maxMem < localResults.get(0).memoryReserved() + 500);
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupBy() {
        // Too many groups causes OOM.
        checkQueryExpectOOM("select K.name, count(K.id), sum(K.grp) from K GROUP BY K.name", true);

        // Local result is quite small.
        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertTrue(BIG_TABLE_SIZE >  localResults.stream().mapToLong(r -> r.getRowCount()).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupByPrimaryKey() throws Exception {
        //TODO: GG-19071: make next test pass without hint.
        // OOM on reducer.
        checkQueryExpectOOM("select K.indexed, sum(K.id) from K USE INDEX (K_IDX) GROUP BY K.indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testUnionSimple() throws Exception {
        maxMem = 2L * MB;

        execQuery("select * from T as T0, T as T1 where T0.id < 2 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 1 AND T2.id < 2", true);

        assertEquals(3, localResults.size());
        assertTrue(maxMem > localResults.get(1).memoryReserved() + localResults.get(2).memoryReserved());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithSortByIndexedCol() {
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", false);

        assertEquals(5, localResults.size());
        assertFalse(localResults.stream().limit(4).anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        // Map
        assertEquals(BIG_TABLE_SIZE, localResults.stream().limit(4).mapToLong(r -> r.getRowCount()).sum());
        // Reduce
        assertTrue(BIG_TABLE_SIZE > localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupsSmallResult() throws Exception {
        execQuery("select K.grp, avg(K.id), min(K.id), sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.

        assertEquals(5, localResults.size());
        assertFalse(localResults.stream().limit(4).anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        // Map
        assertEquals(100, localResults.stream().limit(4).mapToLong(r -> r.getRowCount()).sum());
        // Reduce
        assertEquals(100, localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithHighLimit() throws Exception {
        // OOM on reducer.
        checkQueryExpectOOM("select * from K LIMIT 8000", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem < localResults.get(0).memoryReserved() + 1000);
        assertTrue(8000 > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithHighLimit() {
        checkQueryExpectOOM("select * from K LIMIT 8000", false);

        assertEquals(5, localResults.size());
        // Map
        assertFalse(localResults.stream().limit(4).anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        // Reduce
        assertTrue(maxMem < localResults.get(4).memoryReserved() + 1000);
        assertTrue(8000 > localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithGroupByIndexedColAndDistinctAggregates() throws Exception {
        checkQueryExpectOOM("select K.grp_indexed, count(DISTINCT k.name) from K  USE INDEX (K_GRP_IDX) GROUP BY K.grp_indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(100 > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupThenSort() throws Exception {
        // Tiny local result with sorting.
        execQuery("select K.grp_indexed, sum(K.id) as s from K GROUP BY K.grp_indexed ORDER BY s", false);

        assertEquals(5, localResults.size());
        assertEquals(100, localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSimpleQueryLargeResult() throws Exception {
        execQuery("select * from K", false);

        assertFalse(localResults.isEmpty());
        assertTrue(localResults.size() <= 4);
        assertEquals(BIG_TABLE_SIZE, localResults.stream().limit(4).mapToLong(r -> r.getRowCount()).sum());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupByIndexedCol() throws Exception {
        checkQueryExpectOOM("select K.indexed, sum(K.grp) from K GROUP BY K.indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(100 > localResults.get(0).getRowCount());
    }

    /** Check simple query with DISTINCT constraint. */
    @Test
    @Override public void testQueryWithDistinctAndLowCardinality() throws Exception {
        // Distinct on indexed column with small cardinality.
        execQuery("select DISTINCT K.grp_indexed from K", false);

        assertEquals(5, localResults.size());
        assertEquals(100, localResults.get(4).getRowCount());
    }

    /** Check query failure with DISTINCT constraint. */
    @Test
    @Override public void testQueryWithDistinctAndHighCardinality() throws Exception {
        // Distinct on indexed column with unique values.
        checkQueryExpectOOM("select DISTINCT K.id from K", true);

        assertEquals(5, localResults.size());
        assertFalse(localResults.stream().limit(4).anyMatch(r -> r.memoryReserved() + 1000 > maxMem));
        assertTrue(BIG_TABLE_SIZE > localResults.get(4).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithGroupByThenSort() {
        maxMem = MB / 2;

        // Query failed on map side due too many groups.
        checkQueryExpectOOM("select K.indexed, sum(K.grp) as a from K " +
            "GROUP BY K.indexed ORDER BY a DESC", true);

        // Result on reduce side.
        assertEquals(1, localResults.size());
        assertEquals(0, localResults.get(0).memoryReserved());
        assertEquals(0, localResults.get(0).getRowCount());
    }
}