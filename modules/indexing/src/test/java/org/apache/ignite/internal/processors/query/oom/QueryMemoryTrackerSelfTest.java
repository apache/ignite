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
import javax.cache.CacheException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Query memory manager test for distributed queries.
 */
public class QueryMemoryTrackerSelfTest extends AbstractQueryMemoryTrackerSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testUnionOfSmallDataSetsWithLargeResult() {
        maxMem = 2 * MB;

        // OOM on reducer.
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 1 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 2 AND T2.id < 3", false);

        assertEquals(5, localResults.size());

        // Map side
        assertTrue(maxMem > localResults.get(0).memoryReserved() + localResults.get(1).memoryReserved());
        assertEquals(1000, localResults.get(0).getRowCount());
        assertEquals(1000, localResults.get(1).getRowCount());

        // Reduce side
        assertTrue(maxMem > localResults.get(3).memoryReserved() + localResults.get(4).memoryReserved());
        assertEquals(1000, localResults.get(3).getRowCount());
        assertEquals(1000, localResults.get(4).getRowCount());
        assertTrue(2000 > localResults.get(2).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithLimit() throws Exception {
        execQuery("select * from K LIMIT 500", false);

        assertEquals(2, localResults.size());

        // Map
        assertEquals(500, localResults.get(0).getRowCount());

        //Reduce
        assertEquals(500, localResults.get(1).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithHighLimit() {
        // OOM on reducer.
        checkQueryExpectOOM("select * from K LIMIT 8000", true);

        // Reduce only
        assertEquals(1, localResults.size());
        assertTrue(8000 > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithSortByIndexedCol() {
        // OOM on reducer.
        checkQueryExpectOOM("select * from K ORDER BY K.indexed", true);

        // Reduce only.
        assertEquals(1, localResults.size());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
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

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithDistinctAndGroupBy() {
        checkQueryExpectOOM("select DISTINCT K.name from K GROUP BY K.id", true);

        // Local result is quite small.
        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupsSmallResult() throws Exception {
        execQuery("select K.grp, avg(K.id), min(K.id), sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.

        assertEquals(2, localResults.size());
        //Map
        assertEquals(100, localResults.get(0).getRowCount());
        // Reduce
        assertEquals(100, localResults.get(1).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupThenSort() throws Exception {
        // Tiny local result with sorting.
        execQuery("select K.grp_indexed, sum(K.id) as s from K GROUP BY K.grp_indexed ORDER BY s", false);

        assertEquals(2, localResults.size());
        // Map
        assertEquals(100, localResults.get(0).getRowCount());
        // Reduce
        assertEquals(100, localResults.get(1).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupByIndexedCol() {
        // OOM on reducer.
        checkQueryExpectOOM("select K.indexed, sum(K.grp) from K GROUP BY K.indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithGroupByPrimaryKey() {
        // OOM on reducer.
        checkQueryExpectOOM("select K.indexed, sum(K.grp) from K GROUP BY K.indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** Check simple query with DISTINCT constraint. */
    @Test
    @Override public void testQueryWithDistinctAndLowCardinality() throws Exception {
        // Distinct on indexed column with small cardinality.
        execQuery("select DISTINCT K.grp_indexed from K", false);

        assertEquals(2, localResults.size());
        assertEquals(100, localResults.get(0).getRowCount());
        assertEquals(100, localResults.get(1).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLazyQueryWithGroupByIndexedColAndDistinctAggregates() {
        // OOM on reducer.
        checkQueryExpectOOM("select K.grp_indexed, count(DISTINCT k.name) from K GROUP BY K.grp_indexed", true);

        assertEquals(1, localResults.size());
        assertTrue(maxMem > localResults.get(0).memoryReserved());
        assertTrue(BIG_TABLE_SIZE > localResults.get(0).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testGlobalQuota() throws Exception {
        final List<QueryCursor> cursors = new ArrayList<>();

        try {
            CacheException ex = (CacheException)GridTestUtils.assertThrows(log, () -> {
                for (int i = 0; i < 100; i++) {
                    QueryCursor<List<?>> cur = query("select DISTINCT T.name, T.id from T ORDER BY T.name",
                        true);

                    cursors.add(cur);

                    Iterator<List<?>> iter = cur.iterator();
                    iter.next();
                }

                return null;
            }, CacheException.class, "SQL query run out of memory: Global quota exceeded.");

            IgniteSQLException sqlEx = X.cause(ex, IgniteSQLException.class);

            assertNotNull("SQL exception missed.", sqlEx);
            assertEquals(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, sqlEx.statusCode());
            assertEquals(IgniteQueryErrorCode.codeToSqlState(IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY), sqlEx.sqlState());

            assertEquals(61, localResults.size());
            assertEquals(21, cursors.size());

            IgniteH2Indexing h2 = (IgniteH2Indexing)grid(1).context().query().getIndexing();

            long globalAllocated = h2.memoryManager().memoryReserved();

            assertTrue(h2.memoryManager().maxMemory() < globalAllocated + MB);
        }
        finally {
            for (QueryCursor c : cursors)
                IgniteUtils.closeQuiet(c);
        }
    }
}