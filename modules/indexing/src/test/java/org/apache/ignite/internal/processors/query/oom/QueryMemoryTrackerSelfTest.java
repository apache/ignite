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

import org.junit.Test;

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
        maxMem = 2 * 1024 * 1024;

        // OOM on reducer.
        checkQueryExpectOOM("select * from T as T0, T as T1 where T0.id < 1 " +
            "UNION " +
            "select * from T as T2, T as T3 where T2.id >= 2 AND T2.id < 3", false);

        assertEquals(5, localResults.size());

        // Map side
        assertTrue(maxMem > localResults.get(0).memoryAllocated() + localResults.get(1).memoryAllocated());
        assertEquals(1000, localResults.get(0).getRowCount());
        assertEquals(1000, localResults.get(1).getRowCount());

        // Reduce side
        assertTrue(maxMem > localResults.get(3).memoryAllocated() + localResults.get(4).memoryAllocated());
        assertEquals(1000, localResults.get(3).getRowCount());
        assertEquals(1000, localResults.get(4).getRowCount());
        assertTrue(2000 > localResults.get(2).getRowCount());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testQueryWithLimit() {
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

    /** Check GROUP BY operation on large data set with small result set. */
    @Test
    @Override public void testQueryWithGroupsSmallResult() {
        execQuery("select K.grp, avg(K.id), min(K.id), sum(K.id) from K GROUP BY K.grp", false); // Tiny local result.

        assertEquals(2, localResults.size());
        // Map
        assertEquals(100, localResults.get(0).getRowCount());
        // Reduce
        assertEquals(100, localResults.get(1).getRowCount());

        localResults.clear();

        // Tiny local result with sorting.
        execQuery("select K.grp_indexed, sum(K.id) as s from K GROUP BY K.grp_indexed ORDER BY s", false);

        assertEquals(2, localResults.size());
        // Map
        assertEquals(100, localResults.get(0).getRowCount());
        // Reduce
        assertEquals(100, localResults.get(1).getRowCount());
    }
}