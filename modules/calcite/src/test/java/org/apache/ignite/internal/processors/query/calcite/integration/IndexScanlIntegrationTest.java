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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Index scan test.
 */
public class IndexScanlIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** */
    @Test
    public void testNullsInCNLJSearchRow() {
        executeSql("CREATE TABLE t(i1 INTEGER, i2 INTEGER) WITH TEMPLATE=REPLICATED ");
        executeSql("INSERT INTO t VALUES (0, null), (1, null), (2, 2), (3, null), (4, null), (null, 5)");
        executeSql("CREATE INDEX t_idx ON t(i1)");

        IgniteTable tbl = (IgniteTable)queryProcessor(grid(0)).schemaHolder().schema("PUBLIC").getTable("T");

        RowCountingIndex idx = new RowCountingIndex(tbl.getIndex("T_IDX"));

        tbl.addIndex(idx);

        String sql = "SELECT /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter') */ t1.i1, t2.i1 " +
            "FROM t t1 " +
            "LEFT JOIN t t2 ON t1.i2 = t2.i1";

        assertQuery(sql)
            .matches(QueryChecker.containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .returns(0, null)
            .returns(1, null)
            .returns(2, 2)
            .returns(3, null)
            .returns(4, null)
            .returns(null, null)
            .check();

        // There shouldn't be full index scan in case of null values in search row, only one value must be found by
        // range scan and passed to predicate.
        assertEquals(1, idx.rowsProcessed());
    }

    /** */
    @Test
    public void testNullsInSearchRow() {
        executeSql("CREATE TABLE t(i1 INTEGER, i2 INTEGER) WITH TEMPLATE=REPLICATED ");
        executeSql("INSERT INTO t VALUES (null, 0), (1, null), (2, 2), (3, null)");
        executeSql("CREATE INDEX t_idx ON t(i1, i2)");

        IgniteTable tbl = (IgniteTable)queryProcessor(grid(0)).schemaHolder().schema("PUBLIC").getTable("T");

        RowCountingIndex idx = new RowCountingIndex(tbl.getIndex("T_IDX"));

        tbl.addIndex(idx);

        assertQuery("SELECT * FROM t WHERE i1 = ?")
            .withParams(new Object[] { null })
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .check();

        assertEquals(0, idx.rowsProcessed());

        assertQuery("SELECT * FROM t WHERE i1 = 1 AND i2 = ?")
            .withParams(new Object[] { null })
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .check();

        // Multi ranges.
        assertQuery("SELECT * FROM t WHERE i1 IN (1, 2, 3) AND i2 = ?")
            .withParams(new Object[] { null })
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .check();

        assertEquals(0, idx.rowsProcessed());

        assertQuery("SELECT * FROM t WHERE i1 IN (1, 2) AND i2 IS NULL")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .returns(1, null)
            .check();

        assertEquals(1, idx.rowsProcessed());
    }

    /** */
    @Test
    public void testSegmentedIndexes() {
        IgniteCache<Integer, Employer> emp = client.getOrCreateCache(new CacheConfiguration<Integer, Employer>("emp")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class).setTableName("emp")))
            .setQueryParallelism(10)
        );

        executeSql("CREATE INDEX idx1 ON emp(salary)");
        executeSql("CREATE INDEX idx2 ON emp(name DESC)");

        for (int i = 0; i < 100; i++)
            emp.put(i, new Employer("emp" + i, (double)i));

        assertQuery("SELECT name FROM emp WHERE salary BETWEEN 50 AND 55 ORDER BY salary")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "EMP", "IDX1"))
            .ordered()
            .returns("emp50")
            .returns("emp51")
            .returns("emp52")
            .returns("emp53")
            .returns("emp54")
            .returns("emp55")
            .check();

        assertQuery("SELECT name FROM emp WHERE name BETWEEN 'emp60' AND 'emp65' ORDER BY name DESC")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "EMP", "IDX2"))
            .ordered()
            .returns("emp65")
            .returns("emp64")
            .returns("emp63")
            .returns("emp62")
            .returns("emp61")
            .returns("emp60")
            .check();
    }

    /** */
    @Test
    public void testScanBooleanField() {
        executeSql("CREATE TABLE t(i INTEGER, b BOOLEAN)");
        executeSql("INSERT INTO t VALUES (0, TRUE), (1, TRUE), (2, FALSE), (3, FALSE), (4, null)");
        executeSql("CREATE INDEX t_idx ON t(b)");

        assertQuery("SELECT i FROM t WHERE b = TRUE")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .returns(0)
            .returns(1)
            .check();

        assertQuery("SELECT i FROM t WHERE b = FALSE")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .returns(2)
            .returns(3)
            .check();

        assertQuery("SELECT i FROM t WHERE b IS TRUE")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .returns(0)
            .returns(1)
            .check();

        assertQuery("SELECT i FROM t WHERE b IS FALSE")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .returns(2)
            .returns(3)
            .check();

        // Support index scans for IS TRUE, IS FALSE but not for IS NOT TRUE, IS NOT FALSE, since it requeres multi
        // bounds scan and may not be effective.
        assertQuery("SELECT i FROM t WHERE b IS NOT TRUE")
            .matches(QueryChecker.containsTableScan("PUBLIC", "T"))
            .returns(2)
            .returns(3)
            .returns(4)
            .check();

        assertQuery("SELECT i FROM t WHERE b IS NOT FALSE")
            .matches(QueryChecker.containsTableScan("PUBLIC", "T"))
            .returns(0)
            .returns(1)
            .returns(4)
            .check();

        assertQuery("SELECT i FROM t WHERE b IS NULL")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX"))
            .returns(4)
            .check();
    }

    /** */
    private static class RowCountingIndex extends DelegatingIgniteIndex {
        /** */
        private final AtomicInteger filteredRows = new AtomicInteger();

        /** */
        public RowCountingIndex(IgniteIndex delegate) {
            super(delegate);
        }

        /** {@inheritDoc} */
        @Override public <Row> Iterable<Row> scan(
            ExecutionContext<Row> execCtx,
            ColocationGroup grp,
            Predicate<Row> filters,
            RangeIterable<Row> ranges,
            Function<Row, Row> rowTransformer,
            @Nullable ImmutableBitSet requiredColumns
        ) {
            Predicate<Row> filter = row -> {
                filteredRows.incrementAndGet();

                return true;
            };

            filters = filter.and(filters);

            return delegate.scan(execCtx, grp, filters, ranges, rowTransformer, requiredColumns);
        }

        /** */
        public int rowsProcessed() {
            return filteredRows.getAndSet(0);
        }
    }
}
