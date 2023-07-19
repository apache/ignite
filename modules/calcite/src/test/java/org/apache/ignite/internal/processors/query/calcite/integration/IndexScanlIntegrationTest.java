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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Index scan test.
 */
public class IndexScanlIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int ROWS_CNT = 100;

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

        RowCountingIndex idx = injectRowCountingIndex(grid(0), "T", "T_IDX");

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

        RowCountingIndex idx = injectRowCountingIndex(grid(0), "T", "T_IDX");

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
    @Test
    public void testIsNotDistinctFrom() {
        executeSql("CREATE TABLE t1(i1 INTEGER) WITH TEMPLATE=REPLICATED");
        executeSql("INSERT INTO t1 VALUES (1), (2), (null), (3)");

        executeSql("CREATE TABLE t2(i2 INTEGER, i3 INTEGER) WITH TEMPLATE=REPLICATED");
        executeSql("INSERT INTO t2 VALUES (1, 1), (2, 2), (null, 3), (4, null)");
        executeSql("CREATE INDEX t2_idx ON t2(i2)");

        String sql = "SELECT /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter') */ i1, i3 " +
            "FROM t1 JOIN t2 ON i1 IS NOT DISTINCT FROM i2";

        assertQuery(sql)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T2", "T2_IDX"))
            .returns(1, 1)
            .returns(2, 2)
            .returns(null, 3)
            .check();

        // Collapse expanded IS_NOT_DISTINCT_FROM.
        sql = "SELECT /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter') */ i1, i3 " +
            "FROM t1 JOIN t2 ON i1 = i2 OR (i1 IS NULL AND i2 IS NULL)";

        assertQuery(sql)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "T2", "T2_IDX"))
            .returns(1, 1)
            .returns(2, 2)
            .returns(null, 3)
            .check();
    }

    /** */
    @Test
    public void testInlineScan() {
        // Single column scans.
        checkSingleColumnInlineScan(true, "INTEGER", i -> i);
        checkSingleColumnInlineScan(true, "DOUBLE", i -> (double)i);
        checkSingleColumnInlineScan(true, "UUID", i -> new UUID(0, i));
        checkSingleColumnInlineScan(true, "TIMESTAMP",
            i -> new Timestamp(Timestamp.valueOf("2022-01-01 00:00:00").getTime() + TimeUnit.SECONDS.toMillis(i)));
        checkSingleColumnInlineScan(true, "DATE",
            i -> new Date(Date.valueOf("2022-01-01").getTime() + TimeUnit.DAYS.toMillis(i)));
        checkSingleColumnInlineScan(true, "TIME",
            i -> new Time(Time.valueOf("00:00:00").getTime() + TimeUnit.SECONDS.toMillis(i)));
        checkSingleColumnInlineScan(false, "VARCHAR", i -> "str" + i);
        checkSingleColumnInlineScan(false, "DECIMAL", BigDecimal::valueOf);

        // Multi columns scans.
        executeSql("CREATE TABLE t(id INTEGER PRIMARY KEY, i1 INTEGER, i2 INTEGER, i3 INTEGER)");
        executeSql("CREATE INDEX t_idx ON t(i1, i3)");
        RowCountingIndex idx = injectRowCountingIndex(grid(0), "T", "T_IDX");

        for (int i = 0; i < ROWS_CNT; i++)
            executeSql("INSERT INTO t VALUES (?, ?, ?, ?)", i, i * 2, i * 3, i * 4);

        checkMultiColumnsInlineScan(true, "SELECT i1, i3 FROM t", idx, i -> new Object[] {i * 2, i * 4});
        checkMultiColumnsInlineScan(false, "SELECT i1, i2 FROM t", idx, i -> new Object[] {i * 2, i * 3});
        checkMultiColumnsInlineScan(true, "SELECT sum(i1), i3 FROM t GROUP BY i3", idx,
            i -> new Object[] {(long)i * 2, i * 4});
    }

    /** */
    public void checkSingleColumnInlineScan(boolean expInline, String dataType, IntFunction<Object> valFactory) {
        executeSql("CREATE TABLE t(id INTEGER PRIMARY KEY, val " + dataType + ')');

        try {
            executeSql("CREATE INDEX t_idx ON t(val)");
            RowCountingIndex idx = injectRowCountingIndex(grid(0), "T", "T_IDX");

            for (int i = 0; i < ROWS_CNT; i++)
                executeSql("INSERT INTO t VALUES (?, ?)", i, valFactory.apply(i));

            QueryChecker checker = assertQuery("SELECT val FROM t");

            for (int i = 0; i < ROWS_CNT; i++)
                checker.returns(valFactory.apply(i));

            if (expInline) {
                checker.matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX")).check();

                assertEquals(ROWS_CNT, idx.rowsProcessed());
                assertTrue(idx.isInlineScan());
            }
            else {
                checker.check();

                assertFalse(idx.isInlineScan());
            }
        }
        finally {
            executeSql("DROP TABLE t");
        }
    }

    /** */
    public void checkMultiColumnsInlineScan(
        boolean expInline,
        String sql,
        RowCountingIndex idx,
        IntFunction<Object[]> rowFactory
    ) {
        QueryChecker checker = assertQuery(sql);

        for (int i = 0; i < ROWS_CNT; i++)
            checker.returns(rowFactory.apply(i));

        if (expInline) {
            checker.matches(QueryChecker.containsIndexScan("PUBLIC", "T", "T_IDX")).check();

            assertEquals(ROWS_CNT, idx.rowsProcessed());
            assertTrue(idx.isInlineScan());
        }
        else {
            checker.check();

            assertFalse(idx.isInlineScan());
        }
    }

    /** */
    private RowCountingIndex injectRowCountingIndex(IgniteEx node, String tableName, String idxName) {
        RowCountingIndex idx = null;

        for (Ignite ignite : G.allGrids()) {
            IgniteTable tbl = (IgniteTable)queryProcessor((IgniteEx)ignite).schemaHolder().schema("PUBLIC").getTable(tableName);

            if (ignite == node) {
                idx = new RowCountingIndex(tbl.getIndex(idxName));

                tbl.addIndex(idx);
            }

            tbl.removeIndex(SchemaManager.generateProxyIdxName(idxName));
        }

        return idx;
    }

    /** */
    private static class RowCountingIndex extends DelegatingIgniteIndex {
        /** */
        private final AtomicInteger filteredRows = new AtomicInteger();

        /** */
        private final AtomicBoolean isInlineScan = new AtomicBoolean();

        /** */
        public RowCountingIndex(IgniteIndex delegate) {
            super(delegate);
        }

        /** {@inheritDoc} */
        @Override public <Row> Iterable<Row> scan(
            ExecutionContext<Row> execCtx,
            ColocationGroup grp,
            RangeIterable<Row> ranges,
            @Nullable ImmutableBitSet requiredColumns
        ) {
            IndexScan<Row> scan = (IndexScan<Row>)delegate.scan(execCtx, grp, ranges, requiredColumns);

            isInlineScan.set(scan.isInlineScan());

            return new Iterable<Row>() {
                @NotNull @Override public Iterator<Row> iterator() {
                    return F.iterator(scan.iterator(), r -> {
                        filteredRows.incrementAndGet();

                        return r;
                    }, true);
                }
            };
        }

        /** */
        public int rowsProcessed() {
            return filteredRows.getAndSet(0);
        }

        /** */
        public boolean isInlineScan() {
            return isInlineScan.getAndSet(false);
        }
    }
}
