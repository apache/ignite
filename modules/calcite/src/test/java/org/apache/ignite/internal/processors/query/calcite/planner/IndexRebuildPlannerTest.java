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

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexBound;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.calcite.sql.SqlKind.COUNT;
import static org.apache.calcite.sql.SqlKind.MAX;
import static org.apache.calcite.sql.SqlKind.MIN;
import static org.apache.calcite.sql.SqlKind.SUM0;

/**
 * Planner test for index rebuild.
 */
public class IndexRebuildPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema publicSchema;

    /** */
    private TestTable tbl;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        tbl = createTable("TBL", 100, IgniteDistributions.single(), "ID", Integer.class, "VAL", String.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0).addIndex("TBL_VAL_IDX", 1);

        publicSchema = createSchema(tbl);
    }

    /** */
    @Test
    public void testIndexRebuild() throws Exception {
        String sql = "SELECT * FROM TBL WHERE id = 0";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteIndexScan.class));

        tbl.markIndexRebuildInProgress(true);

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class));

        tbl.markIndexRebuildInProgress(false);

        assertPlan(sql, publicSchema, isInstanceOf(IgniteIndexScan.class));
    }

    /** Test IndexCount is disabled when index is unavailable. */
    @Test
    public void testIndexCountAtIndexRebuild() throws Exception {
        String sql = "SELECT COUNT(*) FROM TBL";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteAggregate.class)
            .and(a -> a.getAggCallList().stream().filter(agg -> agg.getAggregation().getKind() == SUM0).count() == 1)
            .and(nodeOrAnyChild(isInstanceOf(IgniteIndexCount.class)))));

        tbl.markIndexRebuildInProgress(true);

        assertPlan(sql, publicSchema, isInstanceOf(IgniteAggregate.class)
            .and(a -> a.getAggCallList().stream().filter(agg -> agg.getAggregation().getKind() == COUNT).count() == 1)
            .and(nodeOrAnyChild(isInstanceOf(IgniteTableScan.class))));

        tbl.markIndexRebuildInProgress(false);

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteAggregate.class)
            .and(a -> a.getAggCallList().stream().filter(agg -> agg.getAggregation().getKind() == SUM0).count() == 1)
            .and(nodeOrAnyChild(isInstanceOf(IgniteIndexCount.class)))));
    }

    /** Test Index min/max (first/last) is disabled when index is unavailable. */
    @Test
    public void testIndexMinMaxAtIndexRebuild() throws Exception {
        checkMinMaxOptimized();

        tbl.markIndexRebuildInProgress(true);

        assertPlan("SELECT MIN(VAL) FROM TBL", publicSchema, isInstanceOf(IgniteAggregate.class)
            .and(a -> a.getAggCallList().stream().filter(agg -> agg.getAggregation().getKind() == MIN).count() == 1)
            .and(hasChildThat(isTableScan("TBL"))));

        assertPlan("SELECT MAX(VAL) FROM TBL", publicSchema, isInstanceOf(IgniteAggregate.class)
            .and(a -> a.getAggCallList().stream().filter(agg -> agg.getAggregation().getKind() == MAX).count() == 1)
            .and(hasChildThat(isTableScan("TBL"))));

        tbl.markIndexRebuildInProgress(false);

        checkMinMaxOptimized();
    }

    /** */
    private void checkMinMaxOptimized() throws Exception {
        assertPlan("SELECT MIN(VAL) FROM TBL", publicSchema, isInstanceOf(IgniteAggregate.class)
            .and(a -> a.getAggCallList().stream().filter(agg -> agg.getAggregation().getKind() == MIN).count() == 1)
            .and(hasChildThat(isInstanceOf(IgniteIndexBound.class)
                .and(is -> "TBL_VAL_IDX".equals(is.indexName())))));

        assertPlan("SELECT MAX(VAL) FROM TBL", publicSchema, isInstanceOf(IgniteAggregate.class)
            .and(a -> a.getAggCallList().stream().filter(agg -> agg.getAggregation().getKind() == MAX).count() == 1)
            .and(hasChildThat(isInstanceOf(IgniteIndexBound.class)
                .and(is -> "TBL_VAL_IDX".equals(is.indexName())))));
    }

    /** */
    @Test
    public void testConcurrentIndexRebuildStateChange() throws Exception {
        String sql = "SELECT * FROM TBL WHERE id = 0";

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (!stop.get()) {
                tbl.markIndexRebuildInProgress(true);
                tbl.markIndexRebuildInProgress(false);
            }
        });

        try {
            for (int i = 0; i < 1000; i++) {
                IgniteRel rel = physicalPlan(sql, publicSchema);

                assertTrue(rel instanceof IgniteTableScan || rel instanceof IgniteIndexScan);
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }

    /**
     * Test IndexCount is disabled when index becomes unavailable.
     */
    @Test
    public void testIndexCountAtConcurrentIndexRebuild() throws Exception {
        String sql = "SELECT COUNT(*) FROM TBL";

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            boolean lever = true;

            while (!stop.get())
                tbl.markIndexRebuildInProgress(lever = !lever);
        });

        try {
            for (int i = 0; i < 1000; i++)
                assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                    .and(input(isInstanceOf(IgniteIndexCount.class)).or(input(isInstanceOf(IgniteTableScan.class))))));
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }

    /**
     * Test Index min/max is disabled when index becomes unavailable.
     */
    @Test
    public void testIndexMinMaxAtConcurrentIndexRebuild() throws Exception {
        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            boolean lever = true;

            while (!stop.get())
                tbl.markIndexRebuildInProgress(lever = !lever);
        });

        tbl.markIndexRebuildInProgress(true);

        try {
            for (int i = 0; i < 500; i++) {
                assertPlan("SELECT MIN(VAL) FROM TBL", publicSchema,
                    nodeOrAnyChild(isInstanceOf(IgniteAggregate.class)
                        .and(input(isInstanceOf(IgniteIndexBound.class)
                            .and(is -> "TBL_VAL_IDX".equals(is.indexName())))
                            .or(input(isInstanceOf(IgniteTableScan.class))))));

                assertPlan("SELECT MAX(VAL) FROM TBL", publicSchema,
                    nodeOrAnyChild(isInstanceOf(IgniteAggregate.class)
                        .and(input(isInstanceOf(IgniteIndexBound.class)
                            .and(is -> "TBL_VAL_IDX".equals(is.indexName())))
                            .or(input(isInstanceOf(IgniteTableScan.class))))));
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }
}
