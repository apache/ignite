
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

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Before;
import org.junit.Test;

import static java.util.function.Predicate.not;

/** */
public class WindowPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /** */
    private final IgniteDistribution affinity = IgniteDistributions.affinity(1, "affinity_tbl", "hash");

    /** */
    private final IgniteDistribution hash = IgniteDistributions.hash(ImmutableIntList.of(0));

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();
        publicSchema = createSchema(
            createTable("RANDOM_TBL", IgniteDistributions.random(),
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER),
            createTable("SINGLE_TBL", IgniteDistributions.single(),
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER),
            createTable("AFFINITY_TBL", affinity,
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER),
            createTable("INDEXED_AFFINITY_TBL", affinity,
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER)
                .addIndex(RelCollations.of(TraitUtils.createFieldCollation(1, false)), "INDEXED_AFFINITY_TBL_IDX"),
            createTable("HASH_TBL", hash,
                "ID", SqlTypeName.INTEGER, "VALUE", SqlTypeName.INTEGER),
            createTable("INDEXED_TBL", IgniteDistributions.single(),
                "ID1", SqlTypeName.INTEGER, "ID2", SqlTypeName.INTEGER, "ID3", SqlTypeName.INTEGER)
                .addIndex("INDEXED_TBL_IDX", 1, 0, 2)
        );
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testSplitWindowRelsByGroup() throws Exception {
        String sql = "SELECT SUM(VALUE) OVER (PARTITION BY ID), ROW_NUMBER() OVER (), " +
            "MIN(VALUE) OVER (PARTITION BY ID) FROM SINGLE_TBL";

        assertPlan(sql, publicSchema,
            hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(IgniteWindow::isStreaming)
                .and(it -> "window(rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])".equals(it.getGroup().toString()))
                .and(hasChildThat(isInstanceOf(IgniteWindow.class)
                    .and(not(IgniteWindow::isStreaming))
                    .and(it -> "window(partition {0} aggs [COUNT($1), SUM($1), MIN($1)])".equals(it.getGroup().toString()))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testProjectWindowConstantsUsedInAggCalls() throws Exception {
        String sql = "SELECT ID, VALUE, " +
            "MAX(2) OVER (PARTITION BY ID ORDER BY VALUE ROWS BETWEEN 10 PRECEDING AND 20 FOLLOWING) " +
            "FROM SINGLE_TBL";

        assertPlan(sql, publicSchema,
            isInstanceOf(IgniteProject.class)
                // The top project should remove constants from the window rel on position 2..4.
                .and(project -> "[$0, $1, $3]".equals(project.getProjects().toString()))
                .and(input(isInstanceOf(IgniteWindow.class)
                    .and(it -> it.getGroup().lowerBound.getOffset() instanceof RexLiteral
                        && it.getGroup().upperBound.getOffset() instanceof RexLiteral)
                    .and(hasChildThat(isInstanceOf(IgniteProject.class)
                        // The bottom project should add agg call constants from the window rel on position 2..4.
                        .and(project -> "[$0, $1, 2]".equals(project.getProjects().toString())))))),
            "ProjectTableScanMergeRule", "ProjectTableScanMergeSkipCorrelatedRule", "ProjectMergeRule", "ProjectWindowTransposeRule");
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testProjectWindowConstantsUnusedInAggCalls() throws Exception {
        String sql = "SELECT ID, VALUE, " +
            "MAX(VALUE) OVER (PARTITION BY ID ORDER BY VALUE ROWS BETWEEN 10 PRECEDING AND 20 FOLLOWING) " +
            "FROM SINGLE_TBL";

        assertPlan(sql, publicSchema,
            isInstanceOf(IgniteWindow.class)
                    .and(it -> it.getGroup().lowerBound.getOffset() instanceof RexLiteral
                        && it.getGroup().upperBound.getOffset() instanceof RexLiteral)
                    .and(not(hasChildThat(isInstanceOf(IgniteProject.class)))),
            "ProjectTableScanMergeRule", "ProjectTableScanMergeSkipCorrelatedRule", "ProjectMergeRule", "ProjectWindowTransposeRule");
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testProjectWindowTranspose() throws Exception {
        String sql = "SELECT MAX(VALUE) OVER (ORDER BY VALUE ROWS BETWEEN VALUE PRECEDING AND 10 FOLLOWING) FROM SINGLE_TBL";

        assertPlan(sql, publicSchema,
            hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasChildThat(isInstanceOf(IgniteProject.class)
                    // ProjectWindowTransposeRule should eliminate constant (10) in projection.
                    .and(project -> project.getProjects().stream().noneMatch(it -> it instanceof RexLiteral))))),
            "ProjectTableScanMergeRule", "ProjectTableScanMergeSkipCorrelatedRule");
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testOrderByWithWindow() throws Exception {
        String sql = "SELECT MAX(VALUE) OVER (PARTITION BY ID) FROM RANDOM_TBL ORDER BY ID DESC, VALUE";

        RelCollation derivedCollation = RelCollations.of(
            TraitUtils.createFieldCollation(0, false),
            TraitUtils.createFieldCollation(1, true)
        );
        assertPlan(sql, publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
                .and(window -> window.collation().equals(derivedCollation))
                .and(input(isInstanceOf(IgniteExchange.class)))
                .and(exchange -> exchange.collation().equals(derivedCollation))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testHashTableAndWindow() throws Exception {
        String sql = "SELECT FIRST_VALUE(VALUE) OVER (PARTITION BY ID) FROM HASH_TBL";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasDistribution(hash)))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testHashTableWithAnotherDistAndWindow() throws Exception {
        String sql = "SELECT FIRST_VALUE(VALUE) OVER (PARTITION BY VALUE) FROM HASH_TBL";

        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteWindow.class)
            .and(hasDistribution(IgniteDistributions.single())))
            .and(hasChildThat(isInstanceOf(IgniteExchange.class))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testIndexedAffinityTableAndWindow() throws Exception {
        String sql = "SELECT MAX(ID) OVER (PARTITION BY VALUE) FROM (SELECT * FROM INDEXED_AFFINITY_TBL) S";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(input(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasDistribution(affinity))
                .and(input(isIndexScan("INDEXED_AFFINITY_TBL", "INDEXED_AFFINITY_TBL_IDX")))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testCorrelatedSubqueryWithWindow() throws Exception {
        String sql = "SELECT ID, (SELECT row_number() OVER (ORDER BY ID)) FROM RANDOM_TBL i1 ORDER BY ID";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(input(0, nodeOrAnyChild(isTableScan("random_tbl"))))
            .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteWindow.class))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testWindowCollationAndCompatibleTableIndex() throws Exception {
        String sql = "SELECT row_number() OVER (PARTITION BY ID1, ID2 ORDER BY ID3) FROM INDEXED_TBL";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(input(isIndexScan("INDEXED_TBL", "INDEXED_TBL_IDX")))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testWindowCollationAndIncompatibleTableIndex() throws Exception {
        String sql = "SELECT row_number() OVER (PARTITION BY ID3, ID2 ORDER BY ID1) FROM INDEXED_TBL";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(input(not(isIndexScan("INDEXED_TBL", "INDEXED_TBL_IDX"))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testPassThroughCollationWiderThanInputRow() throws Exception {
        String sql = "SELECT ID, VALUE, row_number() OVER (ORDER BY ID) FROM RANDOM_TBL ORDER BY 1, 3, 2";

        RelCollation sortCollation = TraitUtils.createCollation(F.asList(0, 2, 1));

        RelCollation derivedCollation = TraitUtils.createCollation(F.asList(0));

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteSort.class)
            .and(it -> it.collation().equals(sortCollation))
            .and(input(isInstanceOf(IgniteWindow.class)
                .and(it -> it.collation().equals(derivedCollation))
                .and(input(nodeOrAnyChild(isInstanceOf(IgniteSort.class)
                    .and(it -> it.collation().equals(derivedCollation)))))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testDeriveAffinityDistribution() throws Exception {
        String sql = "SELECT row_number() OVER (PARTITION BY VALUE ORDER BY ID) FROM AFFINITY_TBL";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(it -> it.distribution().equals(affinity))
            .and(input(isInstanceOf(IgniteSort.class)
                .and(it -> it.distribution().equals(affinity))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testConstantsInPartitionByAndOrderBy() throws Exception {
        String sql = "SELECT MAX(VALUE) OVER (PARTITION BY 1 ORDER BY 2) FROM AFFINITY_TBL";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(it -> it.getGroup().keys.isEmpty()
                && it.collation().getKeys().isEmpty())
            .and(not(input(nodeOrAnyChild(isInstanceOf(IgniteProject.class)))))));
    }
}
