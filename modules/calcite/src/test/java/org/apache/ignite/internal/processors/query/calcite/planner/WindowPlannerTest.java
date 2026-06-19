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

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.function.Predicate.not;
import static org.apache.calcite.rex.RexWindowBounds.CURRENT_ROW;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING;

/** */
public class WindowPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception if failed
     */
    @Test
    public void testSplitWindowRelsByGroup() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER,
                "C", SqlTypeName.INTEGER, "D", SqlTypeName.INTEGER)
        );

        String sql = "SELECT " +
            "SUM(d) OVER (PARTITION BY a, b ORDER BY c, d), " +
            "ROW_NUMBER() OVER (), " +
            "MIN(a) OVER (PARTITION BY a, b ORDER BY c, d), " +
            "MAX(a) OVER (PARTITION BY a, b ORDER BY c, d RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)" +
            "FROM TBL";
        assertPlan(sql, schema,
            nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
                .and(isWindow(F.asList(0, 1), F.asList(0, 1, 2, 3), CURRENT_ROW, UNBOUNDED_FOLLOWING,
                    SqlStdOperatorTable.MAX)))
                .and(nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
                    .and(IgniteWindow::isStreaming)
                    .and(isWindow(F.asList(), F.asList(0, 1, 2, 3), UNBOUNDED_PRECEDING, CURRENT_ROW, SqlStdOperatorTable.ROW_NUMBER))))
                .and(nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
                    .and(not(IgniteWindow::isStreaming))
                    .and(isWindow(F.asList(0, 1), F.asList(0, 1, 2, 3), UNBOUNDED_PRECEDING, CURRENT_ROW,
                        SqlStdOperatorTable.COUNT, SqlStdOperatorTable.SUM, SqlStdOperatorTable.MIN)))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testMergeWindowRelsByGroup() throws Exception {
        // Different agg calls but with the same window definition.
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
        );

        String sql = "SELECT " +
            "SUM(a) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), " +
            "MIN(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) " +
            "FROM TBL";
        assertPlan(sql, schema,
            hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasChildThat(isInstanceOf(IgniteWindow.class)).negate())));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testProjectWindowConstantsUsedInAggCalls() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
        );

        String sql = "SELECT a, b, " +
            "MAX(2) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 10 PRECEDING AND 20 FOLLOWING) " +
            "FROM TBL";
        assertPlan(sql, schema,
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
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
        );

        String sql = "SELECT a, b, " +
            "MAX(a) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 10 PRECEDING AND 20 FOLLOWING) " +
            "FROM TBL";
        assertPlan(sql, schema,
            isInstanceOf(IgniteWindow.class)
                    .and(it -> it.getGroup().lowerBound.getOffset() instanceof RexLiteral
                        && it.getGroup().upperBound.getOffset() instanceof RexLiteral)
                    .and(not(hasChildThat(isInstanceOf(IgniteProject.class)))),
            "ProjectTableScanMergeRule", "ProjectMergeRule", "ProjectWindowTransposeRule");
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testPassThroughOrderByWithWindow() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.random(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER, "C", SqlTypeName.INTEGER)
        );

        RelCollation derivedCollation = RelCollations.of(
            TraitUtils.createFieldCollation(0, false),
            TraitUtils.createFieldCollation(1, true)
        );

        String sql = "SELECT MAX(c) OVER (PARTITION BY a ORDER BY b) FROM tbl ORDER BY a DESC, b";
        assertPlan(sql, schema,
            nodeOrAnyChild(isInstanceOf(IgniteSort.class).negate())
                .and(hasChildThat(isInstanceOf(IgniteWindow.class)
                    .and(window -> window.collation().equals(derivedCollation))
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(exchange -> exchange.collation().equals(derivedCollation)))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testNotPassThroughOrderByWithWindow() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.random(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER, "C", SqlTypeName.INTEGER)
        );

        RelCollation windowCollation = RelCollations.of(
            TraitUtils.createFieldCollation(0, true),
            TraitUtils.createFieldCollation(1, false)
        );

        Predicate<RelNode> predicate = nodeOrAnyChild(isInstanceOf(IgniteSort.class)
            .and(not(sort -> sort.collation().equals(windowCollation)))
            .and(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(window -> window.collation().equals(windowCollation))
                .and(hasChildThat(isInstanceOf(IgniteSort.class)
                    .and(sort -> sort.collation().equals(windowCollation)))))));

        String sql = "SELECT MAX(b) OVER (PARTITION BY a ORDER BY b DESC) FROM tbl ORDER BY a, c, b DESC";
        assertPlan(sql, schema, predicate);

        sql = "SELECT MAX(b) OVER (PARTITION BY a ORDER BY b DESC) FROM tbl ORDER BY a, c";
        assertPlan(sql, schema, predicate);

        sql = "SELECT MAX(b) OVER (PARTITION BY a ORDER BY b DESC) FROM tbl ORDER BY a, b";
        assertPlan(sql, schema, predicate);
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testHashTableDistributionAndWindow() throws Exception {
        IgniteDistribution hash = IgniteDistributions.hash(ImmutableIntList.of(0));

        RelCollation collation = RelCollations.of(
            TraitUtils.createFieldCollation(1, false),
            TraitUtils.createFieldCollation(0, false)
        );

        IgniteSchema schema = createSchema(
            createTable("TBL", hash,
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
                .addIndex(collation, "TBL_IDX")
        );

        checkDistributionAndCollationDeriviation(schema, hash, collation);
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testAffinityTableDistributionAndWindow() throws Exception {
        IgniteDistribution aff = IgniteDistributions.affinity(0, "affinity_tbl", "hash");

        RelCollation collation = RelCollations.of(
            TraitUtils.createFieldCollation(1, false),
            TraitUtils.createFieldCollation(0, false)
        );

        IgniteSchema schema = createSchema(
            createTable("TBL", aff,
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
                .addIndex(collation, "TBL_IDX")
        );

        checkDistributionAndCollationDeriviation(schema, aff, collation);
    }

    /** */
    private void checkDistributionAndCollationDeriviation(IgniteSchema schema, IgniteDistribution dist,
        RelCollation collation) throws Exception {
        // Both collation and distribution derivation.
        String sql = "SELECT FIRST_VALUE(b) OVER (PARTITION BY a, b) FROM tbl";
        assertPlan(sql, schema, isInstanceOf(IgniteExchange.class)
            .and(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(hasDistribution(dist))
                .and(window -> window.collation().equals(collation))
                .and(hasChildThat(isIndexScan("TBL", "TBL_IDX"))))));

        // Collation only deriviation.
        sql = "SELECT FIRST_VALUE(b) OVER (PARTITION BY b ORDER BY a DESC) FROM tbl";
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(window -> window.collation().equals(collation))
            .and(hasChildThat(isInstanceOf(IgniteExchange.class)))));

        // Distribution only deriviation.
        sql = "SELECT FIRST_VALUE(b) OVER (PARTITION BY a ORDER BY a DESC) FROM tbl";
        assertPlan(sql, schema, isInstanceOf(IgniteExchange.class)
            .and(hasChildThat(isInstanceOf(IgniteWindow.class))
                .and(hasChildThat(isInstanceOf(IgniteSort.class)))));

        // None deriviation.
        sql = "SELECT FIRST_VALUE(b) OVER (PARTITION BY b ORDER BY a) FROM tbl";
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(hasChildThat(isInstanceOf(IgniteExchange.class)))
            .and(hasChildThat(isInstanceOf(IgniteSort.class)))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testHashTableAndWindowWithAnotherDistribution() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.hash(ImmutableIntList.of(0)),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
        );

        String sql = "SELECT FIRST_VALUE(a) OVER (PARTITION BY b) FROM tbl";
        assertPlan(sql, schema, hasChildThat(isInstanceOf(IgniteWindow.class)
            .and(hasDistribution(IgniteDistributions.single())))
            .and(hasChildThat(isInstanceOf(IgniteExchange.class))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testIndexedTableAndWindow() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER, "C", SqlTypeName.INTEGER)
                .addIndex("TBL_IDX", 1, 0, 2)
        );

        // Should derive collation (can use index scan).
        String sql = "SELECT row_number() OVER (PARTITION BY a, b ORDER BY c) FROM tbl";
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(input(isIndexScan("TBL", "TBL_IDX")))));

        // Should not derive collation (cannot use index scan).
        sql = "SELECT row_number() OVER (PARTITION BY a, c) FROM tbl";
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(input(isIndexScan("TBL", "TBL_IDX")).negate())));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testIndexedAffinityTableAndWindow() throws Exception {
        IgniteDistribution aff = IgniteDistributions.affinity(0, "tbl", "hash");
        RelCollation collation = TraitUtils.createCollation(F.asList(0, 1, 2));

        IgniteSchema schema = createSchema(
            createTable("TBL", aff,
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER, "C", SqlTypeName.INTEGER)
                .addIndex(collation, "TBL_IDX")
        );

        // Affinity distribution and index collation deriviation.
        // Table index cover more columns than partition/order by,
        // table index has different directions than partition fields.
        String sql = "SELECT MAX(c) OVER (PARTITION BY a ORDER BY b) FROM (SELECT * FROM tbl) S";
        assertPlan(sql, schema, isInstanceOf(IgniteExchange.class)
            .and(input(hasChildThat(isInstanceOf(IgniteWindow.class)
                .and(it -> it.collation().equals(collation))
                .and(hasDistribution(aff))
                .and(input(isIndexScan("TBL", "TBL_IDX")))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testCorrelatedSubqueryWithWindow() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
        );

        String sql = "SELECT a, (SELECT row_number() OVER (ORDER BY a)) FROM tbl ORDER BY a";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(input(0, nodeOrAnyChild(isTableScan("TBL"))))
            .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteWindow.class))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testPassThroughCollationWiderThanInputRow() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER, "B", SqlTypeName.INTEGER)
        );

        String sql = "SELECT a, b, row_number() OVER (ORDER BY a) FROM tbl ORDER BY 1, 3, 2";

        RelCollation sortCollation = TraitUtils.createCollation(F.asList(0, 2, 1));
        RelCollation windowCollation = TraitUtils.createCollation(F.asList(0));

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteSort.class)
            .and(it -> it.collation().equals(sortCollation))
            .and(input(isInstanceOf(IgniteWindow.class)
                .and(it -> it.collation().equals(windowCollation))
                .and(input(nodeOrAnyChild(isInstanceOf(IgniteSort.class)
                    .and(it -> it.collation().equals(windowCollation)))))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testConstantsInPartitionByAndOrderBy() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TBL", IgniteDistributions.single(),
                "A", SqlTypeName.INTEGER)
        );

        String sql = "SELECT MAX(a) OVER (PARTITION BY 1 ORDER BY 2) FROM tbl";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteWindow.class)
            .and(it -> it.getGroup().keys.isEmpty()
                && it.collation().getKeys().isEmpty())
            .and(not(input(nodeOrAnyChild(isInstanceOf(IgniteProject.class)))))));
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testCorrelatedDistribution() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TA1", IgniteDistributions.affinity(0, "cache1", "hash"),
                "A", Integer.class, "B", Integer.class, "C", Integer.class),
            createTable("TA2", IgniteDistributions.affinity(1, "cache2", "hash"),
                "A", Integer.class, "B", Integer.class, "C", Integer.class),
            createTable("TH1", IgniteDistributions.hash(Arrays.asList(0, 1)),
                "A", Integer.class, "B", Integer.class, "C", Integer.class),
            createTable("TH2", IgniteDistributions.hash(Arrays.asList(1, 2)),
                "A", Integer.class, "B", Integer.class, "C", Integer.class)
        );

        Predicate<RelNode> colocatedPredicate = nodeOrAnyChild(isInstanceOf(IgniteColocatedAggregateBase.class)
            .and(hasChildThat(isInstanceOf(IgniteExchange.class)).negate())
            .and(hasChildThat(isInstanceOf(IgniteWindow.class))
                .and(window -> TraitUtils.correlation(window).correlated())));

        // Affinity distribution and window with one correlated variable.
        String sql = "SELECT a FROM ta1 WHERE EXISTS (SELECT s.a FROM ( " +
            "SELECT a, ROW_NUMBER() OVER ( PARTITION BY ta1.a ORDER BY ta2.b ) rn " +
            "FROM ta2 WHERE ta2.b = ta1.a) s WHERE rn > 1 )";
        assertPlan(sql, schema, colocatedPredicate);

        // Hash distribution on two columns and window without correlated variables.
        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT s.a FROM ( " +
            "SELECT a, ROW_NUMBER() OVER ( PARTITION BY th2.a ORDER BY th2.b ) rn " +
            "FROM th2 WHERE th2.b = th1.a AND th2.c = th1.b) s WHERE rn > 1 )";
        assertPlan(sql, schema, colocatedPredicate);

        // Hash distribution on two columns and window with two correlated variables.
        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT s.a FROM ( " +
            "SELECT a, ROW_NUMBER() OVER ( PARTITION BY th1.a ORDER BY th1.b ) rn " +
            "FROM th2 WHERE th2.b = th1.a AND th2.c = th1.b) s WHERE rn > 1 )";
        assertPlan(sql, schema, colocatedPredicate);
    }

    /** */
    private Predicate<IgniteWindow> isWindow(List<Integer> keys, @Nullable List<Integer> collation,
        RexWindowBound lower, RexWindowBound upper, SqlAggFunction... functions) {
        return isInstanceOf(IgniteWindow.class)
            .and(window -> ImmutableBitSet.of(keys).equals(window.getGroup().keys))
            .and(window -> TraitUtils.createCollation(collation).equals(window.collation()))
            .and(window -> lower.equals(window.getGroup().lowerBound))
            .and(window -> upper.equals(window.getGroup().upperBound))
            .and(window -> F.asList(functions).equals(
                Commons.transform(window.getGroup().aggCalls, Window.RexWinAggCall::getOperator)));
    }
}
