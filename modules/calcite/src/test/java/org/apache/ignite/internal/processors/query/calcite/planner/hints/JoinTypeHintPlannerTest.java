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

package org.apache.ignite.internal.processors.query.calcite.planner.hints;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.RuleApplyListener;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteMultiJoinOptimizeRule;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.CNL_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.MERGE_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NL_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NO_CNL_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NO_MERGE_JOIN;
import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.NO_NL_JOIN;

/**
 * Planner test for index hints.
 */
public class JoinTypeHintPlannerTest extends AbstractPlannerTest {
    /** All the join order optimization rules. */
    private static final String[] JOIN_REORDER_RULES = Stream.of(
        CoreRules.JOIN_COMMUTE,
        JoinPushThroughJoinRule.LEFT, JoinPushThroughJoinRule.RIGHT,
        CoreRules.JOIN_TO_MULTI_JOIN, IgniteMultiJoinOptimizeRule.INSTANCE
    ).map(RelOptRule::toString).toArray(String[]::new);

    /** */
    private IgniteSchema schema;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ((GridTestLog4jLogger)log).setLevel(Level.INFO);
    }

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        TestTable[] tables = new TestTable[5];

        for (int i = 0; i < tables.length; ++i) {
            tables[i] = createTable("TBL" + (i + 1), (int)Math.min(1_000_000, Math.pow(10, i)),
                IgniteDistributions.broadcast(), "ID", Integer.class, "V1", Integer.class, "V2", Integer.class,
                "V3", Integer.class).addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0);
        }

        schema = createSchema(tables);
    }

    /**
     * Tests correlated nested loop join is disabled by hint.
     */
    @Test
    public void testDisableCNLJoin() throws Exception {
        for (HintDefinition hint : Arrays.asList(NO_CNL_JOIN, NL_JOIN, MERGE_JOIN)) {
            doTestDisableJoinTypeWith("TBL1", "TBL5", "INNER", IgniteCorrelatedNestedLoopJoin.class,
                hint);

            doTestDisableJoinTypeWith("TBL1", "TBL5", "LEFT", IgniteCorrelatedNestedLoopJoin.class,
                hint);
        }
    }

    /** */
    @Test
    public void testHintsErrors() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        // Leading hint must disable inconsistent follower.
        LogListener lsnr = LogListener.matches("Skipped hint '" + CNL_JOIN.name() + "'")
            .andMatches("This join type is already disabled or forced to use before").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + NO_CNL_JOIN + ',' + CNL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN " +
            "TBL2 t2 on t1.v3=t2.v3", schema);

        assertTrue(lsnr.check());

        // Wrong table name must not affect next hint.
        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint '" + MERGE_JOIN.name() + "'").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + NL_JOIN + "(UNEXISTING), " + MERGE_JOIN + "(TBL2) */ t1.v1, t2.v2 FROM " +
            "TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3", schema);

        assertTrue(!lsnr.check());

        // Following hint must not override leading.
        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint '" + NL_JOIN.name() + "'")
            .andMatches("This join type is already disabled or forced to use before").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + MERGE_JOIN + "(TBL1)," + NL_JOIN + "(TBL1,TBL2) */ t1.v1, t2.v2 FROM TBL1 " +
            "t1 JOIN TBL2 t2 on t1.v3=t2.v3", schema);

        assertTrue(lsnr.check());

        // Inner hint must override heading. Second inner hint must not override first inner hint.
        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint '" + CNL_JOIN.name() + "' with options 'TBL1','TBL3'")
            .andMatches("Skipped hint '" + NL_JOIN.name() + "' with options 'TBL1'").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + NL_JOIN + "(TBL1) */ t1.v1, t2.v2 FROM TBL1 " +
            "t1 JOIN TBL2 t2 on t1.v3=t2.v3 where t2.v1 in " +
            "(SELECT /*+ " + MERGE_JOIN + "(TBL1), " + CNL_JOIN + "(TBL1,TBL3) */ t3.v3 from TBL3 t3 JOIN TBL1 t4 " +
            "on t3.v2=t4.v2)", schema, JOIN_REORDER_RULES);

        assertTrue(lsnr.check());

        // The same params should hot be warned.
        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + NL_JOIN + "(TBL1, TBL1, TBL2, TBL2, UNEXISTING) */ t1.v1, t2.v2 FROM TBL1 " +
            "t1 JOIN TBL2 t2 on t1.v3=t2.v3", schema);

        assertTrue(!lsnr.check());

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + MERGE_JOIN + "(TBL1)," + MERGE_JOIN + "(TBL1,TBL2,UNEXISTING) */ t1.v1, " +
            "t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3", schema);

        assertTrue(!lsnr.check());

        // Next check: ensures that joins missing table scan inputs are processed correctly too.
        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint '" + NL_JOIN + '\'').build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + MERGE_JOIN + ',' + NL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 " +
            "on t1.v3=t2.v3 where t2.v1 in (SELECT t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)", schema);

        assertTrue(lsnr.check());
    }

    /**
     * Tests nested loop join is disabled by hints.
     */
    @Test
    public void testDisableNLJoin() throws Exception {
        for (HintDefinition hint : Arrays.asList(NO_NL_JOIN, CNL_JOIN, MERGE_JOIN)) {
            doTestDisableJoinTypeWith("TBL5", "TBL4", "INNER", IgniteNestedLoopJoin.class,
                NO_NL_JOIN, "MergeJoinConverter");

            doTestDisableJoinTypeWith("TBL3", "TBL1", "LEFT", IgniteNestedLoopJoin.class,
                NO_NL_JOIN);

            // Correlated nested loop join supports only INNER and LEFT join types.
            if (hint != CNL_JOIN) {
                doTestDisableJoinTypeWith("TBL1", "TBL3", "RIGHT", IgniteNestedLoopJoin.class,
                    NO_NL_JOIN);

                doTestDisableJoinTypeWith("TBL1", "TBL2", "FULL", IgniteNestedLoopJoin.class,
                    NO_NL_JOIN);
            }
        }
    }

    /**
     * Tests merge join is disabled by hints.
     */
    @Test
    public void testDisableMergeJoin() throws Exception {
        for (HintDefinition hint : Arrays.asList(NO_MERGE_JOIN, NL_JOIN, CNL_JOIN)) {
            doTestDisableJoinTypeWith("TBL4", "TBL2", "INNER", IgniteMergeJoin.class, hint);

            doTestDisableJoinTypeWith("TBL4", "TBL2", "LEFT", IgniteMergeJoin.class, hint);

            // Correlated nested loop join supports only INNER and LEFT join types.
            if (hint != CNL_JOIN) {
                doTestDisableJoinTypeWith("TBL4", "TBL2", "RIGHT", IgniteMergeJoin.class, hint);

                doTestDisableJoinTypeWith("TBL4", "TBL2", "FULL", IgniteMergeJoin.class, hint);
            }
        }
    }

    /**
     * Tests the merge join is enabled by the hint instead of the other joins.
     */
    @Test
    public void testMergeJoinEnabled() throws Exception {
        doTestCertainJoinTypeEnabled("TBL1", "INNER", "TBL2", IgniteCorrelatedNestedLoopJoin.class,
            MERGE_JOIN, IgniteMergeJoin.class);

        doTestCertainJoinTypeEnabled("TBL1", "RIGHT", "TBL2", IgniteNestedLoopJoin.class,
            MERGE_JOIN, IgniteMergeJoin.class);

        doTestCertainJoinTypeEnabled("TBL1", "INNER", "TBL2", IgniteCorrelatedNestedLoopJoin.class,
            MERGE_JOIN, IgniteMergeJoin.class, JOIN_REORDER_RULES);

        doTestCertainJoinTypeEnabled("TBL1", "RIGHT", "TBL2", IgniteNestedLoopJoin.class,
            MERGE_JOIN, IgniteMergeJoin.class, JOIN_REORDER_RULES);
    }

    /**
     * Tests the nested loop join is enabled by the hint instead of the other joins.
     */
    @Test
    public void testNLJoinEnabled() throws Exception {
        doTestCertainJoinTypeEnabled("TBL2", "INNER", "TBL1", IgniteCorrelatedNestedLoopJoin.class,
            NL_JOIN, IgniteNestedLoopJoin.class);

        doTestCertainJoinTypeEnabled("TBL5", "INNER", "TBL4", IgniteMergeJoin.class,
            NL_JOIN, IgniteNestedLoopJoin.class);

        doTestCertainJoinTypeEnabled("TBL1", "LEFT", "TBL2", IgniteCorrelatedNestedLoopJoin.class,
            NL_JOIN, IgniteNestedLoopJoin.class, JOIN_REORDER_RULES);

        doTestCertainJoinTypeEnabled("TBL5", "INNER", "TBL4", IgniteMergeJoin.class,
            NL_JOIN, IgniteNestedLoopJoin.class, JOIN_REORDER_RULES);
    }

    /** */
    @Test
    public void testSelfJoin() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL1 t2 on t1.v3=t2.v2 where t2.v3=4";

        assertPlan(String.format(sqlTpl, "/*+ " + MERGE_JOIN + "(TBL1) */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class).negate())
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL1")))));

        assertPlan(String.format(sqlTpl, "/*+ " + NO_CNL_JOIN + "(TBL1) */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class).negate())
                .and(nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(hasNestedTableScan("TBL1")))));
    }

    /**
     * Tests the correlated nested loop join is enabled by the hint instead of the other joins.
     */
    @Test
    public void testCNLJoinEnabled() throws Exception {
        doTestCertainJoinTypeEnabled("TBL2", "LEFT", "TBL1", IgniteNestedLoopJoin.class,
            CNL_JOIN, IgniteCorrelatedNestedLoopJoin.class);

        doTestCertainJoinTypeEnabled("TBL5", "INNER", "TBL4", IgniteMergeJoin.class,
            CNL_JOIN, IgniteCorrelatedNestedLoopJoin.class);

        // Even CNL join doesn't support RIGHT join, join type and join inputs might be switched by Calcite.
        doTestCertainJoinTypeEnabled("TBL1", "RIGHT", "TBL2", IgniteNestedLoopJoin.class,
            CNL_JOIN, IgniteCorrelatedNestedLoopJoin.class);

        doTestCertainJoinTypeEnabled("TBL2", "LEFT", "TBL1", IgniteNestedLoopJoin.class,
            CNL_JOIN, IgniteCorrelatedNestedLoopJoin.class, JOIN_REORDER_RULES);

        doTestCertainJoinTypeEnabled("TBL5", "INNER", "TBL4", IgniteMergeJoin.class,
            CNL_JOIN, IgniteCorrelatedNestedLoopJoin.class, JOIN_REORDER_RULES);
    }

    /**
     * Checks {@code expectedJoin} switches to {@code newJoin} by {@code hint} in the simple query
     * 'SELECT /*+ {@code hint} *&#47; t1.v1, t2.v2 FROM {@code tbl1} t1 {@code joinType} JOIN {@code tbl2} t2 on
     * t1.v3=t2.v3'.
     */
    private void doTestCertainJoinTypeEnabled(
        String tbl1,
        String joinType,
        String tbl2,
        Class<? extends AbstractIgniteJoin> expectedJoin,
        HintDefinition hint,
        Class<? extends AbstractIgniteJoin> newJoin,
        String... disabledRules
    ) throws Exception {
        String sqlTpl = String.format("SELECT %%s t1.v1, t2.v2 FROM %s t1 %s JOIN %s t2 on t1.v3=t2.v3", tbl1,
            joinType, tbl2);

        // Not using table name.
        assertPlan(String.format(sqlTpl, "/*+ " + hint.name() + "(UNEXISTING) */"), schema,
            nodeOrAnyChild(isInstanceOf(expectedJoin).and(hasNestedTableScan(tbl1))
                .and(hasNestedTableScan(tbl2))).and(nodeOrAnyChild(isInstanceOf(newJoin)).negate()), disabledRules);

        for (String t : Arrays.asList("", tbl1, tbl2)) {
            assertPlan(String.format(sqlTpl, "/*+ " + hint.name() + "(" + t + ") */"), schema,
                nodeOrAnyChild(isInstanceOf(expectedJoin).negate())
                    .and(nodeOrAnyChild(isInstanceOf(newJoin).and(hasNestedTableScan(tbl1)
                        .and(hasNestedTableScan(tbl2))))), disabledRules);

            if (t.isEmpty())
                continue;

            // Wrong tbl names must not affect.
            assertPlan(String.format(sqlTpl, "/*+ " + hint.name() + "(UNEXISTING)," + hint.name() + "(UNEXISTING,"
                + t + ",UNEXISTING) */"), schema, nodeOrAnyChild(isInstanceOf(expectedJoin).negate())
                .and(nodeOrAnyChild(isInstanceOf(newJoin).and(hasNestedTableScan(tbl1)
                    .and(hasNestedTableScan(tbl2))))), disabledRules);
        }
    }

    /** */
    private void doTestDisableJoinTypeWith(
        String tbl1,
        String tbl2,
        String sqlJoinType,
        Class<? extends AbstractIgniteJoin> joinRel,
        HintDefinition hint,
        String... disabledRules
    ) throws Exception {
        String sqlTpl = String.format("SELECT %%s t1.v1, t2.v2 FROM %s t1 %s JOIN %s t2 on t1.v3=t2.v3", tbl1,
            sqlJoinType, tbl2);

        String hintPref = "/*+ " + hint.name();

        // Hint with no options.
        assertPlan(String.format(sqlTpl, hintPref + " */"), schema, nodeOrAnyChild(isInstanceOf(joinRel)).negate(),
            disabledRules);

        // Hint with tbl1.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl1 + " ) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);

        // Hint with tbl2.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl2 + " ) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);

        // Hint with both tables.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl2 + ',' + tbl1 + " ) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);

        // Hint with wrong table.
        assertPlan(String.format(sqlTpl, hintPref + "('UNEXISTING') */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)), disabledRules);

        // Hint with correct and incorrect tbl.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl1 + ",UNEXISTING) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);
    }

    /**
     * Test table join hints.
     */
    @Test
    public void testTableHints() throws Exception {
        String sqlTpl = "SELECT %s A2.A, T3.V3, T1.V2 FROM (SELECT 1 AS A, 2 AS B) A2 JOIN TBL3 %s T3 ON A2.B=T3.V2 " +
            "JOIN TBL1 %s T1 on T3.V3=T1.V1 where T1.V2=5";

        assertPlan(String.format(sqlTpl, "", "/*+ " + NL_JOIN + " */", "/*+ " + MERGE_JOIN + " */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(input(1, isTableScan("TBL3"))))
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL1")))))), JOIN_REORDER_RULES);

        // Table hint has a bigger priority. Leading CNL_JOIN is ignored.
        assertPlan(String.format(sqlTpl, "/*+ " + CNL_JOIN + " */", "/*+ " + NL_JOIN + " */", "/*+ " + MERGE_JOIN + " */"),
            schema, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(input(1, isTableScan("TBL3"))))
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL1")))))), JOIN_REORDER_RULES);

        // Leading query hint works only for the second join.
        assertPlan(String.format(sqlTpl, "/*+ " + CNL_JOIN + " */", "/*+ " + NL_JOIN + " */", ""), schema,
            nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(input(1, isTableScan("TBL3"))))
                .and(nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL1")))))), JOIN_REORDER_RULES);

        // Table hint with wrong table name is ignored.
        assertPlan(String.format(sqlTpl, "", "/*+ " + NL_JOIN + "(TBL1), " + CNL_JOIN + " */", ""), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                .and(input(1, isTableScan("TBL3")))), JOIN_REORDER_RULES);
    }

    /**
     * Test table join hints with join reordering.
     */
    @Test
    public void testTableHintsWithReordering() throws Exception {
        assert PlannerHelper.JOINS_COUNT_FOR_HEURISTIC_ORDER >= 3;

        RuleApplyListener planLsnr = new RuleApplyListener(IgniteMultiJoinOptimizeRule.INSTANCE);

        // TBL3 leads, gets NL. TBL1 follows, gets MERGE. TBL5 is to keep joins order enough to launch the optimization.
        assertPlan("SELECT A2.A, T3.V3, T1.V2, T5.V3 FROM (SELECT 1 AS A, 2 AS B) A2 JOIN TBL3 /*+ NL_JOIN */ T3 ON A2.B=T3.V2 " +
                "JOIN TBL1 /*+ MERGE_JOIN */ T1 on T3.V1=T1.V1 JOIN TBL5 t5 on T1.V1=T5.V1 where T1.V3=5 and T5.V1=1", schema, planLsnr,
            checkJoinTypeAndInputScan(IgniteNestedLoopJoin.class, "TBL3")
                .and(checkJoinTypeAndInputScan(IgniteMergeJoin.class, "TBL1"))
        );

        assertTrue(planLsnr.ruleSucceeded());

        // TBL3 leads, gets top-level NL. TBL1 follows, gets MERGE. TBL5 is to keep joins order enough to launch the optimization.
        assertPlan("SELECT /*+ NL_JOIN */ A2.A, T3.V3, T1.V2, T5.V3 FROM (SELECT 1 AS A, 2 AS B) A2 JOIN TBL3 T3 ON A2.B=T3.V2 " +
                "JOIN TBL1 /*+ MERGE_JOIN */ T1 on T3.V1=T1.V1 JOIN TBL5 t5 on T1.V1=T5.V1 where T1.V3=5 and T5.V1=1", schema, planLsnr,
            checkJoinTypeAndInputScan(IgniteNestedLoopJoin.class, "TBL3")
                .and(checkJoinTypeAndInputScan(IgniteMergeJoin.class, "TBL1"))
        );

        assertTrue(planLsnr.ruleSucceeded());

        // TBL1 leads, gets NL. TBL3 follows, gets MERGE. TBL5 is to keep joins order enough to launch the optimization.
        assertPlan("SELECT A2.A, T1.V3, T3.V2, T5.V3 FROM (SELECT 1 AS A, 2 AS B) A2 JOIN TBL1 /*+ NL_JOIN */ T1 ON A2.B=T1.V2 " +
                "JOIN TBL3 /*+ MERGE_JOIN */ T3 on T1.V1=T3.V1 JOIN TBL5 t5 on T3.V3=T5.V3 where T3.V3=5 and T5.V1=1", schema, planLsnr,
            checkJoinTypeAndInputScan(IgniteNestedLoopJoin.class, "TBL1")
                .and(checkJoinTypeAndInputScan(IgniteMergeJoin.class, "TBL3"))
        );

        assertTrue(planLsnr.ruleSucceeded());

        // TBL1 leads, gets top-level NL. TBL3 follows, gets MERGE. TBL5 is to keep joins order enough to launch the optimization.
        assertPlan("SELECT /*+ NL_JOIN */ A2.A, T1.V3, T3.V2, T5.V1 FROM (SELECT 1 AS A, 2 AS B) A2 JOIN TBL1 T1 ON A2.B=T1.V2 " +
                "JOIN TBL3 /*+ MERGE_JOIN */ T3 on T1.V1=T3.V1 JOIN TBL5 t5 on T3.V3=T5.V3 where T3.V3=5 and T5.V1=1", schema, planLsnr,
            checkJoinTypeAndInputScan(IgniteNestedLoopJoin.class, "TBL1")
                .and(checkJoinTypeAndInputScan(IgniteMergeJoin.class, "TBL3"))
        );

        assertTrue(planLsnr.ruleSucceeded());
    }

    /**
     * Searches for a table scan named {@code tblName} in any input of join of type {@code jType} without other joins
     * in the middle.
     */
    private Predicate<RelNode> checkJoinTypeAndInputScan(Class<? extends AbstractIgniteJoin> jType, String tblName) {
        return nodeOrAnyChild(isInstanceOf(jType).and(
            input(0, nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)).negate())
                .and(input(0, nodeOrAnyChild(isTableScan(tblName))))
            .or(input(1, nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)).negate())
                .and(input(1, nodeOrAnyChild(isTableScan(tblName)))))
        ));
    }

    /**
     * Tests disable-join-hint works for a sub-query.
     */
    @Test
    public void testDisableJoinTypeInSubquery() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t2.v2 FROM TBL2 t1 JOIN TBL1 t2 on t1.v3=t2.v3 where t2.v3 in " +
            "(select %s t3.v3 from TBL3 t3 JOIN TBL4 t4 on t3.v1=t4.v1)";

        for (String tbl : Arrays.asList("TBL3", "TBL4")) {
            assertPlan(String.format(sqlTpl, "/*+ " + NO_MERGE_JOIN + "(" + tbl + ") */", ""), schema,
                nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan(tbl))
                ).negate(), JOIN_REORDER_RULES);

            // Hint in the sub-query.
            assertPlan(String.format(sqlTpl, "", "/*+ " + NO_MERGE_JOIN + "(" + tbl + ") */"), schema,
                nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan(tbl))
                ).negate(), JOIN_REORDER_RULES);

            // Also NO-NL-JOIN hint in the sub-query. Must not affect the parent query.
            assertPlan(String.format(sqlTpl, "", "/*+ " + NO_MERGE_JOIN + ',' + NO_NL_JOIN + " */"), schema,
                nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan("TBL2"))
                    .and(hasNestedTableScan("TBL1"))
                ).and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan(tbl))
                ).negate()), JOIN_REORDER_RULES);
        }
    }

    /** */
    @Test
    public void testNestedHintOverrides() throws Exception {
        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)", schema,
            nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()), JOIN_REORDER_RULES);

        assertPlan("SELECT /*+ " + CNL_JOIN + "(TBL1)," + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 " +
                "JOIN TBL2 t2 on t1.v3=t2.v3 where t2.v1 in (SELECT t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, predicateForNestedHintOverrides(false), JOIN_REORDER_RULES);

        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + CNL_JOIN + "(TBL1) */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, predicateForNestedHintOverrides(true), JOIN_REORDER_RULES);

        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT t3.v3 from TBL3 /*+ " + CNL_JOIN + " */ t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, predicateForNestedHintOverrides(true), JOIN_REORDER_RULES);

        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT t3.v3 from TBL3 t3 JOIN TBL1 /*+ " + CNL_JOIN + " */ t4 on t3.v2=t4.v2)",
            schema, predicateForNestedHintOverrides(true), JOIN_REORDER_RULES);
    }

    /** */
    @Test
    public void testNestedHintOverridesWithReordering() throws Exception {
        assert PlannerHelper.JOINS_COUNT_FOR_HEURISTIC_ORDER >= 3;

        RuleApplyListener planLsnr = new RuleApplyListener(IgniteMultiJoinOptimizeRule.INSTANCE);

        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + NL_JOIN + "(TBL1) */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteNestedLoopJoin.class).and(hasNestedTableScan("TBL1")))))
        );

        assertTrue(planLsnr.ruleSucceeded());

        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + NL_JOIN + "(TBL3) */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteNestedLoopJoin.class).and(hasNestedTableScan("TBL3")))))
        );

        assertTrue(planLsnr.ruleSucceeded());

        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + NL_JOIN + " */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteNestedLoopJoin.class)
                    .and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL3")))))
        );

        assertTrue(planLsnr.ruleSucceeded());

        assertPlan("SELECT /*+ " + NL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + MERGE_JOIN + "(TBL1) */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteMergeJoin.class)
                    .and(hasNestedTableScan("TBL1")))))
        );

        assertTrue(planLsnr.ruleSucceeded());

        assertPlan("SELECT /*+ " + NL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + MERGE_JOIN + "(TBL3) */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL3")))))
        );

        assertTrue(planLsnr.ruleSucceeded());

        assertPlan("SELECT /*+ " + NL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + MERGE_JOIN + " */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL3")))))
        );

        assertTrue(planLsnr.ruleSucceeded());

        // Produces unsupported left join.
        assertPlan("SELECT /*+ " + NL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 not in (SELECT /*+ " + MERGE_JOIN + " */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL3")))))
        );

        assertFalse(planLsnr.ruleSucceeded());

        // Produces unsupported outer join.
        assertPlan("SELECT /*+ " + NL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 FULL OUTER JOIN TBL2 t2 on t1.v3=t2.v3 " +
                "where t2.v1 in (SELECT /*+ " + MERGE_JOIN + " */ t3.v3 from TBL3 t3 JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL3")))))
        );

        assertFalse(planLsnr.ruleSucceeded());

        // Produces unsupported outer join.
        assertPlan("SELECT /*+ " + MERGE_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 JOIN TBL2 t2 on t1.v3=t2.v3 where " +
                "t2.v1 in (SELECT /*+ " + NL_JOIN + "(TBL3) */ t3.v3 from TBL3 t3 FULL OUTER JOIN TBL1 t4 on t3.v2=t4.v2)",
            schema, planLsnr, nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(hasChildThat(isInstanceOf(IgniteNestedLoopJoin.class).and(hasNestedTableScan("TBL3")))))
        );

        assertFalse(planLsnr.ruleSucceeded());
    }

    /**
     * @return A {@link Predicate} for {@link #testNestedHintOverrides()}
     */
    private Predicate<RelNode> predicateForNestedHintOverrides(boolean t1MergeT2) {
        Predicate<RelNode> res = nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)).negate()
            .and(nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                .and(hasNestedTableScan("TBL3")).and(hasNestedTableScan("TBL1"))));

        Predicate<RelNode> t1MergeT2Pred = nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
            .and(input(0, nodeOrAnyChild(isTableScan("TBL1")))
                .and(input(1, nodeOrAnyChild(isTableScan("TBL2"))))));

        Predicate<RelNode> t1CnlT2Pred = nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(input(0, nodeOrAnyChild(isTableScan("TBL1")))
                .and(input(1, nodeOrAnyChild(isTableScan("TBL2"))))));

        return t1MergeT2
            ? res.and(t1MergeT2Pred).and(t1CnlT2Pred.negate())
            : res.and(t1MergeT2Pred.negate()).and(t1CnlT2Pred);
    }

    /**
     * Tests that several disable join types are allowed.
     */
    @Test
    public void testSeveralDisables() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t2.v2 FROM TBL1 t1, TBL2 t2 where t1.v3=t2.v3";

        assertPlan(String.format(sqlTpl, "/*+ " + NO_CNL_JOIN + ',' + NO_NL_JOIN + " */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)).negate())
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL2")))), JOIN_REORDER_RULES);

        assertPlan(String.format(sqlTpl, "/*+ " + NO_CNL_JOIN + "(TBL1)," + NO_NL_JOIN + "(TBL2) */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)).negate())
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL2")))), JOIN_REORDER_RULES);

        // Check with forcing in the middle.
        assertPlan(String.format(sqlTpl, "/*+ " + NO_CNL_JOIN + ',' + NL_JOIN + ',' + NO_NL_JOIN + " */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL2")))), JOIN_REORDER_RULES);

        // Check with forcing in the middle with the table name.
        assertPlan(String.format(sqlTpl, "/*+ " + NO_CNL_JOIN + "(TBL1)," + NL_JOIN + "(TBl1)," + NO_NL_JOIN + " */"),
            schema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL2")))), JOIN_REORDER_RULES);

        // Wrong tbl name.
        assertPlan(String.format(sqlTpl, "/*+ " + NO_CNL_JOIN + ',' + NO_NL_JOIN + "(UNEXISTING) */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL2")))), JOIN_REORDER_RULES);

        // Disabling of all joins is prohibited. Last merge must work.
        assertPlan(String.format(sqlTpl, "/*+ " + NO_CNL_JOIN + ',' + NO_NL_JOIN + ',' + NO_MERGE_JOIN + " */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)).negate())
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL1"))
                    .and(hasNestedTableScan("TBL2")))), JOIN_REORDER_RULES);

        // Check many duplicated disables doesn't erase other disables.
        sqlTpl = "SELECT %s t1.v1, t2.v2 FROM TBL1 t1, TBL2 t2 where t1.v1=t2.v1";

        String hints = "/*+ " + NO_CNL_JOIN + ',' + NO_CNL_JOIN + "(TBL1), " + NO_CNL_JOIN + "(TBL1,TBL2), "
            + NO_NL_JOIN + "(TBL1) */";

        assertPlan(String.format(sqlTpl, hints), schema,
            nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class).and(hasChildThat(isTableScan("TBL1")))).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                    .and(hasChildThat(isTableScan("TBL1")))).negate()),
            JOIN_REORDER_RULES);
    }

    /** */
    @Test
    public void testWithAdditionalSelect() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t2.v2 FROM TBL1 t1, (SELECT t3.v2, t3.v3 FROM TBL3 t3 where t3.v1=5) t2 " +
            "where t1.v3=t2.v3";

        assertPlan(String.format(sqlTpl, "/*+ " + MERGE_JOIN + "(TBL1) */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL3"))
                .and(hasNestedTableScan("TBL1")))), JOIN_REORDER_RULES);

        assertPlan(String.format(sqlTpl, "/*+ " + MERGE_JOIN + "(TBL3) */"), schema,
            nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class).and(hasNestedTableScan("TBL3"))
                    .and(hasNestedTableScan("TBL1")))), JOIN_REORDER_RULES);
    }

    /** */
    @Test
    public void testDisableMergeJoinWith3Tables() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t2.v2, t3.v3 FROM TBL3 t1 JOIN TBL4 t2 on t1.v1=t2.v2 JOIN TBL5 t3 on " +
            "t2.v2=t3.v3";

        assertPlan(String.format(sqlTpl, "/*+ " + NO_MERGE_JOIN + " */"), schema,
            nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class))
                .and(nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)
                    .and(j -> j instanceof IgniteMergeJoin)).negate()));

        for (String tbl : Arrays.asList("TBL3", "TBL4", "TBL5")) {
            assertPlan(String.format(sqlTpl, "/*+ " + NO_MERGE_JOIN + "(" + tbl + ") */"), schema,
                nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan(tbl))
                ).negate());
        }
    }

    /** */
    private Predicate<RelNode> hasNestedTableScan(String tbl) {
        return input(0, nodeOrAnyChild(isTableScan(tbl)))
            .or(input(1, nodeOrAnyChild(isTableScan(tbl))));
    }

    /** */
    private Predicate<RelNode> noJoinChildren() {
        return nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)).negate();
    }
}
