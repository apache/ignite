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
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/**
 * Planner test for index hints.
 */
public class JoinTypeHintPlannerTest extends AbstractPlannerTest {
    /** */
    private static final String[] CORE_JOIN_REORDER_RULES =
        {"JoinCommuteRule", "JoinPushThroughJoinRule:left", "JoinPushThroughJoinRule:right"};

    /** */
    private IgniteSchema schema;

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
     * Tests {@link HintDefinition#NO_CNL_JOIN} disables collerated nested loop join.
     */
    @Test
    public void testDisableNCL() throws Exception {
        doTestDisableJoinTypeWith("TBL2", "TBL1", "INNER", IgniteCorrelatedNestedLoopJoin.class,
            HintDefinition.NO_CNL_JOIN);

        doTestDisableJoinTypeWith("TBL1", "TBL2", "LEFT", IgniteCorrelatedNestedLoopJoin.class,
            HintDefinition.NO_CNL_JOIN);

        // RIGHT-join is not supported by correlated nested loop. But Calcite replaces join inputs and join type.
        doTestDisableJoinTypeWith("TBL2", "TBL1", "RIGHT", IgniteCorrelatedNestedLoopJoin.class,
            HintDefinition.NO_CNL_JOIN);

        LogListener lsnr = LogListener.matches("Hint 'NO_CNL_JOIN' was skipped. Reason: Correlated nested " +
            "loop is not supported for join type 'FULL'").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + HintDefinition.NO_CNL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 FULL JOIN " +
            "TBL2 t2 on t1.v3=t2.v3", schema);

        assertTrue(lsnr.check());
    }

    /**
     * Tests {@link HintDefinition#NO_NL_JOIN} disables nested loop join.
     */
    @Test
    public void testDisableNL() throws Exception {
        doTestDisableJoinTypeWith("TBL3", "TBL1", "LEFT", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN);

        doTestDisableJoinTypeWith("TBL1", "TBL3", "RIGHT", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN);

        doTestDisableJoinTypeWith("TBL5", "TBL4", "INNER", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN, "MergeJoinConverter");

        doTestDisableJoinTypeWith("TBL1", "TBL2", "FULL", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN);
    }

    /**
     * Tests {@link HintDefinition#NO_MERGE_JOIN} disables nested loop join.
     */
    @Test
    public void testDisableMerge() throws Exception {
        doTestDisableJoinTypeWith("TBL3", "TBL5", "INNER", IgniteMergeJoin.class,
            HintDefinition.NO_MERGE_JOIN);

        doTestDisableJoinTypeWith("TBL3", "TBL5", "LEFT", IgniteMergeJoin.class,
            HintDefinition.NO_MERGE_JOIN);

        doTestDisableJoinTypeWith("TBL3", "TBL5", "RIGHT", IgniteMergeJoin.class,
            HintDefinition.NO_MERGE_JOIN);

        doTestDisableJoinTypeWith("TBL3", "TBL5", "FULL", IgniteMergeJoin.class,
            HintDefinition.NO_MERGE_JOIN);
    }

    /** */
    private void doTestDisableJoinTypeWith(String tbl1, String tbl2, String sqlJoinType,
        Class<? extends AbstractIgniteJoin> joinRel, HintDefinition hint, String... disabledRules) throws Exception {
        String sqlTpl = String.format("SELECT %%s t1.v1, t2.v2 FROM %s t1 %s JOIN %s t2 on t1.v3=t2.v3", tbl1,
            sqlJoinType, tbl2);

        String hintPref = "/*+ " + hint.name();

        // No hint. Ensure target join type.
        assertPlan(String.format(sqlTpl, ""), schema, nodeOrAnyChild(isInstanceOf(joinRel)), disabledRules);

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
     * Tests disable-join-hint works for a sub-query.
     */
    @Test
    public void testDisableJoinTypeInSubquery() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t2.v2 FROM TBL2 t1 JOIN TBL1 t2 on t1.v3=t2.v3 where t2.v3 in " +
            "(select %s t3.v3 from TBL3 t3 JOIN TBL4 t4 on t3.v1=t4.v1)";

        assertPlan(String.format(sqlTpl, "", ""), schema,
            nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                .and(input(0, noJoinChildren()))
                .and(input(1, noJoinChildren()))
                .and(hasNestedTableScan("TBL2"))
                .and(hasNestedTableScan("TBL1"))
            ).and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                .and(input(0, noJoinChildren()))
                .and(input(1, noJoinChildren()))
                .and(hasNestedTableScan("TBL3"))
                .and(hasNestedTableScan("TBL4"))
            )), CORE_JOIN_REORDER_RULES);

        for (String tbl : Arrays.asList("TBL3", "TBL4")) {
            assertPlan(String.format(sqlTpl, "/*+ " + HintDefinition.NO_MERGE_JOIN + "(" + tbl + ") */", ""), schema,
                nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan(tbl))
                ).negate(), CORE_JOIN_REORDER_RULES);

            // Hint in the sub-query.
            assertPlan(String.format(sqlTpl, "", "/*+ " + HintDefinition.NO_MERGE_JOIN + "(" + tbl + ") */"), schema,
                nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan(tbl))
                ).negate(), CORE_JOIN_REORDER_RULES);

            // Also NO-NL-JOIN hint in the sub-query. Must not affect the parent query.
            assertPlan(String.format(sqlTpl, "", "/*+ " + HintDefinition.NO_MERGE_JOIN + ','
                    + HintDefinition.NO_NL_JOIN + " */"), schema,
                nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan("TBL2"))
                    .and(hasNestedTableScan("TBL1"))
                ).and(nodeOrAnyChild(isInstanceOf(IgniteMergeJoin.class)
                    .and(input(0, noJoinChildren()))
                    .and(input(1, noJoinChildren()))
                    .and(hasNestedTableScan(tbl))
                ).negate()), CORE_JOIN_REORDER_RULES);
        }
    }

    /** */
    @Test
    public void testDisableMergeJoinWith3Tables() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t2.v2, t3.v3 FROM TBL3 t1 JOIN TBL4 t2 on t1.v1=t2.v2 JOIN TBL5 t3 on " +
            "t2.v2=t3.v3";

        // Ensures there is no non-merge joins.
        assertPlan(String.format(sqlTpl, ""), schema, nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class))
            .and(nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)
                .and(j -> !(j instanceof IgniteMergeJoin))).negate()));

        assertPlan(String.format(sqlTpl, "/*+ " + HintDefinition.NO_MERGE_JOIN + " */"), schema,
            nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class))
                .and(nodeOrAnyChild(isInstanceOf(AbstractIgniteJoin.class)
                    .and(j -> j instanceof IgniteMergeJoin)).negate()));

        for (String tbl : Arrays.asList("TBL3", "TBL4", "TBL5")) {
            assertPlan(String.format(sqlTpl, "/*+ " + HintDefinition.NO_MERGE_JOIN + "(" + tbl + ") */"), schema,
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
