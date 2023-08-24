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

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 * Planner test for JOIN hints.
 */
public class JoinOrderHintsPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        int tblNum = 3;
        int fldNum = 3;

        TestTable[] tables = new TestTable[tblNum];
        Object[] fields = new Object[fldNum * 2];

        for (int f = 0; f < fldNum; ++f) {
            fields[f * 2] = "V" + (f + 1);
            fields[f * 2 + 1] = Integer.class;
        }

        for (int t = 0; t < tables.length; ++t) {
            tables[t] = createTable("TBL" + (t + 1), Math.min(1_000_000, (int)Math.pow(10, t + 1)),
                IgniteDistributions.broadcast(), fields);
        }

        schema = createSchema(tables);
    }

    /**
     * Tests {@link JoinPushThroughJoinRule#RIGHT} is disabled by {@link HintDefinition#ORDERED_JOINS}.
     */
    @Test
    public void testDisabledJoinPushThroughJoinRight() throws Exception {
        String disabledRules =
            String.format("DISABLE_RULE('MergeJoinConverter', 'CorrelatedNestedLoopJoin', '%s', '%s')",
                CoreRules.JOIN_COMMUTE.toString(), JoinPushThroughJoinRule.LEFT.toString());

        // Ensures the plan has swapped join order, 'TBL1->TBL3->TBL2' instead of 'TBL1->TBL2->TBL3':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL3)
        //   IgniteTable(TBL2)
        String sql = String.format("select /*+ %s */ t3.* from TBL1 t1, TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and " +
            "t1.v2=t2.v2", disabledRules);

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, isInstanceOf(Join.class)
                .and(input(0, isTableScan("TBL1")))
                .and(input(1, isTableScan("TBL3")))))
            .and(input(1, isTableScan("TBL2")))));

        // Tests the swapping of joins is disabled and the order appears as in the query, 'TBL1->TBL2->TBL3':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL2)
        //   TableScan(TBL3)
        sql = String.format("select /*+ %s, %s */ t3.* from TBL1 t1, TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and " +
            "t1.v2=t2.v2", HintDefinition.ORDERED_JOINS.name(), disabledRules);

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, isInstanceOf(Join.class)
                .and(input(0, isTableScan("TBL1")))
                .and(input(1, isTableScan("TBL2")))))
            .and(input(1, isTableScan("TBL3")))));
    }

    /**
     * Tests {@link JoinPushThroughJoinRule#LEFT} is disabled by {@link HintDefinition#ORDERED_JOINS}.
     */
    @Test
    public void testDisabledJoinPushThroughJoinLeft() throws Exception {
        String disabledRules =
            String.format("DISABLE_RULE('MergeJoinConverter', 'CorrelatedNestedLoopJoin', '%s', '%s')",
                CoreRules.JOIN_COMMUTE.toString(), JoinPushThroughJoinRule.RIGHT.toString());

        // Ensures the plan has swapped join order, 'TBL1->TBL2->TBL3' instead of 'TBL3->TBL2->TBL1':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL2)
        //   IgniteTable(TBL3)
        String sql = String.format("select /*+ %s */ t3.* from TBL3 t3, TBL2 t2, TBL1 t1 where t1.v1=t3.v1 and " +
            "t1.v2=t2.v2", disabledRules);

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, isInstanceOf(Join.class)
                .and(input(0, isTableScan("TBL1")))
                .and(input(1, isTableScan("TBL2")))))
            .and(input(1, isTableScan("TBL3")))));

        // Tests swapping of joins is disabled and the order appears in the query, 'TBL3 -> TBL2 -> TBL1':
        // Join
        //   Join
        //     TableScan(TBL3)
        //     TableScan(TBL2)
        //   TableScan(TBL1)
        sql = String.format("select /*+ %s, %s */ t3.* from TBL3 t3, TBL2 t2, TBL1 t1 where t1.v1=t3.v1 and " +
            "t1.v2=t2.v2", HintDefinition.ORDERED_JOINS.name(), disabledRules);

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, isInstanceOf(Join.class)
                .and(input(0, isTableScan("TBL3")))
                .and(input(1, isTableScan("TBL2")))))
            .and(input(1, isTableScan("TBL1")))));
    }

    /**
     * Tests commuting of LEFT-to-RIGHT JOIN is disabled by {@link HintDefinition#ORDERED_JOINS}.
     */
    @Test
    public void testDisabledLeftJoinTypeCommuting() throws Exception {
        doTestDisabledJoinTypeCommuting("LEFT");
    }

    /**
     * Tests commuting of RIGHT-to-LEFT JOIN is disabled by {@link HintDefinition#ORDERED_JOINS}.
     */
    @Test
    public void testDisabledRightJoinTypeCommuting() throws Exception {
        doTestDisabledJoinTypeCommuting("RIGHT");
    }

    /**
     * Tests commuting of {@code joinType} is disabled.
     *
     * @param joinType LEFT or RIGHT JOIN type to test in upper case.
     */
    private void doTestDisabledJoinTypeCommuting(String joinType) throws Exception {
        String disabledRules =
            String.format("DISABLE_RULE('MergeJoinConverter', 'CorrelatedNestedLoopJoin', '%s', '%s')",
                JoinPushThroughJoinRule.LEFT.toString(), JoinPushThroughJoinRule.RIGHT.toString());

        // Ensures the plan has commuted join type.
        String sql = String.format("select /*+ %s */ t3.* from TBL2 t2 %s JOIN TBL1 t1 on t2.v2=t1.v1 %s JOIN " +
            "TBL3 t3 on t2.v1=t3.v3", disabledRules, joinType, joinType);

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(j -> j.getJoinType() != JoinRelType.valueOf(joinType))));

        // Tests commuting of the join type is disabled.
        sql = String.format("select /*+ %s, %s */ t3.* from TBL2 t2 %s JOIN TBL1 t1 on t2.v2=t1.v1 %s JOIN " +
            "TBL3 t3 on t2.v1=t3.v3", HintDefinition.ORDERED_JOINS.name(), disabledRules, joinType, joinType);

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(j -> j.getJoinType() != JoinRelType.valueOf(joinType))).negate());
    }

    /**
     * Tests the commuting of join inputs is disabled by  by {@link HintDefinition#ORDERED_JOINS}.
     */
    @Test
    public void testDisabledCommutingOfJoinInputs() throws Exception {
        String disabledRules = String.format("DISABLE_RULE('%s', '%s', '%s', '%s')", "MergeJoinConverter",
            "CorrelatedNestedLoopJoin", JoinPushThroughJoinRule.LEFT.toString(),
            JoinPushThroughJoinRule.RIGHT.toString());

        String sql = String.format("select /*+ %s */ t3.* from TBL1 t1 JOIN TBL3 t3 on t1.v1=t3.v3 JOIN TBL2 t2 on " +
            "t2.v2=t1.v1", disabledRules);

        // Ensures the plan has swapped join inputs, 'TBL3,TBL1' instead of 'TBL1,TBL3'.
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(Join.class)
            .and(input(0, isTableScan("TBL3")))
            .and(input(1, isTableScan("TBL1")))));

        sql = String.format("select /*+ %s, %s */ t3.* from TBL1 t1 JOIN TBL3 t3 on t1.v1=t3.v3 JOIN TBL2 t2 on " +
            "t2.v2=t1.v1", HintDefinition.ORDERED_JOINS.name(), disabledRules);

        // Tests the plan has no commuted join inputs.
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(Join.class)
            .and(input(0, isTableScan("TBL3")))
            .and(input(1, isTableScan("TBL1")))).negate());
    }
}
