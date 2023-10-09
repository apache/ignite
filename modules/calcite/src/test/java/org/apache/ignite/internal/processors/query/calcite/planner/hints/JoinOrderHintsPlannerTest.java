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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.junit.Test;

/**
 * Planner test for join order hints.
 */
public class JoinOrderHintsPlannerTest extends AbstractPlannerTest {
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

        int tblNum = 4;
        int fldNum = 4;

        TestTable[] tables = new TestTable[tblNum];
        Object[] fields = new Object[tblNum * 2];

        for (int f = 0; f < fldNum; ++f) {
            fields[f * 2] = "V" + (f + 1);
            fields[f * 2 + 1] = Integer.class;
        }

        // Tables with growing records number.
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

        // Tests the swapping of joins is disabled and the order appears as in the query, 'TBL1->TBL2->TBL3':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL2)
        //   TableScan(TBL3)
        String sql = String.format("select /*+ %s, %s */ t3.* from TBL1 t1, TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and " +
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
        String disabledRules = "DISABLE_RULE('MergeJoinConverter', 'CorrelatedNestedLoopJoin', 'JoinCommuteRule')";//, " +
//            "'JoinPushThroughJoinRule:right', 'JoinPushThroughJoinRule:left')";

        // Tests swapping of joins is disabled and the order appears in the query, 'TBL3 -> TBL2 -> TBL1':
        // Join
        //   Join
        //     TableScan(TBL3)
        //     TableScan(TBL2)
        //   TableScan(TBL1)
        String sql = String.format("select /*+ %s */ t3.*, t1.v1, t3.v3, t4.v4 from TBL3 t3, TBL2 t2, TBL1 t1, TBL4 t4 where " +
            "t2.v1=t3.v3 and t1.v1=t2.v2 and t4.v4=t1.v3", disabledRules);

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

        // Tests commuting of the join type is disabled.
        String sql = String.format("select /*+ %s, %s */ t3.* from TBL2 t2 %s JOIN TBL1 t1 on t2.v2=t1.v1 %s JOIN " +
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

        String sql = String.format("select /*+ %s, %s */ t3.* from TBL1 t1 JOIN TBL3 t3 on t1.v1=t3.v3 JOIN TBL2 t2 on " +
            "t2.v2=t1.v1", HintDefinition.ORDERED_JOINS.name(), disabledRules);

        // Tests the plan has no commuted join inputs.
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(Join.class)
            .and(input(0, isTableScan("TBL3")))
            .and(input(1, isTableScan("TBL1")))).negate());
    }

    /**
     * Tests join plan building duration. Without enabled forced order, takes too long.
     */
    @Test
    public void testJoinPlanBuildingDuration() throws Exception {
        // Just a 3-tables join.
        String sql = "SELECT /*+ " + HintDefinition.ORDERED_JOINS + " */ T1.V1, T2.V1, T2.V2, T3.V1, T3.V2, T3.V3 " +
            "FROM TBL1 T1 JOIN TBL2 T2 ON T1.V3=T2.V1 JOIN TBL3 T3 ON T2.V3=T3.V1 AND T2.V2=T3.V2";

        long time = 0;

        // Heat a bit and measure only the last run.
        for (int i = 0; i < 6; ++i) {
            time = System.nanoTime();

            physicalPlan(sql, schema);

            time = U.nanosToMillis(System.nanoTime() - time);

            log.info("Plan building took " + time + "ms.");
        }

        assertTrue("Plan building took too long: " + time + "ms.", time < 3000L);
    }

    /** */
    @Test
    public void testDisabledCommutingOfJoinInputsInSubquery() throws Exception {
        String sqlTpl = "SELECT %s t1.v1, t3.v2 from TBL1 t1 JOIN TBL3 t3 on t1.v3=t3.v3 where t1.v2 in " +
            "(SELECT %s t2.v2 from TBL2 t2 JOIN TBL3 t3 on t2.v1=t3.v1)";

        // Ensure that the sub-join has inputs order matching the sub-query: 'TBL2->TBL3'.
        assertPlan(String.format(sqlTpl, "/*+ " + HintDefinition.ORDERED_JOINS + " */", ""), schema,
            nodeOrAnyChild(isInstanceOf(Join.class).and(input(0, nodeOrAnyChild(isTableScan("TBL1"))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL2"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBl3")))))))));

        assertPlan(String.format(sqlTpl, "", "/*+ " + HintDefinition.ORDERED_JOINS + " */"), schema,
            nodeOrAnyChild(isInstanceOf(Join.class).and(input(0, nodeOrAnyChild(isTableScan("TBL1"))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL2"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBl3")))))))));
    }

    /** */
    @Test
    public void testWrongParams() throws Exception {
        LogListener lsnr = LogListener.matches("Hint '" + HintDefinition.ORDERED_JOINS
            + "' can't have any key-value option").build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        physicalPlan("select /*+ " + HintDefinition.ORDERED_JOINS.name() + "(a='b') */ t3.* from TBL1 t1, " +
            "TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and t1.v2=t2.v2", schema);

        assertTrue(lsnr.check());

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Hint '" + HintDefinition.ORDERED_JOINS + "' can't have any option").build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        physicalPlan("select /*+ " + HintDefinition.ORDERED_JOINS.name() + "(OPTION) */ t3.* from TBL1 t1, " +
            "TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and t1.v2=t2.v2", schema);

        assertTrue(lsnr.check());
    }
}
