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
import org.apache.calcite.rel.core.SetOp;
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

        int tblNum = 3;
        int fldNum = 3;

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
     * Tests {@link JoinPushThroughJoinRule#LEFT} is disabled by {@link HintDefinition#ENFORCE_JOIN_ORDER}.
     */
    @Test
    public void testDisabledJoinPushThroughJoinLeft() throws Exception {
        // Tests swapping of joins is disabled and the order appears in the query, 'TBL3 -> TBL2 -> TBL1':
        // Join
        //   Join
        //     TableScan(TBL3)
        //     TableScan(TBL2)
        //   TableScan(TBL1)
        String sql = String.format("select /*+ %s, NL_JOIN */ t3.* from TBL3 t3, TBL2 t2, TBL1 t1 where t1.v1=t3.v1 " +
            "and t1.v2=t2.v2", HintDefinition.ENFORCE_JOIN_ORDER.name());

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, isInstanceOf(Join.class)
                .and(input(0, isTableScan("TBL3")))
                .and(input(1, isTableScan("TBL2")))))
            .and(input(1, isTableScan("TBL1")))));
    }

    /**
     * Tests commuting of LEFT-to-RIGHT JOIN is disabled by {@link HintDefinition#ENFORCE_JOIN_ORDER}.
     */
    @Test
    public void testDisabledLeftJoinTypeCommuting() throws Exception {
        doTestDisabledJoinTypeCommuting("LEFT");
    }

    /**
     * Tests commuting of RIGHT-to-LEFT JOIN is disabled by {@link HintDefinition#ENFORCE_JOIN_ORDER}.
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
        // Tests commuting of the join type is disabled.
        String sql = String.format("select /*+ %s, NL_JOIN */ t3.* from TBL2 t2 %s JOIN TBL1 t1 on t2.v2=t1.v1 %s JOIN " +
            "TBL3 t3 on t2.v1=t3.v3", HintDefinition.ENFORCE_JOIN_ORDER.name(), joinType, joinType);

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(j -> j.getJoinType() != JoinRelType.valueOf(joinType))).negate());
    }

    /**
     * Tests {@link JoinPushThroughJoinRule#RIGHT} is disabled by {@link HintDefinition#ENFORCE_JOIN_ORDER}.
     */
    @Test
    public void testDisabledJoinPushThroughJoinRight() throws Exception {
        // Tests the swapping of joins is disabled and the order appears as in the query, 'TBL1->TBL2->TBL3':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL2)
        //   TableScan(TBL3)
        String sql = String.format("select /*+ %s, NL_JOIN */ t3.* from TBL1 t1, TBL2 t2, TBL3 t3 where t1.v1=t3.v1 " +
            "and t1.v2=t2.v2", HintDefinition.ENFORCE_JOIN_ORDER.name());

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, isInstanceOf(Join.class)
                .and(input(0, isTableScan("TBL1")))
                .and(input(1, isTableScan("TBL2")))))
            .and(input(1, isTableScan("TBL3")))));
    }

    /**
     * Tests the commuting of join inputs is disabled by {@link HintDefinition#ENFORCE_JOIN_ORDER}.
     */
    @Test
    public void testDisabledCommutingOfJoinInputs() throws Exception {
        String sql = String.format("select /*+ %s, NL_JOIN */ t3.* from TBL1 t1 JOIN TBL3 t3 on t1.v1=t3.v3 JOIN TBL2 t2 on " +
            "t2.v2=t1.v1", HintDefinition.ENFORCE_JOIN_ORDER.name());

        // Tests the plan has no commuted join inputs.
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(Join.class)
            .and(input(0, isTableScan("TBL1")))
            .and(input(1, isTableScan("TBL3")))));
    }

    /**
     * Tests that the sub-join has inputs order matching the sub-query.
     */
    @Test
    public void testDisabledCommutingOfJoinInputsInSubquery() throws Exception {
        String sqlTpl = "SELECT %s t2.v1, t3.v2 from TBL2 t2 JOIN TBL3 t3 on t2.v1=t3.v1 where t2.v2 in " +
            "(SELECT %s t2.v2 from TBL2 t2 JOIN TBL3 t3 on t2.v2=t3.v3)";

        // Tests the hint is applied for the whole query.
        assertPlan(String.format(sqlTpl, "/*+ " + HintDefinition.ENFORCE_JOIN_ORDER + " */", ""), schema,
            nodeOrAnyChild(isInstanceOf(Join.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL2"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL3")))))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL2"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL3")))))))));

        // Tests the hint is applied for the sub-query.
        assertPlan(String.format(sqlTpl, "", "/*+ " + HintDefinition.ENFORCE_JOIN_ORDER + " */"), schema,
            nodeOrAnyChild(isInstanceOf(Join.class)
                .and(input(0, nodeOrAnyChild(isTableScan("TBL2").or(isTableScan("TBL3")))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL2"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL3")))))))));
    }

    /**
     * Tests join plan building duration. Without enabled forced order, takes too long.
     */
    @Test
    public void testJoinPlanBuildingDuration() throws Exception {
        // Just a 3-tables join.
        String sql = "SELECT /*+ " + HintDefinition.ENFORCE_JOIN_ORDER + " */ T1.V1, T2.V1, T2.V2, T3.V1, T3.V2, T3.V3 " +
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
    public void testUnions() throws Exception {
        String sqlTpl = "SELECT %s t2.v1, t3.v2 from TBL2 t2 JOIN TBL3 t3 on t2.v1=t3.v1 UNION ALL " +
            "SELECT %s t1.v1, t2.v2 from TBL1 t1 JOIN TBL2 t2 on t1.v1=t2.v2";

        String hint = HintDefinition.ENFORCE_JOIN_ORDER.toString();

        assertPlan(String.format(sqlTpl, "/*+ " + hint + " */", ""), schema,
            nodeOrAnyChild(isInstanceOf(SetOp.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL2"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL3")))))))));

        assertPlan(String.format(sqlTpl, "", "/*+ " + hint + " */"), schema,
            nodeOrAnyChild(isInstanceOf(SetOp.class)
                .and(input(1, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL1"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL2")))))))));

        assertPlan(String.format(sqlTpl, "/*+ " + hint + " */", "/*+ " + hint + " */"), schema,
            nodeOrAnyChild(isInstanceOf(SetOp.class)
                .and(input(1, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL1"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL2")))))))
                .and(input(0, nodeOrAnyChild(isInstanceOf(Join.class)
                    .and(input(0, nodeOrAnyChild(isTableScan("TBL2"))))
                    .and(input(1, nodeOrAnyChild(isTableScan("TBL3")))))))));
    }

    /** */
    @Test
    public void testWrongParams() throws Exception {
        LogListener lsnr = LogListener.matches("Hint '" + HintDefinition.ENFORCE_JOIN_ORDER
            + "' can't have any key-value option").build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        physicalPlan("select /*+ " + HintDefinition.ENFORCE_JOIN_ORDER.name() + "(a='b') */ t3.* from TBL1 t1, " +
            "TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and t1.v2=t2.v2", schema);

        assertTrue(lsnr.check());

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Hint '" + HintDefinition.ENFORCE_JOIN_ORDER + "' can't have any option").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("select /*+ " + HintDefinition.ENFORCE_JOIN_ORDER.name() + "(OPTION) */ t3.* from TBL1 t1, " +
            "TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and t1.v2=t2.v2", schema);

        assertTrue(lsnr.check());
    }
}
