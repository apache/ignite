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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.Join;
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
    /** Number of the test tables. */
    private static final int TBL_NUM = 3;

    /** Number of fields of each test table. */
    private static final int FLD_NUM = 3;

    /**
     * Iterations of variating-query tests. Since the planner might not choose the commuted joins, we have to either
     * increase {@link #TBL_NUM} or the test iterations. With the smaller tables number, each test attempt is faster
     * and the plan is more readable.
     */
    private static final int ITERATIONS = 30;

    /**
     * Commutes of join types may not happen at all. Like with INNER. But Calcite can take too long for the commuting
     * tables and their fields.
     *
     * @see #testCommutedInnerJoinsDuration
     */
    private static final int TEST_TIMEOUT = 45_000;

    /** */
    private IgniteSchema schema;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        TestTable[] tables = new TestTable[TBL_NUM];
        Object[] fields = new Object[FLD_NUM * 2];

        for (int f = 0; f < FLD_NUM; ++f) {
            fields[f * 2] = "V" + (f + 1);
            fields[f * 2 + 1] = Integer.class;
        }

        for (int t = 0; t < tables.length; ++t) {
            tables[t] = createTable("TBL" + (t + 1), Math.min(1_000_000, (int)Math.pow(10, t + 1)),
                IgniteDistributions.broadcast(), fields);
        }

        schema = createSchema(tables);
    }

    /** */
    @Test
    public void testNoCommutedLeftJoins() throws Exception {
        doIteratedTestCommutedJoins("LEFT");
    }

    /** */
    @Test
    public void testNoCommutedRightJoins() throws Exception {
        doIteratedTestCommutedJoins("RIGHT");
    }

    /**
     * Tests tables commuting in INNER JOIN doesn't take too long.
     */
    @Test
    public void testCommutedInnerJoinsDuration() throws Exception {
        doIteratedTestCommutedJoins("INNER");
    }

    /**
     * Tests {@link JoinPushThroughJoinRule#LEFT} is disabled.
     */
    @Test
    public void testJoinPushThroughJoinLeftIsDisabled() throws Exception {
        // Ensures the plan has swapped join order, 'TBL1 -> TBL2 -> TBL3':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL2)
        //   IgniteTable(TBL3)
        String sql = String.format("select /*+ DISABLE_RULE('%s', '%s', 'MergeJoinConverter', " +
                "'CorrelatedNestedLoopJoin') */ t3.* from TBL3 t3, TBL2 t2, TBL1 t1 where t1.v1=t3.v1 and t1.v2=t2.v2",
            CoreRules.JOIN_COMMUTE.toString(), JoinPushThroughJoinRule.RIGHT.toString());

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
        assertPlan("select /*+ ORDERED_JOIN, DISABLE_RULE('MergeJoinConverter', 'CorrelatedNestedLoopJoin') */ " +
                "t3.* from TBL3 t3, TBL2 t2, TBL1 t1 where t1.v1=t3.v1 and t1.v2=t2.v2",
            schema, hasChildThat(isInstanceOf(Join.class)
                .and(input(0, isInstanceOf(Join.class)
                    .and(input(0, isTableScan("TBL3")))
                    .and(input(1, isTableScan("TBL2")))))
                .and(input(1, isTableScan("TBL1")))));
    }

    /**
     * Tests {@link JoinPushThroughJoinRule#RIGHT} is disabled.
     */
    @Test
    public void testJoinPushThroughJoinRightIsDisabled() throws Exception {
        // Ensures the plan has swapped join order, 'TBL1 -> TBL3 -> TBL2':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL3)
        //   IgniteTable(TBL2)
        String sql = String.format("select /*+ DISABLE_RULE('%s', '%s', 'MergeJoinConverter', " +
                "'CorrelatedNestedLoopJoin') */ t3.* from TBL1 t1, TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and t1.v2=t2.v2",
            CoreRules.JOIN_COMMUTE.toString(), JoinPushThroughJoinRule.LEFT.toString());

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, isInstanceOf(Join.class)
                .and(input(0, isTableScan("TBL1")))
                .and(input(1, isTableScan("TBL3")))))
            .and(input(1, isTableScan("TBL2")))));

        // Tests swapping of joins is disabled and the order appears in the query, 'TBL1 -> TBL2 -> TBL3':
        // Join
        //   Join
        //     TableScan(TBL1)
        //     TableScan(TBL2)
        //   TableScan(TBL3)
        assertPlan("select /*+ ORDERED_JOIN, DISABLE_RULE('MergeJoinConverter', 'CorrelatedNestedLoopJoin') */ " +
                "t3.* from TBL1 t1, TBL2 t2, TBL3 t3 where t1.v1=t3.v1 and t1.v2=t2.v2",
            schema, hasChildThat(isInstanceOf(Join.class)
                .and(input(0, isInstanceOf(Join.class)
                    .and(input(0, isTableScan("TBL1")))
                    .and(input(1, isTableScan("TBL2")))))
                .and(input(1, isTableScan("TBL3")))));
    }

    /**
     * Tests the plan has no commuted {@code joinType} using {@link #ITERATIONS} attemps.
     */
    private void doIteratedTestCommutedJoins(String joinType) throws Exception {
        List<Integer> tblQueryOrder = IntStream.range(1, TBL_NUM + 1).boxed().collect(Collectors.toList());

        for (int i = 0; i < ITERATIONS; ++i) {
            Collections.shuffle(tblQueryOrder);

            doNoCommutedJoinsTest(tblQueryOrder, joinType);
        }
    }

    /**
     * Tests the plan has no commuted {@code joinType} joint types.
     */
    private void doNoCommutedJoinsTest(List<Integer> tblQueryOrder, String joinType) throws Exception {
        StringBuilder selectValues = new StringBuilder();
        StringBuilder joins = new StringBuilder();

        for (int t = 0; t < tblQueryOrder.size(); ++t) {
            int t1num = tblQueryOrder.get(t);

            for (int v = 0, vcnt = 1 + (t % FLD_NUM); v < vcnt; ++v) {
                selectValues.append("T").append(t1num).append('.').append('V').append(v + 1);

                if (v < vcnt - 1)
                    selectValues.append(", ");
            }

            if (t == 0)
                joins.append(" FROM TBL").append(t1num).append(" T").append(t1num);

            if (t < tblQueryOrder.size() - 1)
                selectValues.append(", ");
            else
                continue;

            int t2num = tblQueryOrder.get(t + 1);

            joins.append(' ').append(joinType).append(" JOIN TBL").append(t2num).append(" T").append(t2num);

            for (int f = 0, joinFldCnt = 1 + (t % FLD_NUM); f < joinFldCnt; ++f) {
                joins.append(f == 0 ? " ON " : " AND ");

                joins.append("T").append(t1num).append(".V").append(FLD_NUM - (f % FLD_NUM)).append('=')
                    .append("T").append(t2num).append(".V").append(1 + (f % FLD_NUM));
            }
        }

        assertPlan("SELECT /*+ " + HintDefinition.ORDERED_JOIN.name() + " */ " + selectValues + joins, schema,
            nodeOrAnyChild(isInstanceOf(Join.class).and(j -> !j.getJoinType().name().equalsIgnoreCase(joinType)))
                .negate());
    }

    /** */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }
}
