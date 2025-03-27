/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.List;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.visualizer.RuleMatchVisualizer;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Test;

/** Tests correctness applying of JOIN_COMMUTE* rules. */
public class JoinCommutePlannerTest extends AbstractPlannerTest {
    /** */
    private static IgniteSchema publicSchema;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        publicSchema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
            "HUGE",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .build(), 1_000) {

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.affinity(0, "HUGE", "hash");
                }
            }
        );

        publicSchema.addTable(
            "SMALL",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .build(), 10) {

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.affinity(0, "SMALL", "hash");
                }
            }
        );
    }

    /** Ensures that join commute rules are disabled if joins count is too high. Checks the tables order in the query. */
    @Test
    public void testCommuteDisabledForManyJoins() throws Exception {
        int maxJoinsToOptimize = Math.max(PlannerHelper.MAX_JOINS_TO_COMMUTE_INPUTS, PlannerHelper.MAX_JOINS_TO_COMMUTE);

        // +2 tables because we use 1 more join in the query and 1 extra table for the correlated query.
        int tablesCnt = maxJoinsToOptimize + 3;

        try {
            for (int i = 0; i < tablesCnt; ++i) {
                TestTable tbl = createTable("TBL" + i, (int)Math.pow(100, (1 + (i % 3))),
                    IgniteDistributions.affinity(0, "TEST_CACHE", "hash"),
                    "ID", Integer.class);

                publicSchema.addTable(tbl.name(), tbl);
            }

            doTestCommuteDisabledForManyJoins(maxJoinsToOptimize + 1, false);

            doTestCommuteDisabledForManyJoins(maxJoinsToOptimize + 1, true);
        }
        finally {
            for (int i = 0; i < tablesCnt; ++i)
                publicSchema.removeTable("TBL" + i);
        }
    }

    /** */
    private void doTestCommuteDisabledForManyJoins(int joinsCnt, boolean addCorrelated) throws Exception {
        StringBuilder select = new StringBuilder();
        StringBuilder joins = new StringBuilder();

        for (int i = 0; i < joinsCnt + 1; ++i) {
            select.append("\nt").append(i).append(".id, ");

            if (i == 0)
                joins.append("TBL0 t0");
            else {
                joins.append(" LEFT JOIN TBL").append(i).append(" t").append(i).append(" ON t").append(i - 1).append(".id=t")
                    .append(i).append(".id");
            }
        }

        if (addCorrelated) {
            int tblNum = joinsCnt + 1;
            String alias = "t" + tblNum;

            select.append("\n(SELECT MAX(").append(alias).append(".id) FROM TBL").append(tblNum).append(" ").append(alias)
                .append(" WHERE ").append(alias).append(".id>").append("t0").append(".id + 1) as corr")
                .append(tblNum).append(", ");
        }

        String sql = "SELECT " + select.substring(0, select.length() - 2) + "\nFROM " + joins;

        PlanningContext ctx = plannerCtx(sql, publicSchema);

        RuleMatchVisualizer lsnr = new RuleMatchVisualizer();

        lsnr.attachTo(ctx.cluster().getPlanner());

        physicalPlan(ctx);

        String lsnrRes = lsnr.getJsonStringResult();

        assertFalse(lsnrRes.contains(CoreRules.JOIN_COMMUTE.toString()));
        assertFalse(lsnrRes.contains(JoinPushThroughJoinRule.LEFT.toString()));
        assertFalse(lsnrRes.contains(JoinPushThroughJoinRule.RIGHT.toString()));
    }

    /** */
    @Test
    public void testOuterCommute() throws Exception {
        String sql = "SELECT COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin");

        assertNotNull(phys);

        IgniteNestedLoopJoin join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        IgniteProject proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNotNull(proj);

        assertEquals(JoinRelType.LEFT, join.getJoinType());

        PlanningContext ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter",
            "CorrelatedNestedLoopJoin");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNull(proj);

        // no commute
        assertEquals(JoinRelType.RIGHT, join.getJoinType());

        ctx = plannerCtx(sql, publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));
    }

    /** */
    @Test
    public void testInnerCommute() throws Exception {
        String sql = "SELECT COUNT(*) FROM SMALL s JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin");

        assertNotNull(phys);

        IgniteNestedLoopJoin join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));
        IgniteProject proj = findFirstNode(phys, byClass(IgniteProject.class));

        IgniteTableScan rightScan = findFirstNode(join.getRight(), byClass(IgniteTableScan.class));
        IgniteTableScan leftScan = findFirstNode(join.getLeft(), byClass(IgniteTableScan.class));

        assertNotNull(join);
        assertNotNull(proj);
        assertNotNull(rightScan);
        assertNotNull(leftScan);

        List<String> rightSchemaWithName = rightScan.getTable().getQualifiedName();

        assertEquals(2, rightSchemaWithName.size());

        assertEquals(rightSchemaWithName.get(1), "SMALL");

        List<String> LeftSchemaWithName = leftScan.getTable().getQualifiedName();

        assertEquals(LeftSchemaWithName.get(1), "HUGE");

        assertEquals(JoinRelType.INNER, join.getJoinType());

        PlanningContext ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter",
            "CorrelatedNestedLoopJoin");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));
        proj = findFirstNode(phys, byClass(IgniteProject.class));

        rightScan = findFirstNode(join.getRight(), byClass(IgniteTableScan.class));
        leftScan = findFirstNode(join.getLeft(), byClass(IgniteTableScan.class));

        assertNotNull(join);
        assertNull(proj);
        assertNotNull(rightScan);
        assertNotNull(leftScan);

        rightSchemaWithName = rightScan.getTable().getQualifiedName();

        assertEquals(2, rightSchemaWithName.size());
        // no commute
        assertEquals(rightSchemaWithName.get(1), "HUGE");

        LeftSchemaWithName = leftScan.getTable().getQualifiedName();

        assertEquals(LeftSchemaWithName.get(1), "SMALL");

        // no commute
        assertEquals(JoinRelType.INNER, join.getJoinType());

        ctx = plannerCtx(sql, publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));
    }
}
