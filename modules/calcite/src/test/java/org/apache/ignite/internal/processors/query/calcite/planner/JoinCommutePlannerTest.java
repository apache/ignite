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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.visualizer.RuleMatchVisualizer;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.ignite.internal.processors.query.calcite.RuleApplyListener;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteMultiJoinOptimizeRule;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/** Tests correctness applying of JOIN_COMMUTE* and {@link IgniteMultiJoinOptimizeRule} rules. */
public class JoinCommutePlannerTest extends AbstractPlannerTest {
    /** */
    private static IgniteSchema publicSchema;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteDistribution distr = IgniteDistributions.affinity(0, "HUGE", "hash");

        publicSchema = createSchema(
            createTable("SMALL", 10, distr, "ID", Integer.class),
            createTable("AVERAGE", 100, distr, "ID", Integer.class),
            createTable("HUGE", 1_000, distr, "ID", Integer.class)
        );
    }

    /**
     * Ensures that join commute rules are disabled if joins count is too high.
     *
     * @see JoinCommuteRule
     * @see JoinPushThroughJoinRule
     */
    @Test
    public void testCommuteDisabledForManyJoins() throws Exception {
        int maxJoinsToOptimize = Math.max(PlannerHelper.MAX_JOINS_TO_COMMUTE_INPUTS, PlannerHelper.MAX_JOINS_TO_COMMUTE);
        int minJoins = Math.min(PlannerHelper.MAX_JOINS_TO_COMMUTE_INPUTS, PlannerHelper.MAX_JOINS_TO_COMMUTE);

        // +2 tables because we use 1 more join in the query and 1 extra table for the correlated query.
        int tablesCnt = maxJoinsToOptimize + 3;

        try {
            for (int i = 0; i < tablesCnt; ++i) {
                TestTable tbl = createTable("TBL" + i, (int)Math.pow(100, (1 + (i % 3))),
                    IgniteDistributions.affinity(0, "TEST_CACHE", "hash"),
                    "ID", Integer.class);

                publicSchema.addTable(tbl.name(), tbl);
            }

            // Rule names to check.
            Collection<RelOptRule> commuteJoins = Collections.singletonList(CoreRules.JOIN_COMMUTE);
            Collection<RelOptRule> commuteSubInputs = Stream.of(JoinPushThroughJoinRule.LEFT,
                JoinPushThroughJoinRule.RIGHT).collect(Collectors.toSet());
            Collection<RelOptRule> allRules = Stream.concat(commuteJoins.stream(), commuteSubInputs.stream()).collect(Collectors.toSet());

            // With the minimal joins number all the rules are expected to launch.
            checkJoinCommutes(minJoins, false, true, allRules);
            checkJoinCommutes(minJoins, true, true, allRules);

            // Checks only JoinCommuteRule rule.
            checkJoinCommutes(PlannerHelper.MAX_JOINS_TO_COMMUTE, false, true, commuteJoins);
            checkJoinCommutes(PlannerHelper.MAX_JOINS_TO_COMMUTE, true, true, commuteJoins);
            checkJoinCommutes(PlannerHelper.MAX_JOINS_TO_COMMUTE + 1, false, false, commuteJoins);

            // Checks only JoinPushThroughJoinRule rules.
            checkJoinCommutes(PlannerHelper.MAX_JOINS_TO_COMMUTE_INPUTS, false, true, commuteSubInputs);
            checkJoinCommutes(PlannerHelper.MAX_JOINS_TO_COMMUTE_INPUTS, true, true, commuteSubInputs);
            checkJoinCommutes(PlannerHelper.MAX_JOINS_TO_COMMUTE_INPUTS + 1, false, false, commuteSubInputs);

            // From this joins count all the rules must not work.
            checkJoinCommutes(maxJoinsToOptimize + 1, false, false, allRules);
        }
        finally {
            for (int i = 0; i < tablesCnt; ++i)
                publicSchema.removeTable("TBL" + i);
        }
    }

    /** */
    private void checkJoinCommutes(int jCnt, boolean addCorrelated, boolean rulesExpected, Collection<RelOptRule> rules) throws Exception {
        StringBuilder select = new StringBuilder();
        StringBuilder joins = new StringBuilder();

        for (int i = 0; i < jCnt + 1; ++i) {
            select.append("\nt").append(i).append(".id, ");

            if (i == 0)
                joins.append("TBL0 t0");
            else {
                joins.append(" LEFT JOIN TBL").append(i).append(" t").append(i).append(" ON t").append(i - 1).append(".id=t")
                    .append(i).append(".id");
            }
        }

        if (addCorrelated) {
            int tblNum = jCnt + 1;
            String alias = "t" + tblNum;

            select.append("\n(SELECT MAX(").append(alias).append(".id) FROM TBL").append(tblNum).append(" ").append(alias)
                .append(" WHERE ").append(alias).append(".id>").append("t0").append(".id + 1) as corr")
                .append(tblNum).append(", ");
        }

        String sql = "SELECT " + select.substring(0, select.length() - 2) + "\nFROM " + joins;

        // The join order optimization should not work. It disables the default join commute rules too.
        RuleApplyListener orderOptmzLsnr = new RuleApplyListener(IgniteMultiJoinOptimizeRule.INSTANCE);

        PlanningContext ctx = plannerCtx(sql, publicSchema, orderOptmzLsnr);

        RuleMatchVisualizer commuteLsnr = new RuleMatchVisualizer() {
            @Override public void ruleAttempted(RuleAttemptedEvent evt) {
                if (rules.contains(evt.getRuleCall().getRule()) && !evt.isBefore())
                    assertTrue(rulesExpected);

                super.ruleAttempted(evt);
            }
        };

        commuteLsnr.attachTo(ctx.cluster().getPlanner());
        ctx.cluster().getPlanner().addListener(commuteLsnr);

        physicalPlan(ctx);

        assertFalse(orderOptmzLsnr.ruleSucceeded());
    }

    /** */
    @Test
    public void testOuterCommute() throws Exception {
        RuleApplyListener planLsnr = new RuleApplyListener(IgniteMultiJoinOptimizeRule.INSTANCE);

        String sql = "SELECT COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema, planLsnr, "MergeJoinConverter", "CorrelatedNestedLoopJoin",
            "HashJoinConverter");

        assertNotNull(phys);

        IgniteNestedLoopJoin join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        IgniteProject proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNotNull(proj);

        assertEquals(JoinRelType.LEFT, join.getJoinType());

        PlanningContext ctx = plannerCtx(sql, publicSchema, planLsnr, "MergeJoinConverter", "CorrelatedNestedLoopJoin",
            "HashJoinConverter");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema, planLsnr, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter",
            "JoinCommuteRule");

        join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNull(proj);

        // no commute
        assertEquals(JoinRelType.RIGHT, join.getJoinType());

        ctx = plannerCtx(sql, publicSchema, planLsnr, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter",
            "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));

        assertFalse(planLsnr.ruleSucceeded());
    }

    /** Tests that {@link IgniteMultiJoinOptimizeRule} is enabled if joins number reaches the threshold. */
    @Test
    public void testHeuristicReorderingIsEnabledByJoinsCount() throws Exception {
        assert PlannerHelper.JOINS_COUNT_FOR_HEURISTIC_ORDER >= 3;

        String sql = "SELECT COUNT(*) FROM SMALL s JOIN HUGE h on h.id = s.id JOIN AVERAGE a on a.id = h.id";

        RuleApplyListener planLsnr = new RuleApplyListener(IgniteMultiJoinOptimizeRule.INSTANCE);

        physicalPlan(sql, publicSchema, planLsnr);

        assertFalse(planLsnr.ruleSucceeded());

        // Joins number is enough. But the optimization is disabled by the hint.
        sql = "SELECT /*+ ENFORCE_JOIN_ORDER */ s.id, h.id, a.id FROM SMALL s JOIN HUGE h on h.id = s.id " +
            "JOIN AVERAGE a on a.id = h.id JOIN SMALL ss on ss.id = a.id";

        physicalPlan(sql, publicSchema, planLsnr);

        assertFalse(planLsnr.ruleSucceeded());

        // Joins number is enough. But the optimization is disabled by the hint for some joins.
        sql = "SELECT s.id, j.b, j.c FROM SMALL s JOIN " +
            "(SELECT /*+ ENFORCE_JOIN_ORDER */ h.id as a, a.id as b, s2.id as c FROM HUGE h JOIN AVERAGE a on h.id=a.id " +
            "JOIN SMALL s2 ON a.id=s2.id) j ON s.id=j.a";

        physicalPlan(sql, publicSchema, planLsnr);

        assertFalse(planLsnr.ruleSucceeded());

        // Joins number is enough and nothing is disabled.
        sql = "SELECT s.id, j.b, j.c FROM SMALL s JOIN " +
            "(SELECT h.id as a, a.id as b, s2.id as c FROM HUGE h JOIN AVERAGE a on h.id=a.id " +
            "JOIN SMALL s2 ON a.id=s2.id) j ON s.id=j.a";

        physicalPlan(sql, publicSchema, planLsnr);

        assertTrue(planLsnr.ruleSucceeded());

        // Joins number is enough and nothing is disabled.
        sql = "SELECT s.id, h.id, a.id FROM SMALL s JOIN HUGE h on h.id = s.id JOIN AVERAGE a on a.id = h.id " +
            "JOIN SMALL ss on ss.id = a.id";

        physicalPlan(sql, publicSchema, planLsnr);

        assertTrue(planLsnr.ruleSucceeded());
    }

    /** */
    @Test
    public void testInnerCommute() throws Exception {
        RuleApplyListener planLsnr = new RuleApplyListener(IgniteMultiJoinOptimizeRule.INSTANCE);

        String sql = "SELECT COUNT(*) FROM SMALL s JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema, planLsnr, "MergeJoinConverter", "CorrelatedNestedLoopJoin",
            "HashJoinConverter");

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

        PlanningContext ctx = plannerCtx(sql, publicSchema, planLsnr, "MergeJoinConverter", "CorrelatedNestedLoopJoin",
            "HashJoinConverter");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema, planLsnr, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter",
            "JoinCommuteRule");

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

        ctx = plannerCtx(sql, publicSchema, planLsnr "MergeJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter",
            "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));

        assertFalse(planLsnr.ruleSucceeded());
    }
}
