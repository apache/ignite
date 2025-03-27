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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
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
            "AVERAGE",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .build(), 100) {

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

    /** */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        lsnrLog.clearListeners();

        ((GridTestLog4jLogger)log).setLevel(Level.INFO);
    }

    /** */
    @Test
    public void testOuterCommute() throws Exception {
        LogListener lsnr = LogListener.matches("Joins order optimization took").times(0).build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

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

        assertTrue(lsnr.check());
    }

    /** Tests that {@link IgniteMultiJoinOptimizeRule} is enabled if joins number reaches the threshold. */
    @Test
    public void testHeuristicReorderingIsEnabledByJoinsCount() throws Exception {
        assert PlannerHelper.JOINS_COUNT_FOR_HEURISTIC_ORDER >= 3;

        String sql = "SELECT COUNT(*) FROM SMALL s JOIN HUGE h on h.id = s.id JOIN AVERAGE a on a.id = h.id";

        LogListener lsnr = LogListener.matches("Joins order optimization took").build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        physicalPlan(sql, publicSchema);

        assertFalse(lsnr.check());

        // Joins number is enough. But the optimization is disabled by the hint.
        sql = "SELECT /*+ ENFORCE_JOIN_ORDER */ s.id, h.id, a.id FROM SMALL s JOIN HUGE h on h.id = s.id " +
            "JOIN AVERAGE a on a.id = h.id JOIN SMALL ss on ss.id = a.id";
        physicalPlan(sql, publicSchema);
        assertFalse(lsnr.check());

        // Joins number is enough. But the optimization is disabled by the hint for some joins.
        sql = "SELECT s.id, j.b, j.c FROM SMALL s JOIN " +
            "(SELECT /*+ ENFORCE_JOIN_ORDER */ h.id as a, a.id as b, s2.id as c FROM HUGE h JOIN AVERAGE a on h.id=a.id " +
            "JOIN SMALL s2 ON a.id=s2.id) j ON s.id=j.a";

        physicalPlan(sql, publicSchema);

        assertFalse(lsnr.check());

        // Joins number is enough and nothing is disabled.
        sql = "SELECT s.id, j.b, j.c FROM SMALL s JOIN " +
            "(SELECT h.id as a, a.id as b, s2.id as c FROM HUGE h JOIN AVERAGE a on h.id=a.id " +
            "JOIN SMALL s2 ON a.id=s2.id) j ON s.id=j.a";

        physicalPlan(sql, publicSchema);

        assertTrue(lsnr.check());

        // Joins number is enough and nothing is disabled.
        sql = "SELECT s.id, h.id, a.id FROM SMALL s JOIN HUGE h on h.id = s.id JOIN AVERAGE a on a.id = h.id " +
            "JOIN SMALL ss on ss.id = a.id";

        lsnr.reset();

        physicalPlan(sql, publicSchema);

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testInnerCommute() throws Exception {
        LogListener lsnr = LogListener.matches("Joins order optimization took").times(0).build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

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

        assertTrue(lsnr.check());
    }
}
