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

    /** */
    @Test
    public void testOuterCommute() throws Exception {
        String sql = "SELECT COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin",
            "HashJoinConverter");

        assertNotNull(phys);

        IgniteNestedLoopJoin join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        IgniteProject proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNotNull(proj);

        assertEquals(JoinRelType.LEFT, join.getJoinType());

        PlanningContext ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "HashJoinConverter",
            "CorrelatedNestedLoopJoin");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter",
            "JoinCommuteRule");

        join = findFirstNode(phys, byClass(IgniteNestedLoopJoin.class));

        proj = findFirstNode(phys, byClass(IgniteProject.class));

        assertNull(proj);

        // no commute
        assertEquals(JoinRelType.RIGHT, join.getJoinType());

        ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "HashJoinConverter",
            "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));
    }

    /** */
    @Test
    public void testInnerCommute() throws Exception {
        String sql = "SELECT COUNT(*) FROM SMALL s JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "HashJoinConverter", "CorrelatedNestedLoopJoin");

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

        PlanningContext ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "HashJoinConverter",
            "CorrelatedNestedLoopJoin");

        RelOptPlanner pl = ctx.cluster().getPlanner();

        RelOptCost costWithCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(sql, publicSchema, "MergeJoinConverter", "HashJoinConverter", "CorrelatedNestedLoopJoin",
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

        ctx = plannerCtx(sql, publicSchema, "MergeJoinConverter", "HashJoinConverter", "CorrelatedNestedLoopJoin",
            "JoinCommuteRule");

        pl = ctx.cluster().getPlanner();

        RelOptCost costWithoutCommute = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("plan: " + RelOptUtil.toString(phys));

        assertTrue(costWithCommute.isLt(costWithoutCommute));
    }
}
