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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** */
public class JoinCommutePlannerTest extends AbstractPlannerTest {
    @Test
    public void test0() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
            "HUGE",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .build(), RewindabilityTrait.REWINDABLE, 1_000) {

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
                    .build(), RewindabilityTrait.REWINDABLE, 10) {

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.affinity(0, "SMALL", "hash");
                }
            }
        );

        String sql = "SELECT COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin"
        );

        RelOptPlanner pl = planner(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin");

        @Nullable RelOptCost cost1 = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("+++ " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule"
        );

        pl = planner(sql, publicSchema, "MergeJoinConverter", "CorrelatedNestedLoopJoin", "JoinCommuteRule");

        @Nullable RelOptCost cost2 = pl.getCost(phys, phys.getCluster().getMetadataQuery());

        System.out.println("+++ " + RelOptUtil.toString(phys));

        System.out.println("cost+++ " + cost1.isLt(cost2));

/*        checkSplitAndSerialization(phys, publicSchema);

        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        List<RexNode> lBound = idxScan.lowerBound();

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), lBound);
        assertEquals(3, lBound.size());

        assertTrue(((RexLiteral)lBound.get(0)).isNull());
        assertTrue(((RexLiteral)lBound.get(2)).isNull());
        assertTrue(lBound.get(1) instanceof RexFieldAccess);

        List<RexNode> uBound = idxScan.upperBound();

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), uBound);
        assertEquals(3, uBound.size());

        assertTrue(((RexLiteral)uBound.get(0)).isNull());
        assertTrue(((RexLiteral)uBound.get(2)).isNull());
        assertTrue(uBound.get(1) instanceof RexFieldAccess);*/
    }
}
