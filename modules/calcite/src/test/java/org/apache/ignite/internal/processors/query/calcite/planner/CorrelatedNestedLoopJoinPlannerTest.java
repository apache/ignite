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

package org.apache.ignite.internal.processors.query.calcite.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class CorrelatedNestedLoopJoinPlannerTest extends AbstractPlannerTest {
    /**
     * Check equi-join. CorrelatedNestedLoopJoinTest is applicable for it.
     */
    @Test
    public void testValidIndexExpressions() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
                "T0",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.broadcast();
                    }
                }
        );

        publicSchema.addTable(
                "T1",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.broadcast();
                    }
                }
                        .addIndex(RelCollations.of(ImmutableIntList.of(1, 0)), "t1_jid_idx")
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter"
        );

        System.out.println("+++ " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        List<RexNode> lowerBound = idxScan.lowerBound();

        assertNotNull(lowerBound, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertEquals(3, lowerBound.size());

        assertTrue(((RexLiteral) lowerBound.get(0)).isNull());
        assertTrue(((RexLiteral) lowerBound.get(2)).isNull());
        assertTrue(lowerBound.get(1) instanceof RexFieldAccess);

        List<RexNode> upperBound = idxScan.upperBound();

        assertNotNull(upperBound, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertEquals(3, upperBound.size());

        assertTrue(((RexLiteral) upperBound.get(0)).isNull());
        assertTrue(((RexLiteral) upperBound.get(2)).isNull());
        assertTrue(upperBound.get(1) instanceof RexFieldAccess);
    }

    /**
     * Check join with not equi condition. Current implementation of the CorrelatedNestedLoopJoinTest is not applicable for such case.
     */
    @Test
    public void testInvalidIndexExpressions() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
                "T0",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.broadcast();
                    }
                }
                        .addIndex(RelCollations.of(ImmutableIntList.of(1, 0)), "t0_jid_idx")
        );

        publicSchema.addTable(
                "T1",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("JID", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.broadcast();
                    }
                }
                        .addIndex(RelCollations.of(ImmutableIntList.of(1, 0)), "t1_jid_idx")
        );

        String sql = "select * "
                + "from t0 "
                + "join t1 on t0.jid + 2 > t1.jid * 2";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeRule"
        );

        assertNotNull(phys);
    }
}
