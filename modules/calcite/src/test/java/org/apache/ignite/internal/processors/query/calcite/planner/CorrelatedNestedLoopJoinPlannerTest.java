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

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
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

                @Override public IgniteDistribution distribution() {
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

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.broadcast();
                }
            }
                .addIndex("t1_jid_idx", 1, 0)
        );

        String sql = "select * " +
            "from t0 " +
            "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "NestedLoopJoinConverter"
        );

        System.out.println("+++ " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        checkSplitAndSerialization(phys, publicSchema);

        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        List<SearchBounds> searchBounds = idxScan.searchBounds();

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), searchBounds);
        assertEquals(3, searchBounds.size());

        assertNull(searchBounds.get(0));
        assertTrue(searchBounds.get(1) instanceof ExactBounds);
        assertTrue(((ExactBounds)searchBounds.get(1)).bound() instanceof RexFieldAccess);
        assertNull(searchBounds.get(2));
    }

    /**
     * Check join with not equi condition.
     * Current implementation of the CorrelatedNestedLoopJoinTest is not applicable for such case.
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

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.broadcast();
                }
            }
                .addIndex("t0_jid_idx", 1, 0)
        );

        publicSchema.addTable(
            "T1",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .add("JID", f.createJavaType(Integer.class))
                    .add("VAL", f.createJavaType(String.class))
                    .build()) {

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.broadcast();
                }
            }
                .addIndex("t1_jid_idx", 1, 0)
        );

        String sql = "select * " +
            "from t0 " +
            "join t1 on t0.jid + 2 > t1.jid * 2";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeRule"
        );

        assertNotNull(phys);

        checkSplitAndSerialization(phys, publicSchema);
    }
}
