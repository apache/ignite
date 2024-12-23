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
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Test;

/**
 *
 */
public class HashIndexSpoolPlannerTest extends AbstractPlannerTest {
    /**
     * Check equi-join on not colocated fields.
     * CorrelatedNestedLoopJoinTest is applicable for this case only with IndexSpool.
     */
    @Test
    public void testSingleKey() throws Exception {
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
                    return IgniteDistributions.affinity(0, "T0", "hash");
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
                    return IgniteDistributions.affinity(0, "T1", "hash");
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
            "MergeJoinConverter", "HashJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToSortedIndexSpoolRule"
        );

        System.out.println("+++\n" + RelOptUtil.toString(phys));

        checkSplitAndSerialization(phys, publicSchema);

        IgniteHashIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteHashIndexSpool.class));

        List<RexNode> searchRow = idxSpool.searchRow();

        assertNotNull(searchRow);
        assertEquals(3, searchRow.size());

        assertNull(searchRow.get(0));
        assertTrue(searchRow.get(1) instanceof RexFieldAccess);
        assertNull(searchRow.get(2));
    }

    /** */
    @Test
    public void testMultipleKeys() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
            "T0",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .add("JID0", f.createJavaType(Integer.class))
                    .add("JID1", f.createJavaType(Integer.class))
                    .add("VAL", f.createJavaType(String.class))
                    .build()) {

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.affinity(0, "T0", "hash");
                }
            }
        );

        publicSchema.addTable(
            "T1",
            new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .add("JID0", f.createJavaType(Integer.class))
                    .add("JID1", f.createJavaType(Integer.class))
                    .add("VAL", f.createJavaType(String.class))
                    .build()) {

                @Override public IgniteDistribution distribution() {
                    return IgniteDistributions.affinity(0, "T1", "hash");
                }
            }
                .addIndex("t1_jid0_idx", 1, 0)
        );

        String sql = "select * " +
            "from t0 " +
            "join t1 on t0.jid0 = t1.jid0 and t0.jid1 = t1.jid1";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "NestedLoopJoinConverter", "HashJoinConverter", "FilterSpoolMergeToSortedIndexSpoolRule"
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteHashIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteHashIndexSpool.class));

        List<RexNode> searchRow = idxSpool.searchRow();

        assertNotNull(searchRow);
        assertEquals(4, searchRow.size());

        assertNull(searchRow.get(0));
        assertTrue(searchRow.get(1) instanceof RexFieldAccess);
        assertTrue(searchRow.get(2) instanceof RexFieldAccess);
        assertNull(searchRow.get(3));
    }

    /**
     * Check equi-join on not colocated fields without indexes.
     */
    @Test
    public void testSourceWithoutCollation() throws Exception {
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
                    return IgniteDistributions.affinity(0, "T0", "hash");
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
                    return IgniteDistributions.affinity(0, "T1", "hash");
                }
            }
        );

        String sql = "select * " +
            "from t0 " +
            "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "NestedLoopJoinConverter", "HashJoinConverter"
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteHashIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteHashIndexSpool.class));

        List<RexNode> searchRow = idxSpool.searchRow();

        assertNotNull(searchRow);
        assertEquals(3, searchRow.size());

        assertNull(searchRow.get(0));
        assertTrue(searchRow.get(1) instanceof RexFieldAccess);
        assertNull(searchRow.get(2));
    }

    /**
     * Test that hash spool can be used with not correlated condition (condition is pushed below the spool).
     */
    @Test
    public void testCorrelatedFilterSplit() throws Exception {
        TestTable tbl = createTable("TBL", IgniteDistributions.random(), "ID", Integer.class);
        IgniteSchema publicSchema = createSchema(tbl);

        String sql = "SELECT (SELECT id FROM tbl AS t2 WHERE t2.id < 50 AND t2.id = t1.id) FROM tbl AS t1";

        assertPlan(sql, publicSchema,
            hasChildThat(isInstanceOf(IgniteHashIndexSpool.class)
                .and(s -> "=($0, $cor0.ID)".equals(s.condition().toString()))
                .and(hasChildThat(isInstanceOf(IgniteTableScan.class)
                    .and(t -> "<($t0, 50)".equals(t.condition().toString()))))));
    }
}
