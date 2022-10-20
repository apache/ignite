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
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class SortedIndexSpoolPlannerTest extends AbstractPlannerTest {
    /**
     * Check equi-join on not colocated fields.
     * CorrelatedNestedLoopJoinTest is applicable for this case only with IndexSpool.
     */
    @Test
    public void testNotColocatedEqJoin() throws Exception {
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
            "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteSortedIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteSortedIndexSpool.class));

        List<SearchBounds> searchBounds = idxSpool.searchBounds();

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), searchBounds);
        assertEquals(3, searchBounds.size());

        assertNull(searchBounds.get(0));
        assertTrue(searchBounds.get(1) instanceof ExactBounds);
        assertTrue(((ExactBounds)searchBounds.get(1)).bound() instanceof RexFieldAccess);
        assertNull(searchBounds.get(2));
    }

    /**
     * Check case when exists index (collation) isn't applied not for whole join condition
     * but may be used by part of condition.
     */
    @Test
    public void testPartialIndexForCondition() throws Exception {
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
            "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );

        System.out.println("+++ \n" + RelOptUtil.toString(phys));

        checkSplitAndSerialization(phys, publicSchema);

        IgniteSortedIndexSpool idxSpool = findFirstNode(phys, byClass(IgniteSortedIndexSpool.class));

        List<SearchBounds> searchBounds = idxSpool.searchBounds();

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), searchBounds);
        assertEquals(4, searchBounds.size());

        assertNull(searchBounds.get(0));
        assertTrue(searchBounds.get(1) instanceof ExactBounds);
        assertTrue(((ExactBounds)searchBounds.get(1)).bound() instanceof RexFieldAccess);
        assertNull(searchBounds.get(2));
        assertNull(searchBounds.get(3));
    }

    /**
     * Check colocated fields with DESC ordering.
     */
    @Test
    public void testDescFields() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable("T0", 10, IgniteDistributions.affinity(0, "T0", "hash"),
                "ID", Integer.class, "JID", Integer.class, "VAL", String.class)
                .addIndex("t0_jid_idx", 1),
            createTable("T1", 100, IgniteDistributions.affinity(0, "T1", "hash"),
                "ID", Integer.class, "JID", Integer.class, "VAL", String.class)
                .addIndex(RelCollations.of(TraitUtils.createFieldCollation(1, false)), "t1_jid_idx")
        );

        String sql = "select * " +
            "from t0 " +
            "join t1 on t1.jid < t0.jid";

        assertPlan(sql, publicSchema,
            isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                .and(input(1, isInstanceOf(IgniteSortedIndexSpool.class)
                    .and(spool -> {
                        List<SearchBounds> searchBounds = spool.searchBounds();

                        // Condition is LESS_THEN, but we have DESC field and condition should be in lower bound
                        // instead of upper bound.
                        assertNotNull(searchBounds);
                        assertEquals(3, searchBounds.size());

                        assertNull(searchBounds.get(0));
                        assertTrue(searchBounds.get(1) instanceof RangeBounds);
                        RangeBounds fld1Bounds = (RangeBounds)searchBounds.get(1);
                        assertTrue(fld1Bounds.lowerBound() instanceof RexFieldAccess);
                        assertFalse(fld1Bounds.lowerInclude());
                        // NULLS LAST in collation, so nulls can be skipped by upper bound.
                        assertEquals("$NULL_BOUND()", fld1Bounds.upperBound().toString());
                        assertFalse(fld1Bounds.upperInclude());
                        assertNull(searchBounds.get(2));

                        return true;
                    })
                    .and(hasChildThat(isIndexScan("T1", "t1_jid_idx")))
                )),
            "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
        );
    }

    /**
     * Check sorted spool without input collation.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-16430")
    public void testRestoreCollation() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable("T0", 100, IgniteDistributions.random(),
                "I0", Integer.class, "I1", Integer.class, "I2", Integer.class, "I3", Integer.class),
            createTable("T1", 10000, IgniteDistributions.random(),
                "I0", Integer.class, "I1", Integer.class, "I2", Integer.class, "I3", Integer.class)
        );

        for (int i = 0; i < 4; i++) {
            final int equalIdx = i;

            String[] conds = IntStream.range(0, 4)
                .mapToObj(idx -> "t0.i" + idx + ((idx == equalIdx) ? "=" : ">") + "t1.i" + idx)
                .toArray(String[]::new);

            String sql = "select * from t0 join t1 on " + String.join(" and ", conds);

            assertPlan(sql, publicSchema,
                hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                    .and(input(1, isInstanceOf(IgniteSortedIndexSpool.class)
                        .and(spool -> spool.collation().getFieldCollations().get(0).getFieldIndex() == equalIdx)
                    ))),
                "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToHashIndexSpoolRule"
            );
        }
    }
}
