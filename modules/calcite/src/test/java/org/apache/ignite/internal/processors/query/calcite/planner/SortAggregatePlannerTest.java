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

import java.util.Arrays;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings({"FieldCanBeLocal"})
public class SortAggregatePlannerTest extends AbstractAggregatePlannerTest {
    /** Hash aggregate rules. */
    private static final String[] HASH_AGG_RULES =
        new String[] {"ColocatedHashAggregateConverterRule", "MapReduceHashAggregateConverterRule"};

    /**
     *
     */
    @Test
    public void notApplicableForSortAggregate() {
        TestTable tbl = createAffinityTable().addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sqlMin = "SELECT MIN(val0) FROM test";

        GridTestUtils.assertThrows(log,
            () -> physicalPlan(
                sqlMin,
                publicSchema,
                HASH_AGG_RULES
            ),
            RelOptPlanner.CannotPlanException.class,
            "There are not enough rules to produce a node with desired properties"
        );
    }

    /** Checks if already sorted input exist and involved [Map|Reduce]SortAggregate */
    @Test
    public void testNoSortAppendingWithCorrectCollation() throws Exception {
        RelFieldCollation coll = TraitUtils.createFieldCollation(1, false);

        TestTable tbl = createAffinityTable().addIndex(RelCollations.of(coll), "val0Idx");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            F.concat(HASH_AGG_RULES,
                "NestedLoopJoinConverter",
                "HashConverter",
                "CorrelatedNestedLoopJoin",
                "CorrelateToNestedLoopRule"
            )
        );

        assertTrue(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            findFirstNode(phys, byClass(IgniteSort.class)) == null &&
            findFirstNode(phys, byClass(IgniteTableScan.class)) == null
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void collationPermuteSingle() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        }
            .addIndex("grp0_1", 3, 4);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT MIN(val0) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            HASH_AGG_RULES
        );

        IgniteColocatedSortAggregate agg = findFirstNode(phys, byClass(IgniteColocatedSortAggregate.class));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), agg);

        assertNull(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            findFirstNode(phys, byClass(IgniteSort.class))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void collationPermuteMapReduce() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "test", "hash");
            }
        }
            .addIndex("grp0_1", 3, 4);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT MIN(val0) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            HASH_AGG_RULES
        );

        IgniteReduceSortAggregate agg = findFirstNode(phys, byClass(IgniteReduceSortAggregate.class));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), agg);

        assertNull(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            findFirstNode(phys, byClass(IgniteSort.class))
        );
    }

    /** */
    @Test
    public void testEmptyCollationPassThroughLimit() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable("TEST", IgniteDistributions.single(), "A", Integer.class));

        assertPlan("SELECT (SELECT test.a FROM test t ORDER BY 1 LIMIT 1) FROM test", publicSchema,
            hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteSort.class)))))))
        );
    }

    /** */
    @Test
    public void testCollationPassThrough() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable("TEST", IgniteDistributions.single(), "A", Integer.class, "B", Integer.class));

        // Sort order equals to grouping set.
        assertPlan("SELECT a, b, COUNT(*) FROM test GROUP BY a, b ORDER BY a, b", publicSchema,
            isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteSort.class)
                    .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(0, 1))))
                    .and(input(isTableScan("TEST"))))),
            HASH_AGG_RULES
        );

        // Sort order equals to grouping set (permuted collation).
        assertPlan("SELECT a, b, COUNT(*) FROM test GROUP BY a, b ORDER BY b, a", publicSchema,
            isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteSort.class)
                    .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(1, 0))))
                    .and(input(isTableScan("TEST"))))),
            HASH_AGG_RULES
        );

        // Sort order is a subset of grouping set.
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY a", publicSchema,
            isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteSort.class)
                    .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(0, 1))))
                    .and(input(isTableScan("TEST"))))),
            HASH_AGG_RULES
        );

        // Sort order is a subset of grouping set (permuted collation).
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY b", publicSchema,
            isInstanceOf(IgniteAggregate.class)
                .and(input(isInstanceOf(IgniteSort.class)
                    .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(1, 0))))
                    .and(input(isTableScan("TEST"))))),
            HASH_AGG_RULES
        );

        // Sort order is a superset of grouping set (additional sorting required).
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY a, b, cnt", publicSchema,
            isInstanceOf(IgniteSort.class)
                .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(0, 1, 2))))
                .and(input(isInstanceOf(IgniteAggregate.class)
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(0, 1))))
                        .and(input(isTableScan("TEST"))))))),
            HASH_AGG_RULES
        );

        // Sort order is not equals to grouping set (additional sorting required).
        assertPlan("SELECT a, b, COUNT(*) cnt FROM test GROUP BY a, b ORDER BY cnt, b", publicSchema,
            isInstanceOf(IgniteSort.class)
                .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(2, 1))))
                .and(input(isInstanceOf(IgniteAggregate.class)
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(s -> s.collation().equals(TraitUtils.createCollation(F.asList(0, 1))))
                        .and(input(isTableScan("TEST"))))))),
            HASH_AGG_RULES
        );
    }
}
