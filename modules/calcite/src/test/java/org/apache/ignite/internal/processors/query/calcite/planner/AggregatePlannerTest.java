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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSingleAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSingleHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSingleSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@SuppressWarnings({"TypeMayBeWeakened"})
@RunWith(Parameterized.class)
public class AggregatePlannerTest extends AbstractPlannerTest {
    /** Algorithm. */
    @Parameterized.Parameter
    public AggregateAlgorithm algo;

    /** */
    @Parameterized.Parameters(name = "Algorithm = {0}")
    public static List<Object[]> parameters() {
        return Stream.of(AggregateAlgorithm.values()).map(a -> new Object[]{a}).collect(Collectors.toList());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void singleWithoutIndex() throws Exception {
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
            .addIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "val0_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisableOtherAlgorithm
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteSingleAggregateBase agg = findFirstNode(phys, byClass(algo.single));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), agg);

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(agg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT)
            assertNotNull(findFirstNode(phys, byClass(IgniteSort.class)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void singleWithIndex() throws Exception {
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
            .addIndex(RelCollations.of(ImmutableIntList.of(3, 4)), "grp0_grp1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER(WHERE val1 > 10) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisableOtherAlgorithm
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteSingleAggregateBase agg = findFirstNode(phys, byClass(algo.single));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), agg);

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(agg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT)
            assertNotNull(findFirstNode(phys, byClass(IgniteIndexScan.class)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void mapReduceGroupBy() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
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
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER (WHERE val1 > 10) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisableOtherAlgorithm
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteMapAggregateBase mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES), rdcAgg);
        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), mapAgg);

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(rdcAgg.aggregateCalls()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(mapAgg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT)
            assertNotNull(findFirstNode(phys, byClass(IgniteSort.class)));
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void mapReduceDistinctWithIndex() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
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
            .addIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "val0_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT DISTINCT val0, val1 FROM test";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisableOtherAlgorithmAndSingle
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteAggregate mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES), rdcAgg);
        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), mapAgg);

        Assert.assertTrue(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.isEmpty(rdcAgg.aggregateCalls()));

        Assert.assertTrue(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.isEmpty(mapAgg.getAggCallList()));

        if (algo == AggregateAlgorithm.SORT)
            assertNotNull(findFirstNode(phys, byClass(IgniteIndexScan.class)));
    }

    /** */
    enum AggregateAlgorithm {
        /** */
        SORT(
            IgniteSingleSortAggregate.class,
            IgniteMapSortAggregate.class,
            IgniteReduceSortAggregate.class,
            new String[] {"HashSingleAggregateConverterRule", "HashMapReduceAggregateConverterRule"},
            new String[] {"HashSingleAggregateConverterRule", "HashMapReduceAggregateConverterRule",
                "SortSingleAggregateConverterRule"}
        ),

        /** */
        HASH(
            IgniteSingleHashAggregate.class,
            IgniteMapHashAggregate.class,
            IgniteReduceHashAggregate.class,
            new String[] {"SortSingleAggregateConverterRule", "SortMapReduceAggregateConverterRule"},
            new String[] {"SortSingleAggregateConverterRule", "SortMapReduceAggregateConverterRule",
                "HashSingleAggregateConverterRule"}
        );

        /** */
        public final Class<? extends IgniteSingleAggregateBase> single;

        /** */
        public final Class<? extends IgniteMapAggregateBase> map;

        /** */
        public final Class<? extends IgniteReduceAggregateBase> reduce;

        /** */
        public final String[] rulesToDisableOtherAlgorithm;

        /** */
        public final String[] rulesToDisableOtherAlgorithmAndSingle;

        /** */
        AggregateAlgorithm(
            Class<? extends IgniteSingleAggregateBase> single,
            Class<? extends IgniteMapAggregateBase> map,
            Class<? extends IgniteReduceAggregateBase> reduce,
            String[] rulesToDisableOtherAlgorithm,
            String[] rulesToDisableOtherAlgorithmAndSingle) {
            this.single = single;
            this.map = map;
            this.reduce = reduce;
            this.rulesToDisableOtherAlgorithm = rulesToDisableOtherAlgorithm;
            this.rulesToDisableOtherAlgorithmAndSingle = rulesToDisableOtherAlgorithmAndSingle;
        }
    }
}
