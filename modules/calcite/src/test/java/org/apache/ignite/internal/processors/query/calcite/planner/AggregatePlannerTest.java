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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class AggregatePlannerTest extends AbstractAggregatePlannerTest {
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
        TestTable tbl = createBroadcastTable().addIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "val0_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable
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
        TestTable tbl = createBroadcastTable().addIndex(RelCollations.of(ImmutableIntList.of(3, 4)), "grp0_grp1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER(WHERE val1 > 10) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable
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
        TestTable tbl = createAffinityTable();

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER (WHERE val1 > 10) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteMapAggregateBase mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES), rdcAgg);
        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), mapAgg);

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(rdcAgg.getAggregateCalls()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(mapAgg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        if (algo == AggregateAlgorithm.SORT)
            assertNotNull(findFirstNode(phys, byClass(IgniteSort.class)));
    }

    /**
     * Test that aggregate has single distribution output even if parent node accept random distibution inputs.
     *
     * @throws Exception If failed.
     */
    @Test
    public void distribution() throws Exception {
        TestTable tbl = createAffinityTable().addIndex(RelCollations.of(ImmutableIntList.of(3)), "grp0");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0), grp0 FROM TEST GROUP BY grp0 UNION ALL SELECT val0, grp0 FROM test";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            F.concat(algo.rulesToDisable, "SortMapReduceAggregateConverterRule",
                "HashMapReduceAggregateConverterRule")
        );

        IgniteSingleAggregateBase singleAgg = findFirstNode(phys, byClass(algo.single));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(singleAgg));

        phys = physicalPlan(
            sql,
            publicSchema,
            F.concat(algo.rulesToDisable, "SortSingleAggregateConverterRule",
                "HashSingleAggregateConverterRule")
        );

        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(rdcAgg));
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void expandDistinctAggregates() throws Exception {
        TestTable tbl = createAffinityTable()
            .addIndex(RelCollations.of(ImmutableIntList.of(3, 1, 0)), "idx_val0")
            .addIndex(RelCollations.of(ImmutableIntList.of(3, 2, 0)), "idx_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        assertFalse(
            "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES),
            findNodes(phys, byClass(IgniteAggregate.class)).stream()
                .anyMatch(n -> ((Aggregate)n).getAggCallList().stream()
                    .anyMatch(AggregateCall::isDistinct)
                )
        );

        assertNotNull(
            "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES),
            findFirstNode(phys, byClass(Join.class)));
    }

    /** */
    enum AggregateAlgorithm {
        /** */
        SORT(
            IgniteSingleSortAggregate.class,
            IgniteMapSortAggregate.class,
            IgniteReduceSortAggregate.class,
            "HashSingleAggregateConverterRule", "HashMapReduceAggregateConverterRule"
        ),

        /** */
        HASH(
            IgniteSingleHashAggregate.class,
            IgniteMapHashAggregate.class,
            IgniteReduceHashAggregate.class,
            "SortSingleAggregateConverterRule", "SortMapReduceAggregateConverterRule"
        );

        /** */
        public final Class<? extends IgniteSingleAggregateBase> single;

        /** */
        public final Class<? extends IgniteMapAggregateBase> map;

        /** */
        public final Class<? extends IgniteReduceAggregateBase> reduce;

        /** */
        public final String[] rulesToDisable;

        /** */
        AggregateAlgorithm(
            Class<? extends IgniteSingleAggregateBase> single,
            Class<? extends IgniteMapAggregateBase> map,
            Class<? extends IgniteReduceAggregateBase> reduce,
            String... rulesToDisable) {
            this.single = single;
            this.map = map;
            this.reduce = reduce;
            this.rulesToDisable = rulesToDisable;
        }
    }
}
