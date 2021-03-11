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
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@SuppressWarnings({"TypeMayBeWeakened"})
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
            algo.ruleToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteAggregateBase agg = findFirstNode(phys, byClass(algo.single));

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

        TestTable tbl = createBroadcastTable().addIndex(RelCollations.of(ImmutableIntList.of(3, 4)), "grp0_grp1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER(WHERE val1 > 10) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.ruleToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteAggregateBase agg = findFirstNode(phys, byClass(algo.single));

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
            algo.ruleToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteAggregate mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), rdcAgg);
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
    @Ignore("Single aggregates must be disabled by hint: https://issues.apache.org/jira/browse/IGNITE-14274")
    public void mapReduceDistinctWithIndex() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = createAffinityTable().addIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "val0_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT DISTINCT val0, val1 FROM test";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.ruleToDisable
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
            IgniteSortAggregate.class,
            IgniteMapSortAggregate.class,
            IgniteReduceSortAggregate.class,
            "HashAggregateConverterRule"
        ),

        /** */
        HASH(
            IgniteHashAggregate.class,
            IgniteMapHashAggregate.class,
            IgniteReduceHashAggregate.class,
            "SortAggregateConverterRule"
        );

        /** */
        public final Class<? extends IgniteAggregateBase> single;

        /** */
        public final Class<? extends IgniteAggregate> map;

        /** */
        public final Class<? extends IgniteReduceAggregateBase> reduce;

        /** */
        public final String ruleToDisable;

        /** */
        AggregateAlgorithm(
            Class<? extends IgniteAggregateBase> single,
            Class<? extends IgniteAggregate> map,
            Class<? extends IgniteReduceAggregateBase> reduce,
            String ruleToDisable) {
            this.single = single;
            this.map = map;
            this.reduce = reduce;
            this.ruleToDisable = ruleToDisable;
        }
    }
}
