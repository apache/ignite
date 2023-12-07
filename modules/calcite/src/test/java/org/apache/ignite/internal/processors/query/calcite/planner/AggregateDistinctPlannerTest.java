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
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class AggregateDistinctPlannerTest extends AbstractAggregatePlannerTest {
    /** Algorithm. */
    @Parameterized.Parameter
    public AggregateAlgorithm algo;

    /** */
    @Parameterized.Parameters(name = "Algorithm = {0}")
    public static List<Object[]> parameters() {
        return Stream.of(AggregateAlgorithm.values()).map(a -> new Object[]{a}).collect(Collectors.toList());
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void mapReduceDistinctWithIndex() throws Exception {
        TestTable tbl = createAffinityTable().addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT DISTINCT val0, val1 FROM test";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteAggregate mapAgg = findFirstNode(phys, byClass(algo.map));
        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES), rdcAgg);
        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), mapAgg);

        Assert.assertTrue(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.isEmpty(rdcAgg.getAggregateCalls()));

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
            IgniteColocatedSortAggregate.class,
            IgniteMapSortAggregate.class,
            IgniteReduceSortAggregate.class,
            "ColocatedHashAggregateConverterRule",
            "MapReduceHashAggregateConverterRule",
            "ColocatedSortAggregateConverterRule"
        ),

        /**
         *
         */
        HASH(
            IgniteColocatedHashAggregate.class,
            IgniteMapHashAggregate.class,
            IgniteReduceHashAggregate.class,
            "ColocatedSortAggregateConverterRule",
            "MapReduceSortAggregateConverterRule",
            "ColocatedHashAggregateConverterRule"
        );

        /** */
        public final Class<? extends IgniteColocatedAggregateBase> single;

        /** */
        public final Class<? extends IgniteMapAggregateBase> map;

        /** */
        public final Class<? extends IgniteReduceAggregateBase> reduce;

        /** */
        public final String[] rulesToDisable;

        /** */
        AggregateAlgorithm(
            Class<? extends IgniteColocatedAggregateBase> single,
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
