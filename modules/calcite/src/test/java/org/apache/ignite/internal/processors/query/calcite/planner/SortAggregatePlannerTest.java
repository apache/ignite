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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class SortAggregatePlannerTest extends AbstractAggregatePlannerTest {
    /**
     *
     */
    @Test
    public void notApplicableForSortAggregate() {
        TestTable tbl = createAffinityTable().addIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "val0_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sqlMin = "SELECT MIN(val0) FROM test";

        RelOptPlanner.CannotPlanException ex = assertThrows(
                RelOptPlanner.CannotPlanException.class,
                () -> physicalPlan(
                        sqlMin,
                        publicSchema,
                        "HashSingleAggregateConverterRule", "HashMapReduceAggregateConverterRule"
                )
        );

        assertThat(ex.getMessage(), startsWith("There are not enough rules to produce a node with desired properties"));
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

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        }
                .addIndex(RelCollations.of(ImmutableIntList.of(3, 4)), "grp0_1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT MIN(val0) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "HashSingleAggregateConverterRule", "HashMapReduceAggregateConverterRule"
        );

        IgniteSingleSortAggregate agg = findFirstNode(phys, byClass(IgniteSingleSortAggregate.class));

        assertNotNull(agg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertNull(
                findFirstNode(phys, byClass(IgniteSort.class)),
                "Invalid plan\n" + RelOptUtil.toString(phys)
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

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "test", "hash");
            }
        }
                .addIndex(RelCollations.of(ImmutableIntList.of(3, 4)), "grp0_1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT MIN(val0) FROM test GROUP BY grp1, grp0";

        IgniteRel phys = physicalPlan(
                sql,
                publicSchema,
                "HashSingleAggregateConverterRule", "HashMapReduceAggregateConverterRule"
        );

        IgniteReduceSortAggregate agg = findFirstNode(phys, byClass(IgniteReduceSortAggregate.class));

        assertNotNull(agg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertNull(
                findFirstNode(phys, byClass(IgniteSort.class)),
                "Invalid plan\n" + RelOptUtil.toString(phys)
        );
    }
}
