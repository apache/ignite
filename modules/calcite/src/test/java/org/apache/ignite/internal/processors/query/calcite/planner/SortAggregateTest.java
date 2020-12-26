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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.CollocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings({"TypeMayBeWeakened"})
public class SortAggregateTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleGroupBy() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public CollocationGroup collocationGroup(PlanningContext ctx) {
                return CollocationGroup.forAssignments(Arrays.asList(
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

        String sql = "SELECT AVG(val0) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "HashAggregateConverterRule"
        );

        assertNotNull(phys);

//        IgniteReduceAggregate rdcAgg = findFirstNode(phys, byClass(IgniteReduceAggregate.class));
//        IgniteMapAggregate mapAgg = findFirstNode(phys, byClass(IgniteMapAggregate.class));
//
//        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), rdcAgg);
//        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), mapAgg);
//
//        Assert.assertThat(
//            "Invalid plan\n" + RelOptUtil.toString(phys),
//            F.first(rdcAgg.aggregateCalls()).getAggregation(),
//            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));
//
//        Assert.assertThat(
//            "Invalid plan\n" + RelOptUtil.toString(phys),
//            F.first(mapAgg.getAggCallList()).getAggregation(),
//            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        System.out.println("+++\n" + RelOptUtil.toString(phys));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void simpleDistinct() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public CollocationGroup collocationGroup(PlanningContext ctx) {
                return CollocationGroup.forAssignments(Arrays.asList(
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
            "HashAggregateConverterRule"
        );

        assertNotNull(phys);

        System.out.println("+++\n" + RelOptUtil.toString(phys));
    }

    /** */
    @Test
    public void notApplicableAggregate() {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("VAL0", f.createJavaType(Integer.class))
                .add("VAL1", f.createJavaType(Integer.class))
                .add("GRP0", f.createJavaType(Integer.class))
                .add("GRP1", f.createJavaType(Integer.class))
                .build()) {

            @Override public CollocationGroup collocationGroup(PlanningContext ctx) {
                return CollocationGroup.forAssignments(Arrays.asList(
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

        String sql = "SELECT MIN(val0) FROM test";

        GridTestUtils.assertThrows(log,
            () -> physicalPlan(
                sql,
                publicSchema,
                "HashAggregateConverterRule"
            ),
            RelOptPlanner.CannotPlanException.class,
            "There are not enough rules to produce a node with desired properties"
        );
    }

}
