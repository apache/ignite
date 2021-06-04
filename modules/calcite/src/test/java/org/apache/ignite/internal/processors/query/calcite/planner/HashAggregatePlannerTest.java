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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 *
 */
public class HashAggregatePlannerTest extends AbstractAggregatePlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void subqueryWithAggregate() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable employer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("SALARY", f.createJavaType(Double.class))
                .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Employers", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("EMPS", employer);

        String sql = "SELECT * FROM emps WHERE emps.salary = (SELECT AVG(emps.salary) FROM emps)";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema
        );

        assertNotNull(phys);

        IgniteReduceHashAggregate rdcAgg = findFirstNode(phys, byClass(IgniteReduceHashAggregate.class));
        IgniteMapHashAggregate mapAgg = findFirstNode(phys, byClass(IgniteMapHashAggregate.class));

        assertNotNull(rdcAgg, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertNotNull(mapAgg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            first(rdcAgg.getAggregateCalls()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            first(mapAgg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));
    }

    /**
     *
     */
    @Test
    public void noGroupByAggregate() throws Exception {
        TestTable tbl = createAffinityTable().addIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "val0_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sqlCount = "SELECT COUNT(*) FROM test";

        IgniteRel phys = physicalPlan(
            sqlCount,
            publicSchema
        );

        IgniteMapHashAggregate mapAgg = findFirstNode(phys, byClass(IgniteMapHashAggregate.class));
        IgniteReduceHashAggregate rdcAgg = findFirstNode(phys, byClass(IgniteReduceHashAggregate.class));

        assertNotNull(rdcAgg, "Invalid plan\n" + RelOptUtil.toString(phys));
        assertNotNull(mapAgg, "Invalid plan\n" + RelOptUtil.toString(phys));

        assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            first(rdcAgg.getAggregateCalls()).getAggregation(),
            IsInstanceOf.instanceOf(SqlCountAggFunction.class));

        assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            first(mapAgg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlCountAggFunction.class));
    }
}
