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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.ignite.internal.processors.query.calcite.metadata.CollocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregateHash;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregateHash;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public class HashAggregateTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable employer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("SALARY", f.createJavaType(Double.class))
                .build()) {

            @Override public CollocationGroup colocationGroup(PlanningContext ctx) {
                return CollocationGroup.forAssignments(Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ));
            }

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

        IgniteReduceAggregateHash rdcAgg = findFirstNode(phys, byClass(IgniteReduceAggregateHash.class));
        IgniteMapAggregateHash mapAgg = findFirstNode(phys, byClass(IgniteMapAggregateHash.class));

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

        System.out.println("+++\n" + RelOptUtil.toString(phys));
    }
}
