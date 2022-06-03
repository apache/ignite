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
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.jdbc.JdbcQueryTest;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
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
public class HashAggregatePlannerTest extends AbstractAggregatePlannerTest {
    /**
     * Tests COUNT(...) plan with and without IndexCount optimization.
     *
     * @see JdbcQueryTest#testIndexCount()
     */
    @Test
    public void indexCount() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        TestTable tbl = createBroadcastTable();
        publicSchema.addTable("TEST", tbl);

        assertPlan("SELECT COUNT(*) FROM TEST", publicSchema,
            hasChildThat(isInstanceOf(IgniteIndexCount.class)).negate());

        // Check with 'primary' index.
        tbl.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0);

        assertIndexCount("SELECT COUNT(*) FROM TEST", publicSchema);

        // Check with other index.
        tbl.removeIndex(QueryUtils.PRIMARY_KEY_INDEX);

        tbl.addIndex("idx", 1);

        assertIndexCount("SELECT COUNT(*) FROM TEST", publicSchema);

        assertIndexCount("SELECT COUNT(*) FROM (SELECT * FROM TEST)", publicSchema);

        // Check caount on certain field.
        assertIndexCount("SELECT COUNT(VAL0) FROM TEST", publicSchema);

        // Check with several 'count()'.
        assertIndexCount("SELECT COUNT(*), COUNT(*) FROM TEST", publicSchema);
        assertIndexCount("SELECT COUNT(VAL0), COUNT(VAL0) FROM TEST", publicSchema);
        assertIndexCount("SELECT COUNT(VAL0), COUNT(VAL1) FROM TEST", publicSchema);
        assertIndexCount("SELECT COUNT(*), COUNT(VAL0), COUNT(VAL1) FROM TEST", publicSchema);
        assertIndexCount("SELECT COUNT(*), COUNT(VAL0), COUNT(VAL1) FROM (SELECT * FROM TEST)", publicSchema);

        // IndexCount can't be used with a condition, groups or other aggregates.
        assertPlan("SELECT COUNT(*) FROM TEST WHERE VAL0>1", publicSchema,
            hasChildThat(isInstanceOf(IgniteIndexCount.class)).negate());

        assertPlan("SELECT COUNT(*), SUM(VAL0) FROM TEST", publicSchema,
            hasChildThat(isInstanceOf(IgniteIndexCount.class)).negate());

        assertPlan("SELECT VAL0, COUNT(*) FROM TEST GROUP BY VAL0", publicSchema,
            hasChildThat(isInstanceOf(IgniteIndexCount.class)).negate());

        assertPlan("SELECT COUNT(*) FROM TEST GROUP BY VAL0", publicSchema,
            hasChildThat(isInstanceOf(IgniteIndexCount.class)).negate());

        publicSchema.addTable("TEST2", createBroadcastTable());

        assertPlan("SELECT COUNT(*) FROM (SELECT T1.VAL0, T2.VAL1 FROM TEST T1, TEST2 T2 WHERE T1.GRP0 = T2.GRP0)",
            publicSchema, hasChildThat(isInstanceOf(IgniteIndexCount.class)).negate());
    }

    /** */
    private void assertIndexCount(String sql, IgniteSchema publicSchema) throws Exception {
        assertPlan(
            sql,
            publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteColocatedHashAggregate.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteIndexCount.class)))))));
    }

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

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), rdcAgg);
        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), mapAgg);

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(rdcAgg.getAggregateCalls()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(mapAgg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlAvgAggFunction.class));
    }

    /**
     *
     */
    @Test
    public void noGroupByAggregate() throws Exception {
        TestTable tbl = createAffinityTable().addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sqlCount = "SELECT COUNT(*) FROM test";

        IgniteRel phys = physicalPlan(
            sqlCount,
            publicSchema
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteMapHashAggregate mapAgg = findFirstNode(phys, byClass(IgniteMapHashAggregate.class));
        IgniteReduceHashAggregate rdcAgg = findFirstNode(phys, byClass(IgniteReduceHashAggregate.class));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), rdcAgg);
        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), mapAgg);

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(rdcAgg.getAggregateCalls()).getAggregation(),
            IsInstanceOf.instanceOf(SqlCountAggFunction.class));

        Assert.assertThat(
            "Invalid plan\n" + RelOptUtil.toString(phys),
            F.first(mapAgg.getAggCallList()).getAggregation(),
            IsInstanceOf.instanceOf(SqlCountAggFunction.class));
    }
}
