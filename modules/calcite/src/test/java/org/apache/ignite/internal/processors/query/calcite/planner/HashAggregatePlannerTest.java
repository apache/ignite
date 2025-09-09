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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexBound;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
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

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.createFieldCollation;

/**
 *
 */
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public class HashAggregatePlannerTest extends AbstractAggregatePlannerTest {
    /** */
    @Test
    public void indexMinMax() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        RelCollation[] collations = new RelCollation[] {
            RelCollations.of(createFieldCollation(1, true)),
            RelCollations.of(createFieldCollation(1, false)),
            RelCollations.of(createFieldCollation(1, true), createFieldCollation(2, true)),
            RelCollations.of(createFieldCollation(1, false), createFieldCollation(2, false)),
            RelCollations.of(createFieldCollation(1, true), createFieldCollation(2, false)),
            RelCollations.of(createFieldCollation(1, false), createFieldCollation(2, true))
        };

        for (RelCollation cll : collations) {
            for (IgniteDistribution distr : distributions()) {
                TestTable tbl = createTable(distr);
                publicSchema.addTable("TEST", tbl);

                assertNoIndexFirstOrLastRecord("SELECT MIN(VAL0) FROM TEST", publicSchema);
                assertNoIndexFirstOrLastRecord("SELECT MIN(VAL0) FROM TEST", publicSchema);

                tbl.addIndex(cll, "TEST_IDX");

                boolean targetFldIdxAcc = !cll.getFieldCollations().get(0).direction.isDescending();

                assertIndexFirstOrLastRecord("SELECT MIN(VAL0) FROM TEST", targetFldIdxAcc, publicSchema);
                assertIndexFirstOrLastRecord("SELECT MAX(VAL0) FROM TEST", !targetFldIdxAcc, publicSchema);
                assertIndexFirstOrLastRecord("SELECT MIN(V) FROM (SELECT MIN(VAL0) AS V FROM TEST)",
                    targetFldIdxAcc, publicSchema);
                assertIndexFirstOrLastRecord("SELECT MAX(V) FROM (SELECT MAX(VAL0) AS V FROM TEST)",
                    !targetFldIdxAcc, publicSchema);
                assertIndexFirstOrLastRecord("SELECT MAX(VAL0) FROM TEST", !targetFldIdxAcc, publicSchema);

                assertNoIndexFirstOrLastRecord("SELECT MIN(VAL0) FROM TEST GROUP BY GRP0", publicSchema);
                assertNoIndexFirstOrLastRecord("SELECT MIN(VAL0) FROM TEST GROUP BY GRP0, GRP1 ORDER BY GRP1 DESC",
                    publicSchema);

                assertNoIndexFirstOrLastRecord("SELECT MIN(VAL1) FROM TEST", publicSchema);
                assertNoIndexFirstOrLastRecord("SELECT MAX(VAL1) FROM TEST", publicSchema);

                assertNoIndexFirstOrLastRecord("SELECT MIN(VAL0) FROM TEST WHERE VAL1 > 1", publicSchema);
                assertNoIndexFirstOrLastRecord("SELECT MAX(VAL0) FROM TEST WHERE VAL1 > 1", publicSchema);
                assertNoIndexFirstOrLastRecord("SELECT VAL1, MIN(VAL0) FROM TEST GROUP BY VAL1", publicSchema);
                assertNoIndexFirstOrLastRecord("SELECT VAL1, MAX(VAL0) FROM TEST GROUP BY VAL1", publicSchema);

                assertNoIndexFirstOrLastRecord("SELECT MIN(VAL0 + 1) FROM TEST", publicSchema);
                assertNoIndexFirstOrLastRecord("SELECT MAX(VAL0 + 1) FROM TEST", publicSchema);

                publicSchema.removeTable("TEST");
            }
        }
    }

    /** */
    private void assertIndexFirstOrLastRecord(String sql, boolean first, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteAggregate.class)
            .and(nodeOrAnyChild(isInstanceOf(IgniteIndexBound.class)
                .and(s -> first && s.first() || !first && !s.first())))));
    }

    /** */
    private void assertNoIndexFirstOrLastRecord(String sql, IgniteSchema publicSchema) throws Exception {
        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteIndexBound.class)).negate());
    }

    /**
     * Tests COUNT(...) plan with and without IndexCount optimization.
     */
    @Test
    public void indexCount() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        for (IgniteDistribution distr : distributions()) {
            TestTable tbl = createTable(distr);
            publicSchema.addTable("TEST", tbl);

            assertNoIndexCount("SELECT COUNT(*) FROM TEST", publicSchema);

            tbl.addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0);

            assertIndexCount("SELECT COUNT(*) FROM TEST", publicSchema);
            assertIndexCount("SELECT COUNT(1) FROM TEST", publicSchema);
            assertIndexCount("SELECT COUNT(*) FROM (SELECT * FROM TEST)", publicSchema);
            assertIndexCount("SELECT COUNT(1) FROM (SELECT * FROM TEST)", publicSchema);

            assertIndexCount("SELECT COUNT(*), COUNT(*), COUNT(1) FROM TEST", publicSchema);

            assertIndexCount("SELECT COUNT(ID) FROM TEST", publicSchema);
            assertNoIndexCount("SELECT COUNT(ID + 1) FROM TEST", publicSchema);
            assertNoIndexCount("SELECT COUNT(VAL0) FROM TEST", publicSchema);

            tbl.addIndex("TEST_IDX", 1, 2);

            assertIndexCount("SELECT COUNT(VAL0) FROM TEST", publicSchema);
            assertNoIndexCount("SELECT COUNT(DISTINCT VAL0) FROM TEST", publicSchema);

            assertNoIndexCount("SELECT COUNT(*), COUNT(VAL0) FROM TEST", publicSchema);

            assertNoIndexCount("SELECT COUNT(1), COUNT(VAL0) FROM TEST", publicSchema);
            assertNoIndexCount("SELECT COUNT(DISTINCT 1), COUNT(VAL0) FROM TEST", publicSchema);

            assertNoIndexCount("SELECT COUNT(*) FILTER (WHERE VAL0>1) FROM TEST", publicSchema);

            // IndexCount can't be used with a condition, groups, other aggregates or distincts.
            assertNoIndexCount("SELECT COUNT(*) FROM TEST WHERE VAL0>1", publicSchema);
            assertNoIndexCount("SELECT COUNT(1) FROM TEST WHERE VAL0>1", publicSchema);

            assertNoIndexCount("SELECT COUNT(*), SUM(VAL0) FROM TEST", publicSchema);

            assertNoIndexCount("SELECT VAL0, COUNT(*) FROM TEST GROUP BY VAL0", publicSchema);

            assertNoIndexCount("SELECT COUNT(*) FROM TEST GROUP BY VAL0", publicSchema);

            assertNoIndexCount("SELECT COUNT(*) FILTER (WHERE VAL0>1) FROM TEST", publicSchema);

            publicSchema.addTable("TEST2", createBroadcastTable());

            assertNoIndexCount("SELECT COUNT(*) FROM (SELECT T1.VAL0, T2.VAL1 FROM TEST T1, " +
                "TEST2 T2 WHERE T1.GRP0 = T2.GRP0)", publicSchema);

            publicSchema.removeTable("TEST");
        }
    }

    /** */
    private static List<IgniteDistribution> distributions() {
        return ImmutableList.of(
            IgniteDistributions.single(),
            IgniteDistributions.random(),
            IgniteDistributions.broadcast(),
            IgniteDistributions.hash(ImmutableList.of(0, 1, 2, 3)));
    }

    /** */
    private void assertIndexCount(String sql, IgniteSchema schema) throws Exception {
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteAggregate.class)
            .and(nodeOrAnyChild(isInstanceOf(IgniteIndexCount.class)))));
    }

    /** */
    private void assertNoIndexCount(String sql, IgniteSchema publicSchema) throws Exception {
        assertPlan(sql, publicSchema, hasChildThat(isInstanceOf(IgniteIndexCount.class)).negate());
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

        String sqlCnt = "SELECT COUNT(*) FROM test";

        IgniteRel phys = physicalPlan(
            sqlCnt,
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
