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

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
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
        TestTable tbl = createBroadcastTable().addIndex("val0_val1", 1, 2);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteColocatedAggregateBase agg = findFirstNode(phys, byClass(algo.colocated));

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
        TestTable tbl = createBroadcastTable().addIndex("grp0_grp1", 3, 4);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0) FILTER(WHERE val1 > 10) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, publicSchema);

        IgniteColocatedAggregateBase agg = findFirstNode(phys, byClass(algo.colocated));

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
        TestTable tbl = createAffinityTable().addIndex("grp0", 3);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT AVG(val0), grp0 FROM TEST GROUP BY grp0 UNION ALL SELECT val0, grp0 FROM test";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            F.concat(algo.rulesToDisable, "MapReduceSortAggregateConverterRule",
                "MapReduceHashAggregateConverterRule")
        );

        IgniteColocatedAggregateBase singleAgg = findFirstNode(phys, byClass(algo.colocated));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(singleAgg));

        phys = physicalPlan(
            sql,
            publicSchema,
            F.concat(algo.rulesToDisable, "ColocatedSortAggregateConverterRule",
                "ColocatedHashAggregateConverterRule")
        );

        IgniteReduceAggregateBase rdcAgg = findFirstNode(phys, byClass(algo.reduce));

        assertEquals(IgniteDistributions.single(), TraitUtils.distribution(rdcAgg));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void expandDistinctAggregates() throws Exception {
        TestTable tbl = createAffinityTable()
            .addIndex("idx_val0", 3, 1, 0)
            .addIndex("idx_val1", 3, 2, 0);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sql = "SELECT " +
            "/*+ EXPAND_DISTINCT_AGG */ " +
            "SUM(DISTINCT val0), AVG(DISTINCT val1) FROM test GROUP BY grp0";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            algo.rulesToDisable);

        checkSplitAndSerialization(phys, publicSchema);

        // Plan must not contain distinct accumulators.
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

        // Check the first aggrgation step is SELECT DISTINCT (doesn't contains any accumulators)
        assertTrue(
            "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES),
            findNodes(phys, byClass(algo.reduce)).stream()
                .allMatch(n -> ((IgniteReduceAggregateBase)n).getAggregateCalls().isEmpty())
        );
        assertTrue(
            "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES),
            findNodes(phys, byClass(algo.map)).stream()
                .allMatch(n -> ((IgniteMapAggregateBase)n).getAggCallList().isEmpty())
        );

        // Check the second aggrgation step contains accumulators.
        assertTrue(
            "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES),
            findNodes(phys, byClass(algo.colocated)).stream()
                .noneMatch(n -> ((IgniteColocatedAggregateBase)n).getAggCallList().isEmpty())
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void singleSumTypes() throws Exception {
        IgniteSchema schema = createSchema(
            createTable(
                "TEST", IgniteDistributions.broadcast(),
                "ID", Integer.class,
                "GRP", Integer.class,
                "VAL_TINYINT", Byte.class,
                "VAL_SMALLINT", Short.class,
                "VAL_INT", Integer.class,
                "VAL_BIGINT", Long.class,
                "VAL_DECIMAL", BigDecimal.class,
                "VAL_FLOAT", Float.class,
                "VAL_DOUBLE", Double.class
            )
        );

        String sql = "SELECT " +
            "SUM(VAL_TINYINT), " +
            "SUM(VAL_SMALLINT), " +
            "SUM(VAL_INT), " +
            "SUM(VAL_BIGINT), " +
            "SUM(VAL_DECIMAL), " +
            "SUM(VAL_FLOAT), " +
            "SUM(VAL_DOUBLE) " +
            "FROM test GROUP BY grp";

        IgniteRel phys = physicalPlan(
            sql,
            schema,
            algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, schema);

        IgniteColocatedAggregateBase agg = findFirstNode(phys, byClass(algo.colocated));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), agg);

        RelDataType rowTypes = agg.getRowType();

        RelDataTypeFactory tf = phys.getCluster().getTypeFactory();

        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(1).getType());
        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(2).getType());
        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(3).getType());
        assertEquals(tf.createJavaType(BigDecimal.class), rowTypes.getFieldList().get(4).getType());
        assertEquals(tf.createJavaType(BigDecimal.class), rowTypes.getFieldList().get(5).getType());
        assertEquals(tf.createJavaType(Double.class), rowTypes.getFieldList().get(6).getType());
        assertEquals(tf.createJavaType(Double.class), rowTypes.getFieldList().get(7).getType());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void mapReduceSumTypes() throws Exception {
        IgniteSchema schema = createSchema(
            createTable(
                "TEST", IgniteDistributions.random(),
                "ID", Integer.class,
                "GRP", Integer.class,
                "VAL_TINYINT", Byte.class,
                "VAL_SMALLINT", Short.class,
                "VAL_INT", Integer.class,
                "VAL_BIGINT", Long.class,
                "VAL_DECIMAL", BigDecimal.class,
                "VAL_FLOAT", Float.class,
                "VAL_DOUBLE", Double.class
            )
        );

        String sql = "SELECT " +
            "SUM(VAL_TINYINT), " +
            "SUM(VAL_SMALLINT), " +
            "SUM(VAL_INT), " +
            "SUM(VAL_BIGINT), " +
            "SUM(VAL_DECIMAL), " +
            "SUM(VAL_FLOAT), " +
            "SUM(VAL_DOUBLE) " +
            "FROM test GROUP BY grp";

        IgniteRel phys = physicalPlan(
            sql,
            schema,
            algo.rulesToDisable
        );

        checkSplitAndSerialization(phys, schema);

        IgniteReduceAggregateBase agg = findFirstNode(phys, byClass(algo.reduce));

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), agg);

        RelDataType rowTypes = agg.getRowType();

        RelDataTypeFactory tf = phys.getCluster().getTypeFactory();

        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(1).getType());
        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(2).getType());
        assertEquals(tf.createJavaType(Long.class), rowTypes.getFieldList().get(3).getType());
        assertEquals(tf.createJavaType(BigDecimal.class), rowTypes.getFieldList().get(4).getType());
        assertEquals(tf.createJavaType(BigDecimal.class), rowTypes.getFieldList().get(5).getType());
        assertEquals(tf.createJavaType(Double.class), rowTypes.getFieldList().get(6).getType());
        assertEquals(tf.createJavaType(Double.class), rowTypes.getFieldList().get(7).getType());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void colocated() throws Exception {
        IgniteSchema schema = createSchema(
            createTable(
                "EMP", IgniteDistributions.affinity(1, "emp", "hash"),
                "EMPID", Integer.class,
                "DEPTID", Integer.class,
                "NAME", String.class,
                "SALARY", Integer.class
            ).addIndex("DEPTID", 1),
            createTable(
                "DEPT", IgniteDistributions.affinity(0, "dept", "hash"),
                "DEPTID", Integer.class,
                "NAME", String.class
            ).addIndex("DEPTID", 0)
        );

        String sql = "SELECT SUM(SALARY) FROM emp GROUP BY deptid";

        assertPlan(sql, schema, hasChildThat(isInstanceOf(algo.colocated)
            .and(hasDistribution(IgniteDistributions.affinity(0, null, "hash")))),
            algo.rulesToDisable);

        sql = "SELECT dept.deptid, agg.cnt " +
            "FROM dept " +
            "JOIN (SELECT deptid, COUNT(*) AS cnt FROM emp GROUP BY deptid) AS agg ON dept.deptid = agg.deptid";

        assertPlan(sql, schema, hasChildThat(isInstanceOf(Join.class)
            .and(input(0, hasDistribution(IgniteDistributions.affinity(0, null, "hash"))))
            .and(input(1, hasDistribution(IgniteDistributions.affinity(0, null, "hash"))))),
            algo.rulesToDisable);
    }

    /** */
    enum AggregateAlgorithm {
        /** */
        SORT(
            IgniteColocatedSortAggregate.class,
            IgniteMapSortAggregate.class,
            IgniteReduceSortAggregate.class,
            "ColocatedHashAggregateConverterRule", "MapReduceHashAggregateConverterRule"
        ),

        /** */
        HASH(
            IgniteColocatedHashAggregate.class,
            IgniteMapHashAggregate.class,
            IgniteReduceHashAggregate.class,
            "ColocatedSortAggregateConverterRule", "MapReduceSortAggregateConverterRule"
        );

        /** */
        public final Class<? extends IgniteColocatedAggregateBase> colocated;

        /** */
        public final Class<? extends IgniteMapAggregateBase> map;

        /** */
        public final Class<? extends IgniteReduceAggregateBase> reduce;

        /** */
        public final String[] rulesToDisable;

        /** */
        AggregateAlgorithm(
            Class<? extends IgniteColocatedAggregateBase> colocated,
            Class<? extends IgniteMapAggregateBase> map,
            Class<? extends IgniteReduceAggregateBase> reduce,
            String... rulesToDisable) {
            this.colocated = colocated;
            this.map = map;
            this.reduce = reduce;
            this.rulesToDisable = rulesToDisable;
        }
    }
}
