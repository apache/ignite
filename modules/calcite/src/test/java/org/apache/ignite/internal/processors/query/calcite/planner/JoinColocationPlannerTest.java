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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.hasSize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test suite to verify join colocation.
 */
public class JoinColocationPlannerTest extends AbstractPlannerTest {
    /** */
    private static final IgniteTypeFactory TYPE_FACTORY = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    /** */
    private static final int DEFAULT_TBL_SIZE = 500_000;

    /**
     * Join of the same tables with a simple affinity is expected to be colocated.
     */
    @Test
    public void joinSameTableSimpleAff() throws Exception {
        TestTable tbl = createTable(
            "TEST_TBL",
            IgniteDistributions.affinity(0, "default", "hash"),
            "ID", Integer.class,
            "VAL", String.class
        );

        tbl.addIndex(new IgniteIndex(RelCollations.of(0), "PK", null, tbl));

        IgniteSchema schema = createSchema(tbl);

        String sql = "select count(*) " +
            "from TEST_TBL t1 " +
            "join TEST_TBL t2 on t1.id = t2.id";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin");

        IgniteMergeJoin join = findFirstNode(phys, byClass(IgniteMergeJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().function().affinity(), is(true));
        assertThat(invalidPlanMsg, join.getLeft(), instanceOf(IgniteIndexScan.class));
        assertThat(invalidPlanMsg, join.getRight(), instanceOf(IgniteIndexScan.class));
    }

    /**
     * Join of the same tables with a complex affinity is expected to be colocated.
     */
    @Test
    public void joinSameTableComplexAff() throws Exception {
        TestTable tbl = createTable(
            "TEST_TBL",
            IgniteDistributions.affinity(ImmutableIntList.of(0, 1), CU.cacheId("default"), "hash"),
            "ID1", Integer.class,
            "ID2", Integer.class,
            "VAL", String.class
        );

        tbl.addIndex(new IgniteIndex(RelCollations.of(ImmutableIntList.of(0, 1)), "PK", null, tbl));

        IgniteSchema schema = createSchema(tbl);

        String sql = "select count(*) " +
            "from TEST_TBL t1 " +
            "join TEST_TBL t2 on t1.id1 = t2.id1 and t1.id2 = t2.id2";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin");

        IgniteMergeJoin join = findFirstNode(phys, byClass(IgniteMergeJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().function().affinity(), is(true));
        assertThat(invalidPlanMsg, join.getLeft(), instanceOf(IgniteIndexScan.class));
        assertThat(invalidPlanMsg, join.getRight(), instanceOf(IgniteIndexScan.class));
    }

    /**
     * Re-hashing based on simple affinity is possible, so bigger table with complex affinity
     * should be sended to the smaller one.
     */
    @Test
    public void joinComplexToSimpleAff() throws Exception {
        TestTable complexTbl = createTable(
            "COMPLEX_TBL",
            2 * DEFAULT_TBL_SIZE,
            IgniteDistributions.affinity(ImmutableIntList.of(0, 1), CU.cacheId("default"), "hash"),
            "ID1", Integer.class,
            "ID2", Integer.class,
            "VAL", String.class
        );

        complexTbl.addIndex(new IgniteIndex(RelCollations.of(ImmutableIntList.of(0, 1)), "PK", null, complexTbl));

        TestTable simpleTbl = createTable(
            "SIMPLE_TBL",
            DEFAULT_TBL_SIZE,
            IgniteDistributions.affinity(0, "default", "hash"),
            "ID", Integer.class,
            "VAL", String.class
        );

        simpleTbl.addIndex(new IgniteIndex(RelCollations.of(0), "PK", null, simpleTbl));

        IgniteSchema schema = createSchema(complexTbl, simpleTbl);

        String sql = "select count(*) " +
            "from COMPLEX_TBL t1 " +
            "join SIMPLE_TBL t2 on t1.id1 = t2.id";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin");

        IgniteMergeJoin join = findFirstNode(phys, byClass(IgniteMergeJoin.class));

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, join, notNullValue());
        assertThat(invalidPlanMsg, join.distribution().function().affinity(), is(true));

        List<IgniteExchange> exchanges = findNodes(phys, node -> node instanceof IgniteExchange
            && ((IgniteRel)node).distribution().function().affinity());

        assertThat(invalidPlanMsg, exchanges, hasSize(1));
        assertThat(invalidPlanMsg, exchanges.get(0).getInput(0), instanceOf(IgniteIndexScan.class));
        assertThat(invalidPlanMsg, exchanges.get(0).getInput(0)
            .getTable().unwrap(TestTable.class), equalTo(complexTbl));
    }

    /**
     * Re-hashing for complex affinity is not supported.
     */
    @Test
    public void joinComplexToComplexAffWithDifferentOrder() throws Exception {
        TestTable complexTblDirect = createTable(
            "COMPLEX_TBL_DIRECT",
            IgniteDistributions.affinity(ImmutableIntList.of(0, 1), CU.cacheId("default"), "hash"),
            "ID1", Integer.class,
            "ID2", Integer.class,
            "VAL", String.class
        );

        complexTblDirect.addIndex(new IgniteIndex(RelCollations.of(ImmutableIntList.of(0, 1)), "PK", null, complexTblDirect));

        TestTable complexTblIndirect = createTable(
            "COMPLEX_TBL_INDIRECT",
            IgniteDistributions.affinity(ImmutableIntList.of(1, 0), CU.cacheId("default"), "hash"),
            "ID1", Integer.class,
            "ID2", Integer.class,
            "VAL", String.class
        );

        complexTblIndirect.addIndex(new IgniteIndex(RelCollations.of(ImmutableIntList.of(0, 1)), "PK", null, complexTblIndirect));

        IgniteSchema schema = createSchema(complexTblDirect, complexTblIndirect);

        String sql = "select count(*) " +
            "from COMPLEX_TBL_DIRECT t1 " +
            "join COMPLEX_TBL_INDIRECT t2 on t1.id1 = t2.id1 and t1.id2 = t2.id2";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin");

        IgniteMergeJoin exchange = findFirstNode(phys, node -> node instanceof IgniteExchange
            && ((IgniteRel)node).distribution().function().affinity());

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, exchange, nullValue());
    }

    /**
     * Creates test table with given params.
     *
     * @param name Name of the table.
     * @param distr Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string
     *               representing a column name, every even item should be a class representing column's type.
     *               E.g. {@code createTable("MY_TABLE", distribution, "ID", Integer.class, "VAL", String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    private static TestTable createTable(String name, IgniteDistribution distr, Object... fields) {
        return createTable(name, DEFAULT_TBL_SIZE, distr, fields);
    }

    /**
     * Creates test table with given params.
     *
     * @param name Name of the table.
     * @param size Required size of the table.
     * @param distr Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string
     *               representing a column name, every even item should be a class representing column's type.
     *               E.g. {@code createTable("MY_TABLE", 500, distribution, "ID", Integer.class, "VAL", String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    private static TestTable createTable(String name, int size, IgniteDistribution distr, Object... fields) {
        if (F.isEmpty(fields) || fields.length % 2 != 0)
            throw new IllegalArgumentException("'fields' should be non-null array with even number of elements");

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(TYPE_FACTORY);

        for (int i = 0; i < fields.length; i += 2)
            b.add((String)fields[i], TYPE_FACTORY.createJavaType((Class<?>)fields[i + 1]));

        return new TestTable(name, b.build(), RewindabilityTrait.REWINDABLE, size) {
            @Override public IgniteDistribution distribution() {
                return distr;
            }
        };
    }

    /**
     * Creates public schema from provided tables.
     *
     * @param tbls Tables to create schema for.
     * @return Public schema.
     */
    private static IgniteSchema createSchema(TestTable... tbls) {
        IgniteSchema schema = new IgniteSchema("PUBLIC");

        for (TestTable tbl : tbls)
            schema.addTable(tbl.name(), tbl);

        return schema;
    }
}
