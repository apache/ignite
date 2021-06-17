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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Test suite to verify join colocation.
 */
public class DmlPlannerTest extends AbstractPlannerTest {
    /** */
    private static final IgniteTypeFactory TYPE_FACTORY = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    /** */
    private static final int DEFAULT_TBL_SIZE = 500_000;

    /**
     * Join of the same tables with a simple affinity is expected to be colocated.
     */
    @Test
    public void test() throws Exception {
        TestTable tbl = createTable(
                "TEST_TBL",
                IgniteDistributions.affinity(0, "default", "hash"),
                "_KEY", Object.class,
                "_VAL", Object.class,
                "VAL", Integer.class
        );

        IgniteSchema schema = createSchema(tbl);

        String sql = "UPDATE TEST_TBL SET val = val + 2";

        RelNode phys = physicalPlan(sql, schema, "NestedLoopJoinConverter", "CorrelatedNestedLoopJoin");

        System.out.println(RelOptUtil.toString(phys));
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
