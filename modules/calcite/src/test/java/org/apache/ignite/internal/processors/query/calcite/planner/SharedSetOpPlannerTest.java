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
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Test;

/**
 * Planner test for UNION/EXCEPT/INTERSECT.
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public class SharedSetOpPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnion() throws Exception {
        IgniteSchema publicSchema = prepareSchema();

        String sql = "" +
            "SELECT * FROM table1 " +
            "UNION " +
            "SELECT * FROM table2 " +
            "UNION " +
            "SELECT * FROM table3 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceAggregateBase.class)
            .and(hasChildThat(isInstanceOf(Union.class)
                    .and(input(0, isTableScan("TABLE1")))
                    .and(input(1, isTableScan("TABLE2")))
                    .and(input(2, isTableScan("TABLE3")))
            )));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnionAll() throws Exception {
        IgniteSchema publicSchema = prepareSchema();

        String sql = "" +
            "SELECT * FROM table1 " +
            "UNION ALL " +
            "SELECT * FROM table2 " +
            "UNION ALL " +
            "SELECT * FROM table3 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteUnionAll.class)
                .and(input(0, hasChildThat(isTableScan("TABLE1"))))
                .and(input(1, hasChildThat(isTableScan("TABLE2"))))
                .and(input(2, hasChildThat(isTableScan("TABLE3")))));
    }

    /** */
    @Test
    public void testSetOpNumbersCast() throws Exception {
        IgniteSchema schema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = TYPE_FACTORY;

        SqlTypeName[] numTypes = new SqlTypeName[] {SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.REAL, SqlTypeName.FLOAT,
            SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DOUBLE, SqlTypeName.DECIMAL};

        for (SqlTypeName t1 : numTypes) {
            for (SqlTypeName t2 : numTypes) {
                for (SqlTypeName t3 : numTypes) {
                    if (t1 == t2 && t1 == t3)
                        continue;

                    RelDataType type = new RelDataTypeFactory.Builder(f)
                        .add("C1", f.createTypeWithNullability(f.createSqlType(t1), true))
                        .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
                        .build();

                    createTable(schema, "TABLE1", type, IgniteDistributions.single(), null);

                    type = new RelDataTypeFactory.Builder(f)
                        .add("C1", f.createTypeWithNullability(f.createSqlType(t2), true))
                        .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
                        .build();

                    createTable(schema, "TABLE2", type, IgniteDistributions.single(), null);

                    type = new RelDataTypeFactory.Builder(f)
                        .add("C1", f.createTypeWithNullability(f.createSqlType(t3), true))
                        .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
                        .build();

                    createTable(schema, "TABLE3", type, IgniteDistributions.single(), null);

                    RelDataType targetT = f.leastRestrictive(Arrays.asList(f.createSqlType(t1), f.createSqlType(t2),
                        f.createSqlType(t3)));

                    for (String op : Arrays.asList("UNION", "INTERSECT", "EXCEPT")) {
                        String sql = "SELECT * FROM table1 " + op + " SELECT * FROM table2 " + op + " SELECT * FROM table3";

                        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(org.apache.calcite.rel.core.SetOp.class)
                            .and(t1 == targetT.getSqlTypeName() ? input(0, isTableScan("TABLE1"))
                                : input(0, projectFromTable("TABLE1", "CAST($0):" + targetT, "$1")))
                            .and(t2 == targetT.getSqlTypeName() ? input(1, isTableScan("TABLE2"))
                                : input(1, projectFromTable("TABLE2", "CAST($0):" + targetT, "$1")))
                            .and(t3 == targetT.getSqlTypeName() ? input(2, isTableScan("TABLE3"))
                                : input(2, projectFromTable("TABLE3", "CAST($0):" + targetT, "$1")))
                        ));
                    }
                }
            }
        }
    }

    /** */
    @Test
    public void testSetOpNumbersCastWithDifferentNullability() throws Exception {
        IgniteSchema schema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        SqlTypeName[] numTypes = new SqlTypeName[] {SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.REAL, SqlTypeName.FLOAT,
            SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DOUBLE, SqlTypeName.DECIMAL};

        for (SqlTypeName t1 : numTypes) {
            for (SqlTypeName t2 : numTypes) {
                RelDataType type = new RelDataTypeFactory.Builder(f)
                    .add("C1", f.createTypeWithNullability(f.createSqlType(t1), false))
                    .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
                    .build();

                createTable(schema, "TABLE1", type, IgniteDistributions.single(), null);

                type = new RelDataTypeFactory.Builder(f)
                    .add("C1", f.createTypeWithNullability(f.createSqlType(t2), true))
                    .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
                    .build();

                createTable(schema, "TABLE2", type, IgniteDistributions.single(), null);

                for (String op : Arrays.asList("UNION", "UNION ALL", "INTERSECT", "EXCEPT")) {
                    String sql = "SELECT * FROM table1 " + op + " SELECT * FROM table2";

                    if (t1 == t2) {
                        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(org.apache.calcite.rel.core.SetOp.class)
                            .and(input(0, isTableScan("TABLE1")))
                            .and(input(1, isTableScan("TABLE2")))
                        ));
                    }
                    else {
                        RelDataType targetT = f.leastRestrictive(Arrays.asList(f.createSqlType(t1), f.createSqlType(t2)));

                        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(org.apache.calcite.rel.core.SetOp.class)
                            .and(t1 == targetT.getSqlTypeName() ? input(0, isTableScan("TABLE1"))
                                : input(0, projectFromTable("TABLE1", "CAST($0):" + targetT, "$1")))
                            .and(t2 == targetT.getSqlTypeName() ? input(1, isTableScan("TABLE2"))
                                : input(1, projectFromTable("TABLE2", "CAST($0):" + targetT, "$1")))
                        ));
                    }
                }
            }
        }
    }

    /** */
    protected Predicate<? extends RelNode> projectFromTable(String tableName, String... exprs) {
        return nodeOrAnyChild(
            isInstanceOf(IgniteProject.class)
                .and(projection -> {
                    String actualProjStr = projection.getProjects().toString();
                    String expectedProjStr = Arrays.asList(exprs).toString();
                    return actualProjStr.equals(expectedProjStr);
                })
                .and(input(isTableScan(tableName)))
        );
    }

    /**
     * @return Ignite schema.
     */
    private IgniteSchema prepareSchema() {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable tbl1 = new TestTable(
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
                return IgniteDistributions.affinity(0, "Table1", "hash");
            }
        };

        TestTable tbl2 = new TestTable(
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
                return IgniteDistributions.affinity(0, "Table2", "hash");
            }
        };

        TestTable tbl3 = new TestTable(
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
                return IgniteDistributions.affinity(0, "Table3", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TABLE1", tbl1);
        publicSchema.addTable("TABLE2", tbl2);
        publicSchema.addTable("TABLE3", tbl3);

        return publicSchema;
    }
}
