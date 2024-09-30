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
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
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


    /** Tests casts of numeric types in SetOps (UNION, EXCEPT, INTERSECT, etc.). */
    @Test
    public void testSetOpNumbersCast() throws Exception {
        List<IgniteDistribution> distrs = Arrays.asList(IgniteDistributions.single(), IgniteDistributions.random(),
            IgniteDistributions.affinity(0, 1001, 0));

        for (IgniteDistribution d1 : distrs) {
            for (IgniteDistribution d2 : distrs) {
                doTestSetOpNumbersCast(d1, d2, true, true);

                doTestSetOpNumbersCast(d1, d2, false, true);

                doTestSetOpNumbersCast(d1, d2, false, false);
            }
        }
    }

    /** */
    private void doTestSetOpNumbersCast(
        IgniteDistribution distr1,
        IgniteDistribution distr2,
        boolean nullable1,
        boolean nullable2
    ) throws Exception {
        IgniteSchema schema = new IgniteSchema(DEFAULT_SCHEMA);

        IgniteTypeFactory f = Commons.typeFactory();

        SqlTypeName[] numTypes = new SqlTypeName[] {SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.REAL, SqlTypeName.FLOAT,
            SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DOUBLE, SqlTypeName.DECIMAL};

        boolean notNull = !nullable1 && !nullable2;

        for (SqlTypeName t1 : numTypes) {
            for (SqlTypeName t2 : numTypes) {
                RelDataType type = new RelDataTypeFactory.Builder(f)
                    .add("C1", f.createTypeWithNullability(f.createSqlType(t1), nullable1))
                    .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
                    .build();

                createTable(schema, "TABLE1", type, distr1, null);

                type = new RelDataTypeFactory.Builder(f)
                    .add("C1", f.createTypeWithNullability(f.createSqlType(t2), nullable2))
                    .add("C2", f.createTypeWithNullability(f.createSqlType(SqlTypeName.VARCHAR), true))
                    .build();

                createTable(schema, "TABLE2", type, distr2, null);

                for (String op : Arrays.asList("UNION", "INTERSECT", "EXCEPT")) {
                    String sql = "SELECT * FROM table1 " + op + " SELECT * FROM table2";

                    if (t1 == t2 && (!nullable1 || !nullable2))
                        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteProject.class)).negate());
                    else {
                        RelDataType targetT = f.leastRestrictive(Arrays.asList(f.createSqlType(t1), f.createSqlType(t2)));

                        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(org.apache.calcite.rel.core.SetOp.class)
                            .and(t1 == targetT.getSqlTypeName() ? input(0, nodeOrAnyChild(isInstanceOf(IgniteProject.class)).negate())
                                : input(0, projectFromTable("TABLE1", "CAST($0):" + targetT + (notNull ? " NOT NULL" : ""), "$1")))
                            .and(t2 == targetT.getSqlTypeName() ? input(1, nodeOrAnyChild(isInstanceOf(IgniteProject.class)).negate())
                                : input(1, projectFromTable("TABLE2", "CAST($0):" + targetT + (notNull ? " NOT NULL" : ""), "$1")))
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
                    String actualProj = projection.getProjects().toString();

                    String expectedProj = Arrays.asList(exprs).toString();

                    return actualProj.equals(expectedProj);
                })
                .and(input(nodeOrAnyChild(isTableScan(tableName))))
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
