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
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Planner test various types, casts and coercions.
 */
public class DataTypesPlannerTest extends AbstractPlannerTest {
    /** Tests casts of numeric types in SetOps (UNION, EXCEPT, INTERSECT, etc.). */
    @Test
    public void testSetOpNumbersCast() throws Exception {
        List<IgniteDistribution> distrs = Arrays.asList(IgniteDistributions.single(), IgniteDistributions.random(),
            IgniteDistributions.affinity(0, 1001, 0));

        for (IgniteDistribution d1 : distrs) {
            for (IgniteDistribution d2 : distrs) {
                //TODO:
//                doTestSetOpNumbersCast(d1, d2, true, true);

                doTestSetOpNumbersCast(d1, d2, false, true);

//                doTestSetOpNumbersCast(d1, d2, false, false);
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

        SqlTypeName[] numTypes = new SqlTypeName[] {SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.REAL,
            SqlTypeName.FLOAT, SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.DOUBLE, SqlTypeName.DECIMAL};

        boolean notNull = !nullable1 && !nullable2;

        //TODO
//        for (SqlTypeName t1 : numTypes) {
//            for (SqlTypeName t2 : numTypes) {
        for (SqlTypeName t1 : F.asList(SqlTypeName.TINYINT)) {
            for (SqlTypeName t2 : F.asList(SqlTypeName.SMALLINT)) {
                //TODO:
                System.err.println("TEST | serr: t1=" + t1 + ("(nullable=" + nullable1) + "), t2=" + t2 + ("(nullable=" + nullable2));

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

                    if (log.isInfoEnabled())
                        log.info("Test query: '" + sql + "', type1: " + t1 + ", t2: " + t2);

                    if (t1 == t2 && (!nullable1 || !nullable2))
                        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteProject.class)).negate());
                    else {
                        RelDataType targetT = f.leastRestrictive(Arrays.asList(f.createSqlType(t1), f.createSqlType(t2)));

                        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(SetOp.class)
                            .and(t1 == targetT.getSqlTypeName() ? input(0, nodeOrAnyChild(isInstanceOf(IgniteProject.class)).negate())
                                : input(0, projectFromTable("TABLE1", "CAST($t0):" + targetT + (notNull ? " NOT NULL" : ""), "$t1")))
                            .and(t2 == targetT.getSqlTypeName() ? input(1, nodeOrAnyChild(isInstanceOf(IgniteProject.class)).negate())
                                : input(1, projectFromTable("TABLE2", "CAST($t0):" + targetT + (notNull ? " NOT NULL" : ""), "$t1")))
                        ));
                    }
                }
            }
        }
    }

    /** */
    protected Predicate<? extends RelNode> projectFromTable(String tableName, String... exprs) {
        // TODO:
        return nodeOrAnyChild(isTableScan(tableName).and(tblScan->{
            String actualProj = tblScan.projects().toString();

            String expectedProj = Arrays.asList(exprs).toString();

            return actualProj.equals(expectedProj);
        }));
    }
}
