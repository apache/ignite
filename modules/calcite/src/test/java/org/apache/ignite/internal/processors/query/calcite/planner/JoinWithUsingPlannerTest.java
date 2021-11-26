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

import java.sql.Date;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Before;
import org.junit.Test;

/**
 * Test table functions.
 */
public class JoinWithUsingPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;


    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();

        publicSchema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type1 = new RelDataTypeFactory.Builder(f)
            .add("EMPID", f.createJavaType(Integer.class))
            .add("DEPTID", f.createJavaType(Integer.class))
            .add("NAME", f.createJavaType(String.class))
            .build();

        RelDataType type2 = new RelDataTypeFactory.Builder(f)
            .add("DEPTID", f.createJavaType(Integer.class))
            .add("NAME", f.createJavaType(String.class))
            .add("PARENTID", f.createJavaType(Integer.class))
            .build();

        RelDataType type3 = new RelDataTypeFactory.Builder(f)
            .add("EMPID", f.createJavaType(Integer.class))
            .add("DEPTID", f.createJavaType(Integer.class))
            .add("D", f.createJavaType(Date.class))
            .build();

        createTable(publicSchema, "T1", type1, IgniteDistributions.random(), null);
        createTable(publicSchema, "T2", type2, IgniteDistributions.random(), null);
        createTable(publicSchema, "T3", type3, IgniteDistributions.random(), null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWithUsing() throws Exception {
        String sql = "SELECT * FROM T1 AS T1 JOIN T2 AS T2 USING (DEPTID)";
        //String sql = "SELECT * FROM T1 JOIN T2 USING (DEPTID)";
        //String sql = "SELECT * FROM (SELECT * FROM T1) AS T1 JOIN (SELECT * FROM T2) AS T2 USING (DEPTID)";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableFunctionScan.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNaturalJoin() throws Exception {
        String sql = "SELECT * FROM broadcast_tbl t JOIN TABLE(system_range(1, 1)) r ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)).negate()
            .and(nodeOrAnyChild(isInstanceOf(Join.class)
                .and(input(0, nodeOrAnyChild(isTableScan("broadcast_tbl"))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableFunctionScan.class))))
            )));
    }
}
