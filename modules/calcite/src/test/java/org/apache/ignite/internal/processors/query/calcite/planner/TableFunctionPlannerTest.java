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

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test table functions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TableFunctionPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;


    /**
     *
     */
    @BeforeAll
    public void setup() {
        publicSchema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("SALARY", f.createJavaType(Double.class))
                .build();

        createTable(publicSchema, "RANDOM_TBL", type, IgniteDistributions.random());
        createTable(publicSchema, "BROADCAST_TBL", type, IgniteDistributions.broadcast());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTableFunctionScan() throws Exception {
        String sql = "SELECT * FROM TABLE(system_range(1, 1))";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableFunctionScan.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBroadcastTableAndTableFunctionJoin() throws Exception {
        String sql = "SELECT * FROM broadcast_tbl t JOIN TABLE(system_range(1, 1)) r ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)).negate()
                .and(nodeOrAnyChild(isInstanceOf(Join.class)
                        .and(input(0, nodeOrAnyChild(isTableScan("broadcast_tbl"))))
                        .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableFunctionScan.class))))
                )));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomTableAndTableFunctionJoin() throws Exception {
        String sql = "SELECT * FROM random_tbl t JOIN TABLE(system_range(1, 1)) r ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(Join.class)
                .and(input(0, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                        .and(nodeOrAnyChild(isTableScan("random_tbl"))))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableFunctionScan.class))))
        ));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCorrelatedTableFunctionJoin() throws Exception {
        String sql = "SELECT t.id, (SELECT x FROM TABLE(system_range(t.id, t.id))) FROM random_tbl t";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                .and(input(0, nodeOrAnyChild(isTableScan("random_tbl"))))
                .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteTableFunctionScan.class))))
        ));
    }
}
