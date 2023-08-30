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

package org.apache.ignite.internal.processors.query.calcite.planner.hints;

import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 * Planner test for index hints.
 */
public class JoinTypeHintPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** */
    private TestTable tbl1;

    /** */
    private TestTable tbl2;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        tbl1 = createTable("TBL1", 10, IgniteDistributions.single(), "ID", Integer.class,
            "V1", Integer.class, "V2", Integer.class, "V3", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0);

        tbl2 = createTable("TBL2", 1_000_000, IgniteDistributions.single(), "ID", Integer.class,
            "V1", Integer.class, "V2", Integer.class, "V3", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0);

        schema = createSchema(tbl1, tbl2);
    }

    /** */
    @Test
    public void testJoins() throws Exception {
//        assertPlan("SELECT t1.v1, t2.v2 FROM TBL1 t2 JOIN TBL2 t1 on " +
//            "t1.v3=t2.v3", schema, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
//            .and(input(0, isTableScan("TBL2"))).and(input(1, isTableScan("TBL1")))));

        assertPlan("SELECT /*+ MERGE_JOIN */ t1.v1, t2.v2 FROM TBL1 t2 JOIN TBL2 t1 on " +
            "t1.v3=t2.v3", schema, nodeOrAnyChild(isInstanceOf(IgniteNestedLoopJoin.class)
            .and(input(0, isTableScan("TBL2"))).and(input(1, isTableScan("TBL1")))));
    }
}
