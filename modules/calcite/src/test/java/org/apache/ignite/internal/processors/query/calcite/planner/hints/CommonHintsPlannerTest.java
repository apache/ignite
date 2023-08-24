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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 * Common test for SQL hints.
 */
public class CommonHintsPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** */
    private TestTable tbl;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        tbl = createTable("TBL", 100, IgniteDistributions.random(), "ID", Integer.class, "VAL",
            Integer.class).addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0).addIndex("IDX", 1);

        schema = createSchema(tbl);
    }

    /**
     * Tests hint 'DISABLE_RULE' works for whole query despite it is not set for the all the root nodes.
     */
    @Test
    public void testDisableRuleInHeader() throws Exception {
        assertPlan("SELECT /*+ DISABLE_RULE('ExposeIndexRule') */ VAL FROM TBL where val=1 UNION ALL " +
            "SELECT VAL FROM TBL where ID=1", schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());
    }
}
