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
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 * Planner test for index hints.
 */
public class IndexHintPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** */
    private TestTable tbl;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        tbl = createTable("TBL", 1, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", Integer.class, "VAL2", Integer.class, "VAL3", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX1", 1)
            .addIndex("IDX2", 2)
            .addIndex("IDX3", 3);

        schema = createSchema(tbl);
    }

    /** */
    @Test
    public void testWithTableAndSchemaName() throws Exception {
        assertPlan("SELECT * FROM TBL WHERE val2=1 and val3=2 and val1=3",
            schema, nodeOrAnyChild(isIndexScan("TBL", "IDX2"))
                .and(nodeOrAnyChild(isIndexScan("TBL", "IDX3"))
                    .and(nodeOrAnyChild(isIndexScan("TBL", "IDX1")))));
    }
}
