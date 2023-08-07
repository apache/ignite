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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 * Planner test for index hints.
 */
public class NoIndexHintPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema publicSchema;

    /** */
    private TestTable tbl;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        tbl = createTable("TBL", 100, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", String.class, "VAL2", String.class, "VAL3", Long.class, "VAL4", Double.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("idx1", 1)
            .addIndex("IDX1", 1)
            .addIndex("IDX2", 2)
            .addIndex("IdX3", 3);

        publicSchema = createSchema(tbl);
    }

    /** */
    @Test
    public void testWithoutParams() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL WHERE id = 0");
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL WHERE val1 = 'testVal'");
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL WHERE val2 = 'testVal'");
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL WHERE val3 = 'testVal'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL WHERE id = 0 and val3 = 'testVal'");
    }

    /** */
    @Test
    public void testCertainIndex() throws Exception {
//        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL WHERE val1 = 'testVal'", "idx1");
//
//        assertNoCertainIndex("SELECT /*+ NO_INDEX('" + QueryUtils.PRIMARY_KEY_INDEX + "') */ * FROM TBL WHERE id = 0", QueryUtils.PRIMARY_KEY_INDEX);
//
//        assertNoCertainIndex("SELECT /*+ NO_INDEX('IDX1') */ * FROM TBL WHERE val1 = 'testVal'", "IDX1");
//        assertCertainIndex("SELECT /*+ NO_INDEX('IDX1') */ * FROM TBL WHERE val1 = 'testVal'", "idx1");
//
//        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx2') */ * FROM TBL WHERE val2 = 'testVal'", "idx2");
//        assertCertainIndex("SELECT /*+ NO_INDEX('idx2') */ * FROM TBL WHERE val2 = 'testVal'", "IDX2");
//
        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL WHERE val1 = 'testVal' and val2 = 'testVal'", "idx1");
        assertNoCertainIndex("SELECT /*+ NO_INDEX('IDX1') */ * FROM TBL WHERE val1 = 'testVal' and val2 = 'testVal'", "IDX1");
        assertCertainIndex("SELECT /*+ NO_INDEX('IDX1') */ * FROM TBL WHERE val1 = 'testVal' and val2 = 'testVal'", "idx1");
        assertCertainIndex("SELECT /*+ NO_INDEX('idx1', 'IDX1') */ * FROM TBL WHERE val1 = 'testVal' and val2 = 'testVal'", "IDX2");
    }

    /** */
    private void assertNoAnyIndex(String sql) throws Exception {
        assertPlan(sql, publicSchema, isInstanceOf(IgniteTableScan.class));
    }

    /** */
    private void assertNoCertainIndex(String sql, String... idxNames) throws Exception {
        for(String idx : idxNames)
            assertPlan(sql, publicSchema, isIndexScan("TBL", idx).negate());
    }

    /** */
    private void assertCertainIndex(String sql, String... idxNames) throws Exception {
        for(String idx : idxNames)
            assertPlan(sql, publicSchema, isIndexScan("TBL", idx));
    }
}
