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

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Planner test for index hints.
 */
public class IndexHintPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** */
    private TestTable tbl1;

    /** */
    private TestTable tbl2;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        tbl1 = createTable("TBL1", 1, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", Integer.class, "VAL2", Integer.class, "VAL3", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX1", 1)
            .addIndex("IDX2", 2)
            .addIndex("IDX3", 3);

        tbl2 = createTable("TBL2", 1_000_000, IgniteDistributions.single(), "ID", Integer.class,
            "VAL21", Integer.class, "VAL22", Integer.class, "VAL23", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX1", 1)
            .addIndex("IDX22", 2)
            .addIndex("IDX23", 3);

        schema = createSchema(tbl1, tbl2);
    }

    /**
     * Tests incorrect hint params are not passed.
     */
    @Test
    public void testWrongParams() {
        assertThrows(
            null,
            () -> assertPlan("SELECT /*+ USE_INDEX */ * FROM TBL2 where val21=1 and val22=2 and val23=3", schema,
                n -> true),
            Throwable.class,
            "Hint 'USE_INDEX' must have at least one plain or key-value option."
        );
    }

    /** */
    @Test
    public void testBasicIndexSelection() throws Exception {
        assertPlan("SELECT /*+ USE_INDEX('IDX23') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX23")));

        assertPlan("SELECT /*+ USE_INDEX('IDX22') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX22")));

        assertPlan("SELECT /*+ USE_INDEX('IDX1') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX1")));

        assertPlan("SELECT /*+ USE_INDEX('IDX23'), USE_INDEX('IDX1') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX23")));

        assertPlan("SELECT /*+ USE_INDEX('IDX23', 'IDX1') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX23")));

        assertPlan("SELECT /*+ USE_INDEX('UNEXISTING', 'IDX23', 'IDX1') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX23")));

        assertPlan("SELECT /*+ USE_INDEX('UNEXISTING') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23"))));
    }

    /**
     * Tests first hint prevails.
     */
    @Test
    public void testWintNoIndex() throws Exception {
        assertPlan("SELECT /*+ NO_INDEX, USE_INDEX('IDX23') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());

        assertPlan("SELECT /*+ NO_INDEX('IDX1'), USE_INDEX('IDX1') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23"))));

        assertPlan("SELECT /*+ NO_INDEX('IDX23'), USE_INDEX('IDX1') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))));

        assertPlan("SELECT /*+ USE_INDEX('IDX23'), NO_INDEX */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX23")));
    }

    /** */
    @Test
    public void testPrevailsOverTinyTableScan() throws Exception {
        assertPlan("SELECT * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());

        assertPlan("SELECT /*+ USE_INDEX('IDX1') */ * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX1")));

        assertPlan("SELECT /*+ USE_INDEX('IDX2') */ * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2")));

        assertPlan("SELECT /*+ USE_INDEX('IDX3') */ * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX3")));
    }

    /** */
    @Test
    public void testTwoTables() throws Exception {
//        assertPlan("SELECT val1 FROM TBL1, TBL2 WHERE val1=val21 and val2=val22 and val3=val23",
//            schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX1")).negate()
//                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate())
//                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
//                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
//                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")))
//                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        assertPlan("SELECT /*+ USE_INDEX('IDX1') */ val1 FROM TBL1, TBL2 WHERE val1=val21 and val2=val22 and val3=val23",
            schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))));
    }
}
