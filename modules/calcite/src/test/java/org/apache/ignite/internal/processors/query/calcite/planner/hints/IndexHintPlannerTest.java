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
            "VAL1", Integer.class, "VAL2", Integer.class, "VAL3", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX1", 1)
            .addIndex("IDX2", 2)
            .addIndex("IDX3", 3);

        schema = createSchema(tbl1, tbl2);
    }

    /**
     * Tests incorrect hint params are not passed.
     */
    @Test
    public void testWrongParams() {
        assertThrows(
            null,
            () -> assertPlan("SELECT /*+ USE_INDEX */ * FROM TBL2 where val1=1 and val2=2 and val3=3", schema,
                n -> true),
            Throwable.class,
            "Hint 'USE_INDEX' must have exactly one plain or key-value option."
        );

        assertThrows(
            null,
            () -> assertPlan("SELECT /*+ USE_INDEX('IDX3','IDX1') */ * FROM TBL2 where val1=1 and val2=2 and val3=3",
                schema, n -> true),
            Throwable.class,
            "Hint 'USE_INDEX' must have exactly one plain or key-value option."
        );

        assertThrows(
            null,
            () -> assertPlan("SELECT /*+ USE_INDEX(TBL2='IDX3',TBL1='IDX1') */ t1.val1 FROM TBL1 t1, TBL2 t2 " +
                "where t1.val1=1 and t2.val2=2 and t1.val3=t2.val3", schema, n -> true),
            Throwable.class,
            "Hint 'USE_INDEX' must have exactly one plain or key-value option."
        );
    }

    /** */
    @Test
    public void testBasicIndexSelection() throws Exception {
        assertPlan("SELECT /*+ USE_INDEX('IDX3') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")));

        assertPlan("SELECT /*+ USE_INDEX('IDX2') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2")));

        assertPlan("SELECT /*+ USE_INDEX('IDX1') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX1")));

        assertPlan("SELECT /*+ USE_INDEX('IDX3'), USE_INDEX('IDX1') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")));

        assertPlan("SELECT /*+ USE_INDEX('IDX2'), USE_INDEX('IDX1') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2")));

        assertPlan("SELECT /*+ USE_INDEX('IDX1'), USE_INDEX('IDX3') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX1")));
    }

    /** */
    @Test
    public void testWintNoIndex() throws Exception {
        assertPlan("SELECT /*+ NO_INDEX('IDX1'), USE_INDEX('IDX3') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")));

        assertPlan("SELECT /*+ NO_INDEX('IDX3'), USE_INDEX('IDX3') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2")))));

        assertPlan("SELECT /*+ USE_INDEX('IDX3'), NO_INDEX('IDX3') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2")))));

        assertPlan("SELECT /*+ USE_INDEX('IDX3'), USE_INDEX('IDX2') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")));

        assertPlan("SELECT /*+ NO_INDEX, USE_INDEX('IDX3') */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class).negate()));

        assertPlan("SELECT /*+ USE_INDEX('IDX3'), NO_INDEX */ * FROM TBL2 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class).negate()));
    }

//    /** */
//    @Test
//    public void testPrevailsOverTableScan() throws Exception {
//        assertPlan("SELECT /*+ USE_INDEX('IDX3') */ * FROM TBL WHERE val3=1 and val1=2 and val2=3",
//            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")));
//
//        assertPlan("SELECT /*+ USE_INDEX('IDX2') */ * FROM TBL WHERE val3=1 and val1=2 and val2=3",
//            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2")));
//
//        assertPlan("SELECT /*+ USE_INDEX('IDX1') */ * FROM TBL WHERE val3=1 and val1=2 and val2=3",
//            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX1")));
//    }
}
