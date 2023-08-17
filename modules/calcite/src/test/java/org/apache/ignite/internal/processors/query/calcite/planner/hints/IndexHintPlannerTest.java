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

        // A tiny table.
        tbl1 = createTable("TBL1", 1, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", Integer.class, "VAL2", Integer.class, "VAL3", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX1", 1)
            .addIndex("IDX2", 2)
            .addIndex("IDX3", 3);

        // A large table. Has the same first inndex name 'IDX1' as of TBL1.
        tbl2 = createTable("TBL2", 10_000, IgniteDistributions.single(), "ID", Integer.class,
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
            () -> assertPlan("SELECT /*+ FORCE_INDEX */ * FROM TBL2 where val21=1 and val22=2 and val23=3", schema,
                n -> true),
            Throwable.class,
            "Hint 'FORCE_INDEX' needs at least one option."
        );
    }

    /** */
    @Test
    public void testBasicIndexSelection() throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(IDX23) */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isTableScan("TBL2")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23"))));

        assertPlan("SELECT /*+ FORCE_INDEX(UNEXISTING,IDX23,UNEXISTING) */ * FROM TBL2 WHERE val23=1 and val21=2 " +
            "and val22=3", schema, nodeOrAnyChild(isTableScan("TBL2")).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23"))));

        assertPlan("SELECT /*+ FORCE_INDEX(TBL2='IDX23') */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isTableScan("TBL2")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23"))));

        assertPlan("SELECT /*+ FORCE_INDEX(TBL2='IDX23',TBL2='IDX1') */ * FROM TBL2 WHERE val23=1 and val21=2 " +
            "and val22=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23"))).negate());

        assertPlan("SELECT /*+ FORCE_INDEX(IDX22,IDX23) */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isTableScan("TBL2")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX23), FORCE_INDEX(IDX23) */ * FROM TBL2 WHERE val23=1 and val21=2 " +
            "and val22=3", schema, nodeOrAnyChild(isTableScan("TBL2")).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        assertPlan("SELECT /*+ FORCE_INDEX(TBL2='IDX22,IDX23') */ * FROM TBL2 WHERE val23=1 and val21=2 and " +
            "val22=3", schema, nodeOrAnyChild(isTableScan("TBL2")).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));
    }
    
    /** */
    @Test
    public void testJoins() throws Exception {
        // Make sure there is no full tnl scan on TBL2 for INNER and LEFT.
        assertPlan("SELECT t1.val1, t2.val22 FROM TBL1 t1 LEFT JOIN TBL2 t2 on t1.val3=t2.val23 and " +
            "t1.val1=t2.val22", schema, nodeOrAnyChild(isTableScan("TBL1"))
            .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        assertPlan("SELECT t1.val1, t2.val22 FROM TBL1 t1 INNER JOIN TBL2 t2 on t1.val3=t2.val23 and " +
            "t1.val1=t2.val22", schema, nodeOrAnyChild(isTableScan("TBL1"))
            .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        doTestJoins("LEFT");
        doTestJoins("RIGHT");
        doTestJoins("INNER");
    }

    /** */
    private void doTestJoins(String jt) throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(TBL2='IDX22') */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
            "t2 on t1.val3=t2.val23 and t1.val1=t2.val22", schema, nodeOrAnyChild(isTableScan("TBL1"))
            .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")).negate()));

        assertPlan("SELECT /*+ FORCE_INDEX(TBL2='IDX23') */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
            "t2 on t1.val3=t2.val23 and t1.val1=t2.val22", schema, nodeOrAnyChild(isTableScan("TBL1"))
            .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")).negate()));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX22,IDX23) */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
            "t2 on t1.val3=t2.val23 and t1.val1=t2.val22", schema, nodeOrAnyChild(isTableScan("TBL1"))
            .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        // With additional filter.
        assertPlan("SELECT /*+ FORCE_INDEX(IDX22,IDX23) */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
            "t2 on t1.val3=t2.val23 and t1.val1=t2.val22 where t2.val22=2 and t1.val3=3 and t2.val21=1", schema,
            nodeOrAnyChild(isTableScan("TBL1"))
            .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        assertPlan("SELECT /*+ FORCE_INDEX(TBL2='IDX1') */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
                "t2 on t1.val3=t2.val23 and t1.val1=t2.val22 where t2.val22=2 and t1.val3=3 and t2.val21=1", schema,
            nodeOrAnyChild(isTableScan("TBL1"))
                .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")).negate()));
    }

    /** */
    @Test
    public void testOrderBy() throws Exception {
        assertPlan("SELECT val2, val3 FROM TBL1 ORDER by val2, val1, val3", schema,
            nodeOrAnyChild(isTableScan("TBL1"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX3) */ val2, val3 FROM TBL1 ORDER by val2, val1, val3", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2) */ val2, val3 FROM TBL1 ORDER by val2, val1, val3", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1) */ val2, val3 FROM TBL1 ORDER by val2, val1, val3", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))));
    }

    /** */
    @Test
    public void testAggregates() throws Exception {
        doTestAggregates("sum");
        doTestAggregates("avg");
        doTestAggregates("min");
        doTestAggregates("max");
    }
    
    /** */
    private void doTestAggregates(String op) throws Exception {
        assertPlan("SELECT avg(val1) FROM TBL1 group by val2", schema,
            nodeOrAnyChild(isTableScan("TBL1"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT " + op + "(val1) FROM TBL1 where val1=1 group by val2", schema,
            nodeOrAnyChild(isTableScan("TBL1"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2) */ " + op + "(val1) FROM TBL1 group by val2", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1,IDX2) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2",
            schema, nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
                    .or(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")))));
    }

    /** */
    @Test
    public void testOverridesOverTinyTableScan() throws Exception {
        assertPlan("SELECT * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1) */ * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2) */ * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX3) */ * FROM TBL1 WHERE val3=1 and val1=2 and val2=3",
            schema, nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3"))));
    }

    /** */
    @Test
    public void testTwoTables() throws Exception {
        assertPlan("SELECT val1 FROM TBL1, TBL2 WHERE val1=val21 and val2=val22 and val3=val23",
            schema, nodeOrAnyChild(isTableScan("TBL1"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")))
                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1,IDX22) */ val1 FROM TBL1, TBL2 WHERE val1=val21 and " +
            "val2=val22 and val3=val23", schema, nodeOrAnyChild(isTableScan("TBL1")).negate()
            .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")))
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")).negate())));

        assertPlan("SELECT /*+ FORCE_INDEX(TBL1='IDX1',TBL2='IDX1,IDX22') */ val1 FROM TBL1, TBL2 WHERE " +
                "val1=val21 and val2=val22 and val3=val23", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))
                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")))
                    .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")).negate())));

        assertPlan("SELECT /*+ FORCE_INDEX(TBL1='IDX1',TBL2='IDX22') */ val1 FROM TBL1, TBL2 WHERE " +
                "val1=val21 and val2=val22 and val3=val23", schema,
            nodeOrAnyChild(isTableScan("TBL1")).negate()
                .and(nodeOrAnyChild(isTableScan("TBL2")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX22")))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX23")).negate()));
    }
}
